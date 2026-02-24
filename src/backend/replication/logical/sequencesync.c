/*-------------------------------------------------------------------------
 * sequencesync.c
 *	  PostgreSQL logical replication: sequence synchronization
 *
 * Copyright (c) 2025-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/sequencesync.c
 *
 * NOTES
 *	  This file contains code for sequence synchronization for
 *	  logical replication.
 *
 * Sequences requiring synchronization are tracked in the pg_subscription_rel
 * catalog.
 *
 * Sequences to be synchronized will be added with state INIT when either of
 * the following commands is executed:
 * CREATE SUBSCRIPTION
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION
 *
 * The apply worker periodically scans pg_subscription_rel for sequences in
 * INIT state. When such sequences are found, it spawns a sequencesync worker
 * to handle synchronization.
 *
 * A single sequencesync worker is responsible for synchronizing all sequences.
 * It begins by retrieving the list of sequences that are flagged for
 * synchronization, i.e., those in the INIT state. These sequences are then
 * processed in batches, allowing multiple entries to be synchronized within a
 * single transaction. The worker fetches the current sequence values and page
 * LSNs from the remote publisher, updates the corresponding sequences on the
 * local subscriber, and finally marks each sequence as READY upon successful
 * synchronization.
 *
 * The sequencesync worker then fetches all sequences that are
 * in the READY state, queries the publisher for current sequence values, and
 * updates any sequences that have drifted and then goes to sleep. The sleep
 * interval starts as SEQSYNC_MIN_SLEEP_MS and doubles after each wake cycle
 * (up to SEQSYNC_MAX_SLEEP_MS). When drift is detected, the interval resets to
 * the minimum to ensure timely updates.
 *
 * After CREATE SUBSCRIPTION, sequences begin in the INIT state. Sequences
 * added through ALTER SUBSCRIPTION.. REFRESH PUBLICATION also start in the INIT
 * state. All INIT sequences are synchronized unconditionally, then transition
 * to the READY state. Once in the READY state, sequences are checked for drift
 * from the publisher and synchronized only when drift is detected.
 *
 * Sequence state transitions follow this pattern:
 *  INIT --> READY ->-+
 *             ^      | (check/synchronize)
 *             |      |
 *             +--<---+
 *
 * To avoid creating too many transactions, up to MAX_SEQUENCES_SYNC_PER_BATCH
 * sequences are synchronized per transaction. The locks on the sequence
 * relation will be periodically released at each transaction commit.
 *
 * XXX: We didn't choose launcher process to maintain the launch of sequencesync
 * worker as it didn't have database connection to access the sequences from the
 * pg_subscription_rel system catalog that need to be synchronized.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/sequence.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "replication/logicalworker.h"
#include "replication/worker_internal.h"
#include "storage/latch.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"
#include "utils/usercontext.h"

#define REMOTE_SEQ_COL_COUNT 10

typedef enum CopySeqResult
{
	COPYSEQ_SUCCESS,
	COPYSEQ_ALLOWED,
	COPYSEQ_MISMATCH,
	COPYSEQ_INSUFFICIENT_PERM,
	COPYSEQ_SKIPPED,
	COPYSEQ_NO_DRIFT,
} CopySeqResult;

/* Sleep intervals for sync */
#define SEQSYNC_MIN_SLEEP_MS 2000	/* 2 seconds */
#define SEQSYNC_MAX_SLEEP_MS 30000	/* 30 seconds */

static MemoryContext SequenceSyncContext = NULL;

/*
 * Apply worker determines whether a sequence sync worker is needed.
 *
 * Start a sequencesync worker if one is not already running. The active
 * sequencesync worker will handle all pending sequence synchronization. If any
 * sequences remain unsynchronized after it exits, a new worker can be started
 * in the next iteration.
 *
 * The pointer to the sequencesync worker is cached to avoid scanning the
 * workers array each time via logicalrep_worker_find().
 */
void
MaybeLaunchSequenceSyncWorker(void)
{
	static LogicalRepWorker *sequencesync_worker = NULL;

	int			nsyncworkers;
	bool		has_pending_sequences;
	bool		started_tx;

	FetchRelationStates(NULL, &has_pending_sequences, &started_tx);

	if (started_tx)
	{
		CommitTransactionCommand();
		pgstat_report_stat(true);
	}

	if (!has_pending_sequences)
		return;

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	/*
	 * Quick exit if the sequence sync worker for the current subscription is
	 * already alive.
	 */
	if (sequencesync_worker &&
		sequencesync_worker->proc &&
		isSequenceSyncWorker(sequencesync_worker) &&
		sequencesync_worker->subid == MyLogicalRepWorker->subid)
	{
		LWLockRelease(LogicalRepWorkerLock);
		return;
	}

	/* Check if there is a sequencesync worker already running? */
	sequencesync_worker = logicalrep_worker_find(WORKERTYPE_SEQUENCESYNC,
												 MyLogicalRepWorker->subid,
												 InvalidOid, true);
	if (sequencesync_worker)
	{
		LWLockRelease(LogicalRepWorkerLock);
		return;
	}

	/*
	 * Count running sync workers for this subscription, while we have the
	 * lock.
	 */
	nsyncworkers = logicalrep_sync_worker_count(MyLogicalRepWorker->subid);
	LWLockRelease(LogicalRepWorkerLock);

	/*
	 * It is okay to read/update last_seqsync_start_time here in apply worker
	 * as we have already ensured that sync worker doesn't exist.
	 */
	launch_sync_worker(WORKERTYPE_SEQUENCESYNC, nsyncworkers, InvalidOid,
					   &MyLogicalRepWorker->last_seqsync_start_time);
}

/*
 * get_sequences_string
 *
 * Build a comma-separated string of schema-qualified sequence names
 * for the given list of sequence indexes.
 */
static void
get_sequences_string(List *seqindexes, List *seqinfos, StringInfo buf)
{
	resetStringInfo(buf);
	foreach_int(seqidx, seqindexes)
	{
		LogicalRepSequenceInfo *seqinfo =
			(LogicalRepSequenceInfo *) list_nth(seqinfos, seqidx);

		if (buf->len > 0)
			appendStringInfoString(buf, ", ");

		appendStringInfo(buf, "\"%s.%s\"", seqinfo->nspname, seqinfo->seqname);
	}
}

/*
 * report_sequence_errors
 *
 * Report discrepancies found during sequence synchronization between
 * the publisher and subscriber. Emits warnings for:
 * a) mismatched definitions or concurrent rename
 * b) insufficient privileges
 * c) missing sequences on the subscriber
 * Then raises an ERROR to indicate synchronization failure.
 */
static void
report_sequence_errors(List *mismatched_seqs_idx, List *insuffperm_seqs_idx,
					   List *missing_seqs_idx, List *seqinfos)
{
	StringInfo	seqstr;

	/* Quick exit if there are no errors to report */
	if (!mismatched_seqs_idx && !insuffperm_seqs_idx && !missing_seqs_idx)
		return;

	seqstr = makeStringInfo();

	if (mismatched_seqs_idx)
	{
		get_sequences_string(mismatched_seqs_idx, seqinfos, seqstr);
		ereport(WARNING,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("mismatched or renamed sequence on subscriber (%s)",
							  "mismatched or renamed sequences on subscriber (%s)",
							  list_length(mismatched_seqs_idx),
							  seqstr->data));
	}

	if (insuffperm_seqs_idx)
	{
		get_sequences_string(insuffperm_seqs_idx, seqinfos, seqstr);
		ereport(WARNING,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("insufficient privileges on sequence (%s)",
							  "insufficient privileges on sequences (%s)",
							  list_length(insuffperm_seqs_idx),
							  seqstr->data));
	}

	if (missing_seqs_idx)
	{
		get_sequences_string(missing_seqs_idx, seqinfos, seqstr);
		ereport(WARNING,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("missing sequence on publisher (%s)",
							  "missing sequences on publisher (%s)",
							  list_length(missing_seqs_idx),
							  seqstr->data));
	}

	ereport(ERROR,
			errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("logical replication sequence synchronization failed for subscription \"%s\"",
				   MySubscription->name));
}

/*
 * get_and_validate_seq_info
 *
 * Extracts remote sequence information from the tuple slot received from the
 * publisher, and validates it against the corresponding local sequence
 * definition.
 */
static CopySeqResult
get_and_validate_seq_info(TupleTableSlot *slot, Relation *sequence_rel,
						  LogicalRepSequenceInfo **seqinfo, int *seqidx,
						  List *seqinfos)
{
	bool		isnull;
	int			col = 0;
	Datum		datum;
	Oid			remote_typid;
	int64		remote_start;
	int64		remote_increment;
	int64		remote_min;
	int64		remote_max;
	bool		remote_cycle;
	CopySeqResult result = COPYSEQ_ALLOWED;
	HeapTuple	tup;
	Form_pg_sequence local_seq;
	LogicalRepSequenceInfo *seqinfo_local;

	*seqidx = DatumGetInt32(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	/* Identify the corresponding local sequence for the given index. */
	*seqinfo = seqinfo_local =
		(LogicalRepSequenceInfo *) list_nth(seqinfos, *seqidx);

	/*
	 * last_value can be NULL if the sequence was dropped concurrently (see
	 * pg_get_sequence_data()).
	 */
	datum = slot_getattr(slot, ++col, &isnull);
	if (isnull)
		return COPYSEQ_SKIPPED;
	seqinfo_local->last_value = DatumGetInt64(datum);

	seqinfo_local->is_called = DatumGetBool(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqinfo_local->page_lsn = DatumGetLSN(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	remote_typid = DatumGetObjectId(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	remote_start = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	remote_increment = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	remote_min = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	remote_max = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	remote_cycle = DatumGetBool(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	/* Sanity check */
	Assert(col == REMOTE_SEQ_COL_COUNT);

	seqinfo_local->found_on_pub = true;

	*sequence_rel = try_table_open(seqinfo_local->localrelid, RowExclusiveLock);

	/* Sequence was concurrently dropped? */
	if (!*sequence_rel)
		return COPYSEQ_SKIPPED;

	tup = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqinfo_local->localrelid));

	/* Sequence was concurrently dropped? */
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for sequence %u",
			 seqinfo_local->localrelid);

	local_seq = (Form_pg_sequence) GETSTRUCT(tup);

	/* Sequence parameters for remote/local are the same? */
	if (local_seq->seqtypid != remote_typid ||
		local_seq->seqstart != remote_start ||
		local_seq->seqincrement != remote_increment ||
		local_seq->seqmin != remote_min ||
		local_seq->seqmax != remote_max ||
		local_seq->seqcycle != remote_cycle)
		result = COPYSEQ_MISMATCH;

	/* Sequence was concurrently renamed? */
	if (strcmp(seqinfo_local->nspname,
			   get_namespace_name(RelationGetNamespace(*sequence_rel))) ||
		strcmp(seqinfo_local->seqname, RelationGetRelationName(*sequence_rel)))
		result = COPYSEQ_MISMATCH;

	ReleaseSysCache(tup);
	return result;
}

/*
 * Check whether the user has required privileges on the sequence and
 * whether the sequence has drifted.
 */
static CopySeqResult
validate_seqsync_state(LogicalRepSequenceInfo *seqinfo, Relation sequence_rel)
{
	AclResult	aclresult;
	Oid			seqoid = seqinfo->localrelid;

	/* Perform drift check if it's not the initial sync */
	if (seqinfo->relstate == SUBREL_STATE_READY)
	{
		int64		local_last_value;
		bool		local_is_called;

		/*
		 * Skip synchronization if the current user does not have sufficient
		 * privileges to read the sequence data.
		 */
		if (!GetSequence(sequence_rel, &local_last_value, &local_is_called))
			return COPYSEQ_INSUFFICIENT_PERM;

		/*
		 * Skip synchronization if the sequence has not drifted from the
		 * publisher's value.
		 */
		if (local_last_value == seqinfo->last_value &&
			local_is_called == seqinfo->is_called)
			return COPYSEQ_NO_DRIFT;
	}

	/* Verify that the current user can update the sequence */
	aclresult = pg_class_aclcheck(seqoid, GetUserId(), ACL_UPDATE);

	if (aclresult != ACLCHECK_OK)
		return COPYSEQ_INSUFFICIENT_PERM;

	return COPYSEQ_ALLOWED;
}

/*
 * Apply remote sequence state to local sequence. For sequences in INIT state,
 * always synchronize and then move them to READY state upon completion. For
 * sequences already in READY state, synchronize only if drift is detected.
 */
static CopySeqResult
copy_sequence(LogicalRepSequenceInfo *seqinfo, Relation sequence_rel)
{
	UserContext ucxt;
	bool		run_as_owner = MySubscription->runasowner;
	Oid			seqoid = seqinfo->localrelid;
	CopySeqResult result;
	XLogRecPtr	local_page_lsn;

	(void) GetSubscriptionRelState(MySubscription->oid,
								   RelationGetRelid(sequence_rel),
								   &local_page_lsn);

	/*
	 * If the user did not opt to run as the owner of the subscription
	 * ('run_as_owner'), then copy the sequence as the owner of the sequence.
	 */
	if (!run_as_owner)
		SwitchToUntrustedUser(sequence_rel->rd_rel->relowner, &ucxt);

	result = validate_seqsync_state(seqinfo, sequence_rel);

	if (result != COPYSEQ_ALLOWED)
	{
		if (!run_as_owner)
			RestoreUserContext(&ucxt);

		return result;
	}

	/*
	 * The log counter (log_cnt) tracks how many sequence values are still
	 * unused locally. It is only relevant to the local node and managed
	 * internally by nextval() when allocating new ranges. Since log_cnt does
	 * not affect the visible sequence state (like last_value or is_called)
	 * and is only used for local caching, it need not be copied to the
	 * subscriber during synchronization.
	 */
	SetSequence(seqoid, seqinfo->last_value, seqinfo->is_called);

	if (!run_as_owner)
		RestoreUserContext(&ucxt);

	/*
	 * Record the remote sequence's LSN in pg_subscription_rel and mark the
	 * sequence as READY if updating a sequence that is in INIT state.
	 */
	if (seqinfo->relstate == SUBREL_STATE_INIT ||
		seqinfo->page_lsn != local_page_lsn)
		UpdateSubscriptionRelState(MySubscription->oid, seqoid, SUBREL_STATE_READY,
								   seqinfo->page_lsn, false);

	return COPYSEQ_SUCCESS;
}

/*
 * Copy existing data of sequences from the publisher.
 *
 * If the state of sequence is SUBREL_STATE_READY, only synchronize sequences
 * that have drifted from their publisher values. Otherwise, synchronize all
 * sequences.
 *
 * Returns true/false if any sequences were actually copied.
 */
static bool
copy_sequences(WalReceiverConn *conn, List *seqinfos)
{
	int			cur_batch_base_index = 0;
	int			n_seqinfos = list_length(seqinfos);
	List	   *mismatched_seqs_idx = NIL;
	List	   *missing_seqs_idx = NIL;
	List	   *insuffperm_seqs_idx = NIL;
	StringInfo	seqstr = makeStringInfo();
	StringInfo	cmd = makeStringInfo();
	MemoryContext oldctx;
	bool		sequence_copied = false;

#define MAX_SEQUENCES_SYNC_PER_BATCH 100

	while (cur_batch_base_index < n_seqinfos)
	{
		Oid			seqRow[REMOTE_SEQ_COL_COUNT] = {INT8OID, INT8OID,
		BOOLOID, LSNOID, OIDOID, INT8OID, INT8OID, INT8OID, INT8OID, BOOLOID};
		int			batch_size = 0;
		int			batch_succeeded_count = 0;
		int			batch_mismatched_count = 0;
		int			batch_skipped_count = 0;
		int			batch_insuffperm_count = 0;
		int			batch_no_drift = 0;
		int			batch_missing_count;
		Relation	sequence_rel = NULL;

		WalRcvExecResult *res;
		TupleTableSlot *slot;

		StartTransactionCommand();

		for (int idx = cur_batch_base_index; idx < n_seqinfos; idx++)
		{
			char	   *nspname_literal;
			char	   *seqname_literal;

			LogicalRepSequenceInfo *seqinfo =
				(LogicalRepSequenceInfo *) list_nth(seqinfos, idx);

			if (seqstr->len > 0)
				appendStringInfoString(seqstr, ", ");

			nspname_literal = quote_literal_cstr(seqinfo->nspname);
			seqname_literal = quote_literal_cstr(seqinfo->seqname);

			appendStringInfo(seqstr, "(%s, %s, %d)",
							 nspname_literal, seqname_literal, idx);

			if (++batch_size == MAX_SEQUENCES_SYNC_PER_BATCH)
				break;
		}

		/*
		 * We deliberately avoid acquiring a local lock on the sequence before
		 * querying the publisher to prevent potential distributed deadlocks
		 * in bi-directional replication setups.
		 *
		 * Example scenario:
		 *
		 * - On each node, a background worker acquires a lock on a sequence
		 * as part of a sync operation.
		 *
		 * - Concurrently, a user transaction attempts to alter the same
		 * sequence, waiting on the background worker's lock.
		 *
		 * - Meanwhile, a query from the other node tries to access metadata
		 * that depends on the completion of the alter operation.
		 *
		 * - This creates a circular wait across nodes:
		 *
		 * Node-1: Query -> waits on Alter -> waits on Sync Worker
		 *
		 * Node-2: Query -> waits on Alter -> waits on Sync Worker
		 *
		 * Since each node only sees part of the wait graph, the deadlock may
		 * go undetected, leading to indefinite blocking.
		 *
		 * Note: Each entry in VALUES includes an index 'seqidx' that
		 * represents the sequence's position in the local 'seqinfos' list.
		 * This index is propagated to the query results and later used to
		 * directly map the fetched publisher sequence rows back to their
		 * corresponding local entries without relying on result order or name
		 * matching.
		 */
		appendStringInfo(cmd,
						 "SELECT s.seqidx, ps.*, seq.seqtypid,\n"
						 "       seq.seqstart, seq.seqincrement, seq.seqmin,\n"
						 "       seq.seqmax, seq.seqcycle\n"
						 "FROM ( VALUES %s ) AS s (schname, seqname, seqidx)\n"
						 "JOIN pg_namespace n ON n.nspname = s.schname\n"
						 "JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = s.seqname\n"
						 "JOIN pg_sequence seq ON seq.seqrelid = c.oid\n"
						 "JOIN LATERAL pg_get_sequence_data(seq.seqrelid) AS ps ON true\n",
						 seqstr->data);

		res = walrcv_exec(conn, cmd->data, lengthof(seqRow), seqRow);
		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("could not fetch sequence information from the publisher: %s",
						   res->err));

		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		{
			CopySeqResult sync_status;
			LogicalRepSequenceInfo *seqinfo;
			int			seqidx;

			CHECK_FOR_INTERRUPTS();

			if (ConfigReloadPending)
			{
				ConfigReloadPending = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			sync_status = get_and_validate_seq_info(slot, &sequence_rel,
													&seqinfo, &seqidx, seqinfos);

			if (sync_status == COPYSEQ_ALLOWED)
				sync_status = copy_sequence(seqinfo, sequence_rel);

			switch (sync_status)
			{
				case COPYSEQ_SUCCESS:
					elog(DEBUG1,
						 "logical replication synchronization has updated sequence \"%s.%s\" in subscription \"%s\"",
						 seqinfo->nspname, seqinfo->seqname, MySubscription->name);
					batch_succeeded_count++;
					sequence_copied = true;
					break;
				case COPYSEQ_MISMATCH:

					/*
					 * Remember mismatched sequences in SequenceSyncContext
					 * since these will be used after the transaction is
					 * committed.
					 */
					oldctx = MemoryContextSwitchTo(SequenceSyncContext);
					mismatched_seqs_idx = lappend_int(mismatched_seqs_idx,
													  seqidx);
					MemoryContextSwitchTo(oldctx);
					batch_mismatched_count++;
					break;
				case COPYSEQ_INSUFFICIENT_PERM:

					/*
					 * Remember sequences with insufficient privileges in
					 * SequenceSyncContext since these will be used after the
					 * transaction is committed.
					 */
					oldctx = MemoryContextSwitchTo(SequenceSyncContext);
					insuffperm_seqs_idx = lappend_int(insuffperm_seqs_idx,
													  seqidx);
					MemoryContextSwitchTo(oldctx);
					batch_insuffperm_count++;
					break;
				case COPYSEQ_SKIPPED:

					/*
					 * Concurrent removal of a sequence on the subscriber is
					 * treated as success, since the only viable action is to
					 * skip the corresponding sequence data. Missing sequences
					 * on the publisher are treated as ERROR.
					 */
					if (seqinfo->found_on_pub)
					{
						ereport(LOG,
								errmsg("skip synchronization of sequence \"%s.%s\" because it has been dropped concurrently",
									   seqinfo->nspname,
									   seqinfo->seqname));
						batch_skipped_count++;
					}
					break;
				case COPYSEQ_NO_DRIFT:
					/* Nothing to do */
					batch_no_drift++;
					break;
				default:
					elog(ERROR, "unrecognized Sequence replication result: %d", (int) sync_status);

			}

			if (sequence_rel)
				table_close(sequence_rel, NoLock);
		}

		ExecDropSingleTupleTableSlot(slot);
		walrcv_clear_result(res);
		resetStringInfo(seqstr);
		resetStringInfo(cmd);

		batch_missing_count = batch_size - (batch_succeeded_count +
											batch_mismatched_count +
											batch_insuffperm_count +
											batch_skipped_count +
											batch_no_drift);

		elog(DEBUG1,
			 "logical replication sequence synchronization for subscription \"%s\" - batch #%d = %d attempted, %d succeeded, %d mismatched, %d insufficient permission, %d missing from publisher, %d skipped, %d no drift",
			 MySubscription->name,
			 (cur_batch_base_index / MAX_SEQUENCES_SYNC_PER_BATCH) + 1,
			 batch_size, batch_succeeded_count, batch_mismatched_count,
			 batch_insuffperm_count, batch_missing_count, batch_skipped_count, batch_no_drift);

		/* Commit this batch, and prepare for next batch */
		CommitTransactionCommand();

		if (batch_missing_count)
		{
			for (int idx = cur_batch_base_index; idx < cur_batch_base_index + batch_size; idx++)
			{
				LogicalRepSequenceInfo *seqinfo =
					(LogicalRepSequenceInfo *) list_nth(seqinfos, idx);

				/* If the sequence was not found on publisher, record it */
				if (!seqinfo->found_on_pub)
					missing_seqs_idx = lappend_int(missing_seqs_idx, idx);
			}
		}

		/*
		 * cur_batch_base_index is not incremented sequentially because some
		 * sequences may be missing, and the number of fetched rows may not
		 * match the batch size.
		 */
		cur_batch_base_index += batch_size;
	}

	/* Report mismatches, permission issues, or missing sequences */
	report_sequence_errors(mismatched_seqs_idx, insuffperm_seqs_idx,
						   missing_seqs_idx, seqinfos);

	return sequence_copied;
}

/*
 * Identifies sequences that require synchronization and initiates the
 * synchronization process.
 *
 * Returns true if sequences have been updated.
 */
static bool
LogicalRepSyncSequences(WalReceiverConn *conn)
{
	Relation	rel;
	HeapTuple	tup;
	ScanKeyData skey[1];
	SysScanDesc scan;
	Oid			subid = MyLogicalRepWorker->subid;
	bool		sequence_copied = false;
	List	   *seqinfos = NIL;
	MemoryContext oldctx;

	Assert(SequenceSyncContext);

	StartTransactionCommand();

	rel = table_open(SubscriptionRelRelationId, AccessShareLock);

	/* Scan for all sequences belonging to this subscription */
	ScanKeyInit(&skey[0],
				Anum_pg_subscription_rel_srsubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 1, skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_subscription_rel subrel;
		LogicalRepSequenceInfo *seq;
		Relation	sequence_rel;
		char		relstate;

		CHECK_FOR_INTERRUPTS();

		subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

		sequence_rel = try_table_open(subrel->srrelid, RowExclusiveLock);

		/* Skip if sequence was dropped concurrently */
		if (!sequence_rel)
			continue;

		/* Skip if the relation is not a sequence */
		if (sequence_rel->rd_rel->relkind != RELKIND_SEQUENCE)
		{
			table_close(sequence_rel, NoLock);
			continue;
		}

		relstate = subrel->srsubstate;

		Assert(relstate == SUBREL_STATE_INIT || relstate == SUBREL_STATE_READY);

		/*
		 * Worker needs to process sequences across transaction boundary, so
		 * allocate them under SequenceSyncContext.
		 */
		oldctx = MemoryContextSwitchTo(SequenceSyncContext);
		seq = palloc0_object(LogicalRepSequenceInfo);
		seq->localrelid = subrel->srrelid;
		seq->nspname = get_namespace_name(RelationGetNamespace(sequence_rel));
		seq->seqname = pstrdup(RelationGetRelationName(sequence_rel));
		seq->relstate = relstate;
		seqinfos = lappend(seqinfos, seq);
		MemoryContextSwitchTo(oldctx);

		table_close(sequence_rel, NoLock);
	}

	/* Cleanup */
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	/*
	 * Exit early if no catalog entries found, likely due to concurrent drops.
	 */
	if (!seqinfos)
		return false;

	/* Process sequences */
	sequence_copied = copy_sequences(conn, seqinfos);

	return sequence_copied;
}

/*
 * Execute the sequence sync with error handling. Disable the subscription,
 * if required.
 *
 * Note that we don't handle FATAL errors which are probably because of system
 * resource error and are not repeatable.
 */
static void
start_sequence_sync(void)
{
	Assert(am_sequencesync_worker());

	PG_TRY();
	{
		char	   *err;
		bool		must_use_password;
		StringInfoData app_name;
		long		sleep_ms = SEQSYNC_MIN_SLEEP_MS;

		/* Is the use of a password mandatory? */
		must_use_password = MySubscription->passwordrequired &&
			!MySubscription->ownersuperuser;

		initStringInfo(&app_name);
		appendStringInfo(&app_name, "pg_%u_sequence_sync_" UINT64_FORMAT,
						 MySubscription->oid, GetSystemIdentifier());

		/*
		 * Establish the connection to the publisher for sequence
		 * synchronization.
		 */
		LogRepWorkerWalRcvConn =
			walrcv_connect(MySubscription->conninfo, true, true,
						   must_use_password,
						   app_name.data, &err);
		if (LogRepWorkerWalRcvConn == NULL)
			ereport(ERROR,
					errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("sequencesync worker for subscription \"%s\" could not connect to the publisher: %s",
						   MySubscription->name, err));

		pfree(app_name.data);

		/*
		 * Init the SequenceSyncContext which we clean up after each sequence
		 * synchronization.
		 */
		SequenceSyncContext = AllocSetContextCreate(ApplyContext,
													"SequenceSyncContext",
													ALLOCSET_DEFAULT_SIZES);

		for (;;)
		{
			bool		sequence_copied = false;
			MemoryContext oldctx;

			CHECK_FOR_INTERRUPTS();

			/* Process any invalidation messages that might have accumulated */
			AcceptInvalidationMessages();
			maybe_reread_subscription();

			/*
			 * Perform sequence synchronization under SequenceSyncContext and
			 * reset it each cycle to avoid manual memory management.
			 */
			oldctx = MemoryContextSwitchTo(SequenceSyncContext);

			/*
			 * Synchronize all sequences (both READY and INIT states).
			 */
			sequence_copied = LogicalRepSyncSequences(LogRepWorkerWalRcvConn);

			MemoryContextReset(SequenceSyncContext);
			MemoryContextSwitchTo(oldctx);

			/*
			 * Adjust sleep interval based on whether sequences were copied
			 * over
			 */
			if (sequence_copied)
			{
				sleep_ms = SEQSYNC_MIN_SLEEP_MS;
			}
			else
			{
				/*
				 * Double the sleep time, but not beyond the maximum allowable
				 * value.
				 */
				sleep_ms = Min(sleep_ms * 2, SEQSYNC_MAX_SLEEP_MS);
			}

			/* Sleep for the configured interval */
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 sleep_ms,
							 WAIT_EVENT_LOGICAL_SEQSYNC_MAIN);
			ResetLatch(MyLatch);
		}
	}
	PG_CATCH();
	{
		if (MySubscription->disableonerr)
			DisableSubscriptionAndExit();
		else
		{
			/*
			 * Report the worker failed during sequence synchronization. Abort
			 * the current transaction so that the stats message is sent in an
			 * idle state.
			 */
			AbortOutOfAnyTransaction();
			pgstat_report_subscription_error(MySubscription->oid);

			PG_RE_THROW();
		}
	}
	PG_END_TRY();
}

/* Logical Replication sequencesync worker entry point */
void
SequenceSyncWorkerMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);

	SetupApplyOrSyncWorker(worker_slot);

	start_sequence_sync();

	FinishSyncWorker();
}
