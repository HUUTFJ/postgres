/*-------------------------------------------------------------------------
 * sequencesync.c
 *	  PostgreSQL logical replication: sequence synchronization
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
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
 * Executing the following command resets all sequences in the subscription to
 * state DATASYNC, triggering re-synchronization:
 * ALTER SUBSCRIPTION ... REFRESH SEQUENCES
 *
 * The apply worker periodically scans pg_subscription_rel for sequences in
 * INIT or DATASYNC state. When such sequences are found, it spawns a
 * sequencesync worker to handle synchronization.
 *
 * The sequencesync worker is responsible for synchronizing sequences marked in
 * pg_subscription_rel. It begins by retrieving the list of sequences flagged
 * for synchronization. These sequences are then processed in batches, allowing
 * multiple entries to be synchronized within a single transaction. The worker
 * fetches the current sequence values and page LSNs from the remote publisher,
 * updates the corresponding sequences on the local subscriber, and finally
 * marks each sequence as READY upon successful synchronization.
 *
 * Sequence state transitions follow this pattern:
 *   INIT / DATASYNC → READY
 *
 * To avoid creating too many transactions, up to MAX_SEQUENCES_SYNC_PER_BATCH
 * (100) sequences are synchronized per transaction. The locks on the sequence
 * relation will be periodically released at each transaction commit.
 *
 * XXX: An alternative design was considered where the launcher process would
 * periodically check for sequences that need syncing and then start the
 * sequencesync worker. However, the approach of having the apply worker
 * manage the sequencesync worker was chosen for the following reasons:
 * a) It avoids overloading the launcher, which handles various other
 *    subscription requests.
 * b) It offers a more straightforward path for extending support for
 *    incremental sequence synchronization.
 * c) It utilizes the existing tablesync worker code to start the sequencesync
 *    process, thus preventing code duplication in the launcher.
 * d) It simplifies code maintenance by consolidating changes to a single
 *    location rather than multiple components.
 * e) The apply worker can access the sequences that need to be synchronized
 *    from the pg_subscription_rel system catalog. Whereas the launcher process
 *    operates without direct database access so would need a framework to
 *    establish connections with the databases to retrieve the sequences for
 *    synchronization.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/sequence.h"
#include "common/hashfn.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/worker_internal.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "utils/usercontext.h"

#define REMOTE_SEQ_COL_COUNT 11

static HTAB *sequences_to_copy = NULL;

/*
 * Handle sequence synchronization cooperation from the apply worker.
 *
 * Start a sequencesync worker if one is not already running. The active
 * sequencesync worker will handle all pending sequence synchronization. If any
 * sequences remain unsynchronized after it exits, a new worker can be started
 * in the next iteration.
 */
void
ProcessSyncingSequencesForApply(void)
{
	LogicalRepWorker *sequencesync_worker;
	int			nsyncworkers;
	bool		has_pending_sequences;
	bool		started_tx;

	FetchRelationStates(&has_pending_sequences, &started_tx);

	if (started_tx)
	{
		CommitTransactionCommand();
		pgstat_report_stat(true);
	}

	if (!has_pending_sequences)
		return;

	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

	/* Check if there is a sequencesync worker already running? */
	sequencesync_worker = logicalrep_worker_find(MyLogicalRepWorker->subid,
												 InvalidOid,
												 WORKERTYPE_SEQUENCESYNC,
												 true);
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

	launch_sync_worker(nsyncworkers, InvalidOid,
					   &MyLogicalRepWorker->last_seqsync_start_time);
}

/*
 * report_error_sequences
 *
 * Report discrepancies in sequence data between the publisher and subscriber.
 * It identifies sequences that do not have sufficient privileges, as well as
 * sequences that exist on both sides but have mismatched values.
 */
static void
report_error_sequences(StringInfo insuffperm_seqs, StringInfo mismatched_seqs)
{
	StringInfo	combined_error_detail = makeStringInfo();
	StringInfo	combined_error_hint = makeStringInfo();

	if (insuffperm_seqs->len)
	{
		appendStringInfo(combined_error_detail, "Insufficient permission for sequence(s): (%s).",
						 insuffperm_seqs->data);
		appendStringInfoString(combined_error_hint, "Grant permissions for the sequence(s).");
	}

	if (mismatched_seqs->len)
	{
		if (insuffperm_seqs->len)
		{
			appendStringInfo(combined_error_detail, "; mismatched sequence(s) on subscriber: (%s).",
							 mismatched_seqs->data);
			appendStringInfoString(combined_error_hint, " For mismatched sequences, alter or re-create local sequences to have matching parameters as publishers.");
		}
		else
		{
			appendStringInfo(combined_error_detail, "Mismatched sequence(s) on subscriber: (%s).",
							 mismatched_seqs->data);
			appendStringInfoString(combined_error_hint, "For mismatched sequences, alter or re-create local sequences to have matching parameters as publishers");
		}
	}

	ereport(ERROR,
			errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("logical replication sequence synchronization failed for subscription \"%s\"", MySubscription->name),
			errdetail("%s", combined_error_detail->data),
			errhint("%s", combined_error_hint->data));
}

/*
 * Appends a qualified sequence name to a StringInfo buffer. Optionally
 * increments a counter if provided. Used to build comma-separated lists of
 * sequences.
 */
static void
append_sequence_name(StringInfo buf, const char *nspname, const char *seqname,
					 int *count)
{
	if (buf->len > 0)
		appendStringInfoString(buf, ", ");

	appendStringInfo(buf, "\"%s.%s\"", nspname, seqname);

	if (count)
		(*count)++;
}


/*
 * Copy existing data of sequence from the publisher.
 *
 * Fetch the sequence value from the publisher and set the subscriber sequence
 * with the same value.
 */
static void
copy_sequence(TupleTableSlot *slot, LogicalRepSequenceInfo *seqinfo,
			  StringInfo mismatched_seqs, StringInfo insuffperm_seqs,
			  int *succeeded_count, int *mismatched_count, int *skipped_count,
			  int *insuffperm_count)
{
	int			col = 0;
	bool		isnull;
	char	   *nspname;
	char	   *seqname;
	int64		last_value;
	bool		is_called;
	XLogRecPtr	page_lsn;
	Oid			seqtypid;
	int64		seqstart;
	int64		seqmin;
	int64		seqmax;
	int64		seqincrement;
	bool		seqcycle;
	HeapTuple	tup;
	Relation	sequence_rel;
	Form_pg_sequence seqform;
	UserContext ucxt;
	AclResult	aclresult;
	bool		run_as_owner = MySubscription->runasowner;

	CHECK_FOR_INTERRUPTS();

	/* Get sequence information from the fetched tuple */
	nspname = TextDatumGetCString(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqname = TextDatumGetCString(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	last_value = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	is_called = DatumGetBool(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	page_lsn = DatumGetLSN(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqtypid = DatumGetObjectId(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqstart = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqincrement = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqmin = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqmax = DatumGetInt64(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	seqcycle = DatumGetBool(slot_getattr(slot, ++col, &isnull));
	Assert(!isnull);

	/* Sanity check */
	Assert(col == REMOTE_SEQ_COL_COUNT);

	/* Get the local sequence object */
	sequence_rel = try_table_open(seqinfo->localrelid, RowExclusiveLock);
	tup = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seqinfo->localrelid));
	if (!sequence_rel || !HeapTupleIsValid(tup))
	{
		(*skipped_count)++;
		elog(LOG, "skip synchronization of sequence \"%s.%s\" because it has been dropped concurrently",
			 nspname, seqname);
		return;
	}

	/* Skip if the entry is no longer valid */
	if (!seqinfo->entry_valid)
	{
		ReleaseSysCache(tup);
		table_close(sequence_rel, RowExclusiveLock);
		(*skipped_count)++;
		ereport(LOG, errmsg("skip synchronization of sequence \"%s.%s\" because it has been altered concurrently",
							nspname, seqname));
		return;
	}

	seqform = (Form_pg_sequence) GETSTRUCT(tup);

	/* Update the sequence only if the parameters are identical */
	if (seqform->seqtypid == seqtypid &&
		seqform->seqmin == seqmin && seqform->seqmax == seqmax &&
		seqform->seqcycle == seqcycle &&
		seqform->seqstart == seqstart &&
		seqform->seqincrement == seqincrement)
	{
		if (!run_as_owner)
			SwitchToUntrustedUser(seqinfo->seqowner, &ucxt);

		/* Check for sufficient permissions */
		aclresult = pg_class_aclcheck(seqinfo->localrelid, GetUserId(), ACL_UPDATE);

		if (!run_as_owner)
			RestoreUserContext(&ucxt);

		if (aclresult != ACLCHECK_OK)
		{
			append_sequence_name(insuffperm_seqs, nspname, seqname,
								 insuffperm_count);
			ReleaseSysCache(tup);
			table_close(sequence_rel, RowExclusiveLock);
			return;
		}

		SetSequence(seqinfo->localrelid, last_value, is_called);
		(*succeeded_count)++;

		ereport(DEBUG1,
				errmsg_internal("logical replication synchronization for subscription \"%s\", sequence \"%s.%s\" has finished",
								MySubscription->name, nspname, seqname));

		UpdateSubscriptionRelState(MySubscription->oid, seqinfo->localrelid,
								   SUBREL_STATE_READY, page_lsn, false);
	}
	else
		append_sequence_name(mismatched_seqs, nspname, seqname,
							 mismatched_count);

	ReleaseSysCache(tup);
	table_close(sequence_rel, NoLock);
}

/*
 * Copy existing data of sequences from the publisher. Caller is responsible
 * for locking the local relation.
 */
static void
copy_sequences(WalReceiverConn *conn, Oid subid)
{
	int			total_seqs = hash_get_num_entries(sequences_to_copy);
	int			current_index = 0;
	StringInfo	mismatched_seqs = makeStringInfo();
	StringInfo	missing_seqs = makeStringInfo();
	StringInfo	insuffperm_seqs = makeStringInfo();
	StringInfo	seqstr = makeStringInfo();
	StringInfo	cmd = makeStringInfo();
	HASH_SEQ_STATUS status;
	LogicalRepSequenceInfo *entry;

#define MAX_SEQUENCES_SYNC_PER_BATCH 100

	ereport(LOG,
			errmsg("logical replication sequence synchronization for subscription \"%s\" - total unsynchronized: %d",
				   MySubscription->name, total_seqs));

	while (current_index < total_seqs)
	{
		Oid			seqRow[REMOTE_SEQ_COL_COUNT] = {TEXTOID, TEXTOID, INT8OID,
		BOOLOID, LSNOID, OIDOID, INT8OID, INT8OID, INT8OID, INT8OID, BOOLOID};
		int			batch_size = 0;
		int			batch_succeeded_count = 0;
		int			batch_mismatched_count = 0;
		int			batch_skipped_count = 0;
		int			batch_insuffperm_count = 0;

		WalRcvExecResult *res;
		TupleTableSlot *slot;

		StartTransactionCommand();
		hash_seq_init(&status, sequences_to_copy);

		/* Collect a batch of sequences */
		while ((entry = (LogicalRepSequenceInfo *) hash_seq_search(&status)) != NULL)
		{
			if (entry->remote_seq_queried)
				continue;

			if (seqstr->len > 0)
				appendStringInfoString(seqstr, ", ");

			appendStringInfo(seqstr, "(\'%s\', \'%s\')", entry->nspname, entry->seqname);
			entry->remote_seq_queried = true;

			batch_size++;
			if (batch_size == MAX_SEQUENCES_SYNC_PER_BATCH ||
				(current_index + batch_size == total_seqs))
				break;
		}

		hash_seq_term(&status);

		appendStringInfo(cmd,
						 "SELECT s.schname, s.seqname, ps.*, seq.seqtypid,\n"
						 "       seq.seqstart, seq.seqincrement, seq.seqmin,\n"
						 "       seq.seqmax, seq.seqcycle\n"
						 "FROM ( VALUES %s ) AS s (schname, seqname)\n"
						 "JOIN pg_namespace n ON n.nspname = s.schname\n"
						 "JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = s.seqname\n"
						 "JOIN pg_sequence seq ON seq.seqrelid = c.oid\n"
						 "JOIN LATERAL pg_get_sequence_data(seq.seqrelid) AS ps ON true\n"
						 "ORDER BY s.schname, s.seqname\n",
						 seqstr->data);

		res = walrcv_exec(conn, cmd->data, lengthof(seqRow), seqRow);
		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("could not receive list of sequence information from the publisher: %s",
						   res->err));

		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		{
			LogicalRepSequenceInfo *seqinfo;
			LogicalRepSeqHashKey key;
			bool		isnull;
			bool		found;

			CHECK_FOR_INTERRUPTS();

			if (ConfigReloadPending)
			{
				ConfigReloadPending = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			key.nspname = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
			Assert(!isnull);

			key.seqname = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
			Assert(!isnull);

			seqinfo = hash_search(sequences_to_copy, &key, HASH_FIND, &found);
			Assert(seqinfo);

			copy_sequence(slot, seqinfo, mismatched_seqs,
						  insuffperm_seqs, &batch_succeeded_count,
						  &batch_mismatched_count, &batch_skipped_count,
						  &batch_insuffperm_count);

			/* Remove successfully processed sequence */
			if (!hash_search(sequences_to_copy, &key, HASH_REMOVE, NULL))
				elog(ERROR, "hash table corrupted");
		}

		ExecDropSingleTupleTableSlot(slot);
		walrcv_clear_result(res);
		resetStringInfo(seqstr);
		resetStringInfo(cmd);

		ereport(LOG,
				errmsg("logical replication sequence synchronization for subscription \"%s\" - batch #%d = %d attempted, %d succeeded, %d skipped, %d mismatched, %d insufficient permission, %d missing from publisher",
					   MySubscription->name, (current_index / MAX_SEQUENCES_SYNC_PER_BATCH) + 1, batch_size,
					   batch_succeeded_count, batch_skipped_count, batch_mismatched_count, batch_insuffperm_count,
					   batch_size - (batch_succeeded_count + batch_skipped_count + batch_mismatched_count + batch_insuffperm_count)));

		/* Commit this batch, and prepare for next batch */
		CommitTransactionCommand();

		/*
		 * current_indexes is not incremented sequentially because some
		 * sequences may be missing, and the number of fetched rows may not
		 * match the batch size. The `hash_search` with HASH_REMOVE takes care
		 * of the count.
		 */
		current_index += batch_size;
	}

	/*
	 * Any sequences remaining in the hash table were not found on the
	 * publisher. This is because they were included in a query
	 * (remote_seq_queried) but were not returned in the result set.
	 */
	hash_seq_init(&status, sequences_to_copy);
	while ((entry = (LogicalRepSequenceInfo *) hash_seq_search(&status)) != NULL)
	{
		Assert(entry->remote_seq_queried);
		append_sequence_name(missing_seqs, entry->nspname, entry->seqname, NULL);
	}

	/* Log missing sequences if any */
	if (missing_seqs->len)
		ereport(LOG,
				errmsg_internal("sequences not found on publisher removed from resynchronization: (%s)",
								missing_seqs->data));

	/* Report errors if mismatches or permission issues occurred */
	if (insuffperm_seqs->len || mismatched_seqs->len)
		report_error_sequences(insuffperm_seqs, mismatched_seqs);

	destroyStringInfo(missing_seqs);
	destroyStringInfo(mismatched_seqs);
	destroyStringInfo(insuffperm_seqs);
}

/*
 * Relcache invalidation callback
 */
static void
sequencesync_list_invalidate_cb(Datum arg, Oid reloid)
{
	HASH_SEQ_STATUS status;
	LogicalRepSequenceInfo *entry;

	/* Quick exit if no sequence is listed yet */
	if (hash_get_num_entries(sequences_to_copy) == 0)
		return;

	if (reloid != InvalidOid)
	{
		hash_seq_init(&status, sequences_to_copy);

		while ((entry = (LogicalRepSequenceInfo *) hash_seq_search(&status)) != NULL)
		{
			if (entry->localrelid == reloid)
			{
				entry->entry_valid = false;
				hash_seq_term(&status);
				break;
			}
		}
	}
	else
	{
		/* invalidate all entries */
		hash_seq_init(&status, sequences_to_copy);
		while ((entry = (LogicalRepSequenceInfo *) hash_seq_search(&status)) != NULL)
			entry->entry_valid = false;
	}
}

static uint32
LogicalRepSeqHash(const void *key, Size keysize)
{
	const LogicalRepSeqHashKey *k = (const LogicalRepSeqHashKey *) key;
	uint32		h1 = string_hash(k->nspname, strlen(k->nspname));
	uint32		h2 = string_hash(k->seqname, strlen(k->seqname));

	return h1 ^ h2;
}

static int
LogicalRepSeqMatchFunc(const void *key1, const void *key2, Size keysize)
{
	int			cmp;
	const LogicalRepSeqHashKey *k1 = (const LogicalRepSeqHashKey *) key1;
	const LogicalRepSeqHashKey *k2 = (const LogicalRepSeqHashKey *) key2;

	/* Compare by namespace name first */
	cmp = strcmp(k1->nspname, k2->nspname);
	if (cmp != 0)
		return cmp;

	/* If namespace names are equal, compare by sequence name */
	return strcmp(k1->seqname, k2->seqname);
}

/*
 * Start syncing the sequences in the sequencesync worker.
 */
static void
LogicalRepSyncSequences(void)
{
	char	   *err;
	bool		must_use_password;
	Relation	rel;
	HeapTuple	tup;
	ScanKeyData skey[2];
	SysScanDesc scan;
	Oid			subid = MyLogicalRepWorker->subid;
	StringInfoData app_name;
	HASHCTL		ctl;
	bool		found;
	HASH_SEQ_STATUS hash_seq;
	LogicalRepSequenceInfo *seq_entry;

	ctl.keysize = sizeof(LogicalRepSeqHashKey);
	ctl.entrysize = sizeof(LogicalRepSequenceInfo);
	ctl.hcxt = CacheMemoryContext;
	ctl.hash = LogicalRepSeqHash;
	ctl.match = LogicalRepSeqMatchFunc;
	sequences_to_copy = hash_create("Logical replication sequences", 256, &ctl,
									HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(sequencesync_list_invalidate_cb,
								  (Datum) 0);

	StartTransactionCommand();

	rel = table_open(SubscriptionRelRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_subscription_rel_srsubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	ScanKeyInit(&skey[1],
				Anum_pg_subscription_rel_srsubstate,
				BTEqualStrategyNumber, F_CHARNE,
				CharGetDatum(SUBREL_STATE_READY));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 2, skey);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_subscription_rel subrel;
		char		relkind;
		Relation	sequence_rel;
		LogicalRepSeqHashKey key;
		MemoryContext oldctx;

		CHECK_FOR_INTERRUPTS();

		subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

		/* Skip if the relation is not a sequence */
		relkind = get_rel_relkind(subrel->srrelid);
		if (relkind != RELKIND_SEQUENCE)
			continue;

		/* Skip if sequence was dropped concurrently */
		sequence_rel = try_table_open(subrel->srrelid, RowExclusiveLock);
		if (!sequence_rel)
			continue;

		key.seqname = RelationGetRelationName(sequence_rel);
		key.nspname = get_namespace_name(RelationGetNamespace(sequence_rel));

		/* Allocate the tracking info in a permanent memory context. */
		oldctx = MemoryContextSwitchTo(CacheMemoryContext);

		seq_entry = hash_search(sequences_to_copy, &key, HASH_ENTER, &found);
		Assert(!found);

		memset(seq_entry, 0, sizeof(LogicalRepSequenceInfo));

		seq_entry->seqname = pstrdup(key.seqname);
		seq_entry->nspname = pstrdup(key.nspname);
		seq_entry->localrelid = subrel->srrelid;
		seq_entry->remote_seq_queried = false;
		seq_entry->seqowner = sequence_rel->rd_rel->relowner;
		seq_entry->entry_valid = true;

		MemoryContextSwitchTo(oldctx);

		table_close(sequence_rel, RowExclusiveLock);
	}

	/* Cleanup */
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	/* Is the use of a password mandatory? */
	must_use_password = MySubscription->passwordrequired &&
		!MySubscription->ownersuperuser;

	initStringInfo(&app_name);
	appendStringInfo(&app_name, "pg_%u_sequence_sync_" UINT64_FORMAT,
					 MySubscription->oid, GetSystemIdentifier());

	/*
	 * Establish the connection to the publisher for sequence synchronization.
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

	/* If there are any sequences that need to be copied */
	if (hash_get_num_entries(sequences_to_copy))
	{
		copy_sequences(LogRepWorkerWalRcvConn, subid);

		hash_seq_init(&hash_seq, sequences_to_copy);
		while ((seq_entry = hash_seq_search(&hash_seq)) != NULL)
		{
			pfree(seq_entry->seqname);
			pfree(seq_entry->nspname);
		}
	}

	hash_destroy(sequences_to_copy);
	sequences_to_copy = NULL;
}

/*
 * Execute the initial sync with error handling. Disable the subscription,
 * if required.
 *
 * Allocate the slot name in long-lived context on return. Note that we don't
 * handle FATAL errors which are probably because of system resource error and
 * are not repeatable.
 */
static void
start_sequence_sync()
{
	Assert(am_sequencesync_worker());

	PG_TRY();
	{
		/* Call initial sync. */
		LogicalRepSyncSequences();
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
			pgstat_report_subscription_error(MySubscription->oid,
											 WORKERTYPE_SEQUENCESYNC);

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

	FinishSyncWorker(WORKERTYPE_SEQUENCESYNC);
}
