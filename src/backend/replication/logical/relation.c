/*-------------------------------------------------------------------------
 * relation.c
 *	   PostgreSQL logical replication relation mapping cache
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/relation.c
 *
 * NOTES
 *	  Routines in this file mainly have to do with mapping the properties
 *	  of local replication target relations to the properties of their
 *	  remote counterpart.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_subscription_rel.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static MemoryContext LogicalRepRelMapContext = NULL;

static HTAB *LogicalRepRelMap = NULL;

/*
 * Partition map (LogicalRepPartMap)
 *
 * When a partitioned table is used as replication target, replicated
 * operations are actually performed on its leaf partitions, which requires
 * the partitions to also be mapped to the remote relation.  Parent's entry
 * (LogicalRepRelMapEntry) cannot be used as-is for all partitions, because
 * individual partitions may have different attribute numbers, which means
 * attribute mappings to remote relation's attributes must be maintained
 * separately for each partition.
 */
static MemoryContext LogicalRepPartMapContext = NULL;
static HTAB *LogicalRepPartMap = NULL;
typedef struct LogicalRepPartMapEntry
{
	Oid			partoid;		/* LogicalRepPartMap's key */
	LogicalRepRelMapEntry relmapentry;
} LogicalRepPartMapEntry;

static Oid	FindLogicalRepLocalIndex(Relation localrel, LogicalRepRelation *remoterel,
									 AttrMap *attrMap);

/*
 * Relcache invalidation callback for our relation map cache.
 */
static void
logicalrep_relmap_invalidate_cb(Datum arg, Oid reloid)
{
	LogicalRepRelMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepRelMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		/* TODO, use inverse lookup hashtable? */
		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->localreloid == reloid)
			{
				entry->localrelvalid = false;
				hash_seq_term(&status);
				break;
			}
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
			entry->localrelvalid = false;
	}
}

/*
 * Initialize the relation map cache.
 */
static void
logicalrep_relmap_init(void)
{
	HASHCTL		ctl;

	if (!LogicalRepRelMapContext)
		LogicalRepRelMapContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "LogicalRepRelMapContext",
								  ALLOCSET_DEFAULT_SIZES);

	/* Initialize the relation hash table. */
	ctl.keysize = sizeof(LogicalRepRelId);
	ctl.entrysize = sizeof(LogicalRepRelMapEntry);
	ctl.hcxt = LogicalRepRelMapContext;

	LogicalRepRelMap = hash_create("logicalrep relation map cache", 128, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(logicalrep_relmap_invalidate_cb,
								  (Datum) 0);
}

/*
 * Release local index list
 */
static void
free_local_unique_indexes(LogicalRepRelMapEntry *entry)
{
	Assert(am_leader_apply_worker());

	foreach_ptr(LogicalRepSubscriberIdx, idxinfo, entry->local_unique_indexes)
		bms_free(idxinfo->indexkeys);

	list_free_deep(entry->local_unique_indexes);
	entry->local_unique_indexes = NIL;
}

/*
 * Release foreign key list
 */
static void
free_local_fkeys(LogicalRepRelMapEntry *entry)
{
	Assert(am_leader_apply_worker());

	foreach_ptr(LogicalRepSubscriberFK, fkinfo, entry->local_fkeys)
		bms_free(fkinfo->conkeys);

	list_free_deep(entry->local_fkeys);
	entry->local_fkeys = NIL;
}

/*
 * Free the entry of a relation map cache.
 */
static void
logicalrep_relmap_free_entry(LogicalRepRelMapEntry *entry)
{
	LogicalRepRelation *remoterel;

	remoterel = &entry->remoterel;

	pfree(remoterel->nspname);
	pfree(remoterel->relname);

	if (remoterel->natts > 0)
	{
		int			i;

		for (i = 0; i < remoterel->natts; i++)
			pfree(remoterel->attnames[i]);

		pfree(remoterel->attnames);
		pfree(remoterel->atttyps);
	}
	bms_free(remoterel->attkeys);

	if (entry->attrmap)
		free_attrmap(entry->attrmap);

	if (entry->local_unique_indexes != NIL)
		free_local_unique_indexes(entry);

	if (entry->local_fkeys != NIL)
		free_local_fkeys(entry);
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the publisher to update
 * our expected view of incoming data from said publisher.
 *
 * Note that we do not check the user-defined constraints here. PostgreSQL has
 * already assumed that CHECK constraints' conditions are immutable and here
 * follows the rule.
 */
void
logicalrep_relmap_update(LogicalRepRelation *remoterel)
{
	MemoryContext oldctx;
	LogicalRepRelMapEntry *entry;
	bool		found;
	int			i;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(LogicalRepRelMap, &remoterel->remoteid,
						HASH_ENTER, &found);

	if (found)
		logicalrep_relmap_free_entry(entry);

	memset(entry, 0, sizeof(LogicalRepRelMapEntry));

	/* Make cached copy of the data */
	oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
	entry->remoterel.remoteid = remoterel->remoteid;
	entry->remoterel.nspname = pstrdup(remoterel->nspname);
	entry->remoterel.relname = pstrdup(remoterel->relname);
	entry->remoterel.natts = remoterel->natts;
	entry->remoterel.attnames = palloc_array(char *, remoterel->natts);
	entry->remoterel.atttyps = palloc_array(Oid, remoterel->natts);
	for (i = 0; i < remoterel->natts; i++)
	{
		entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
		entry->remoterel.atttyps[i] = remoterel->atttyps[i];
	}
	entry->remoterel.replident = remoterel->replident;

	/*
	 * XXX The walsender currently does not transmit the relkind of the remote
	 * relation when replicating changes. Since we support replicating only
	 * table changes at present, we default to initializing relkind as
	 * RELKIND_RELATION. This is needed in CheckSubscriptionRelkind() to check
	 * if the publisher and subscriber relation kinds are compatible.
	 */
	entry->remoterel.relkind =
		(remoterel->relkind == 0) ? RELKIND_RELATION : remoterel->relkind;

	entry->remoterel.attkeys = bms_copy(remoterel->attkeys);

	entry->parallel_safe = LOGICALREP_PARALLEL_UNKNOWN;
	MemoryContextSwitchTo(oldctx);
}

/*
 * Find attribute index in TupleDesc struct by attribute name.
 *
 * Returns -1 if not found.
 */
static int
logicalrep_rel_att_by_name(LogicalRepRelation *remoterel, const char *attname)
{
	int			i;

	for (i = 0; i < remoterel->natts; i++)
	{
		if (strcmp(remoterel->attnames[i], attname) == 0)
			return i;
	}

	return -1;
}

/*
 * Returns a comma-separated string of attribute names based on the provided
 * relation and bitmap indicating which attributes to include.
 */
static char *
logicalrep_get_attrs_str(LogicalRepRelation *remoterel, Bitmapset *atts)
{
	StringInfoData attsbuf;
	int			attcnt = 0;
	int			i = -1;

	Assert(!bms_is_empty(atts));

	initStringInfo(&attsbuf);

	while ((i = bms_next_member(atts, i)) >= 0)
	{
		attcnt++;
		if (attcnt > 1)
			/* translator: This is a separator in a list of entity names. */
			appendStringInfoString(&attsbuf, _(", "));

		appendStringInfo(&attsbuf, _("\"%s\""), remoterel->attnames[i]);
	}

	return attsbuf.data;
}

/*
 * If attempting to replicate missing or generated columns, report an error.
 * Prioritize 'missing' errors if both occur though the prioritization is
 * arbitrary.
 */
static void
logicalrep_report_missing_or_gen_attrs(LogicalRepRelation *remoterel,
									   Bitmapset *missingatts,
									   Bitmapset *generatedatts)
{
	if (!bms_is_empty(missingatts))
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("logical replication target relation \"%s.%s\" is missing replicated column: %s",
							  "logical replication target relation \"%s.%s\" is missing replicated columns: %s",
							  bms_num_members(missingatts),
							  remoterel->nspname,
							  remoterel->relname,
							  logicalrep_get_attrs_str(remoterel,
													   missingatts)));

	if (!bms_is_empty(generatedatts))
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg_plural("logical replication target relation \"%s.%s\" has incompatible generated column: %s",
							  "logical replication target relation \"%s.%s\" has incompatible generated columns: %s",
							  bms_num_members(generatedatts),
							  remoterel->nspname,
							  remoterel->relname,
							  logicalrep_get_attrs_str(remoterel,
													   generatedatts)));
}

/*
 * Check if replica identity matches and mark the updatable flag.
 *
 * We allow for stricter replica identity (fewer columns) on subscriber as
 * that will not stop us from finding unique tuple. IE, if publisher has
 * identity (id,timestamp) and subscriber just (id) this will not be a
 * problem, but in the opposite scenario it will.
 *
 * We just mark the relation entry as not updatable here if the local
 * replica identity is found to be insufficient for applying
 * updates/deletes (inserts don't care!) and leave it to
 * check_relation_updatable() to throw the actual error if needed.
 */
static void
logicalrep_rel_mark_updatable(LogicalRepRelMapEntry *entry)
{
	Bitmapset  *idkey;
	LogicalRepRelation *remoterel = &entry->remoterel;
	int			i;

	entry->updatable = true;

	idkey = RelationGetIndexAttrBitmap(entry->localrel,
									   INDEX_ATTR_BITMAP_IDENTITY_KEY);
	/* fallback to PK if no replica identity */
	if (idkey == NULL)
	{
		idkey = RelationGetIndexAttrBitmap(entry->localrel,
										   INDEX_ATTR_BITMAP_PRIMARY_KEY);

		/*
		 * If no replica identity index and no PK, the published table must
		 * have replica identity FULL.
		 */
		if (idkey == NULL && remoterel->replident != REPLICA_IDENTITY_FULL)
			entry->updatable = false;
	}

	i = -1;
	while ((i = bms_next_member(idkey, i)) >= 0)
	{
		int			attnum = i + FirstLowInvalidHeapAttributeNumber;

		if (!AttrNumberIsForUserDefinedAttr(attnum))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("logical replication target relation \"%s.%s\" uses "
							"system columns in REPLICA IDENTITY index",
							remoterel->nspname, remoterel->relname)));

		attnum = AttrNumberGetAttrOffset(attnum);

		if (entry->attrmap->attnums[attnum] < 0 ||
			!bms_is_member(entry->attrmap->attnums[attnum], remoterel->attkeys))
		{
			entry->updatable = false;
			break;
		}
	}
}

/*
 * Collect all local unique indexes that can be used for dependency tracking.
 */
static void
collect_local_indexes(LogicalRepRelMapEntry *entry)
{
	List	   *idxlist;

	if (entry->local_unique_indexes != NIL)
		free_local_unique_indexes(entry);

	entry->local_unique_indexes_collected = true;

	idxlist = RelationGetIndexList(entry->localrel);

	/* Quick exit if there are no indexes */
	if (idxlist == NIL)
		return;

	/* Iterate indexes to list all usable indexes */
	foreach_oid(idxoid, idxlist)
	{
		Relation	idxrel;
		int			indnkeys;
		AttrMap	   *attrmap;
		Bitmapset  *indexkeys = NULL;
		bool		suitable = true;

		idxrel = index_open(idxoid, AccessShareLock);

		/*
		 * Check whether the index can be used for the dependency tracking.
		 *
		 * For simplification, the same condition as REPLICA IDENTITY FULL,
		 * plus it must be a unique index.
		 */
		if (!(idxrel->rd_index->indisunique &&
			  IsIndexUsableForReplicaIdentityFull(idxrel, entry->attrmap)))
		{
			index_close(idxrel, AccessShareLock);
			continue;
		}

		indnkeys = idxrel->rd_index->indnkeyatts;
		attrmap = entry->attrmap;

		Assert(indnkeys);

		/* Seek each attributes and add to a Bitmap */
		for (int i = 0; i < indnkeys; i++)
		{
			AttrNumber localcol = idxrel->rd_index->indkey.values[i];
			AttrNumber remotecol;

			/*
			 * XXX: Mark a relation as parallel-unsafe if it has expression
			 * indexes because we cannot compute the hash value for the
			 * dependency tracking. For safety, transactions that modify such
			 * tables can wait for applications till the lastly dispatched
			 * transaction is committed.
			 */
			if (!AttributeNumberIsValid(localcol))
			{
				entry->parallel_safe = LOGICALREP_PARALLEL_RESTRICTED;
				suitable = false;
				break;
			}

			remotecol = attrmap->attnums[AttrNumberGetAttrOffset(localcol)];

			/*
			 * Skip if the column does not exist on publisher node. In this
			 * case the replicated tuples always have NULL or default value.
			 */
			if (remotecol < 0)
			{
				suitable = false;
				break;
			}

			/* Checks are passed, remember the attribute */
			indexkeys = bms_add_member(indexkeys, remotecol);
		}

		index_close(idxrel, AccessShareLock);

		/*
		 * Skip using the index if it is not suitable. This can happen if
		 * 1) one of the columns does not exist on the publisher side, or
		 * 2) there is an expression column.
		 */
		if (!suitable)
		{
			if (indexkeys)
				bms_free(indexkeys);

			continue;
		}

		/* This index is usable, store on memory */
		if (indexkeys)
		{
			MemoryContext				oldctx;
			LogicalRepSubscriberIdx	   *idxinfo;

			oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
			idxinfo = palloc(sizeof(LogicalRepSubscriberIdx));
			idxinfo->indexoid = idxoid;
			idxinfo->indexkeys = bms_copy(indexkeys);
			entry->local_unique_indexes =
				lappend(entry->local_unique_indexes, idxinfo);

			pfree(indexkeys);
			MemoryContextSwitchTo(oldctx);
		}
	}

	list_free(idxlist);
}

/*
 * Search a relmap entry by local relation OID.
 */
static LogicalRepRelMapEntry *
logicalrep_get_relentry_by_local_oid(Oid localreloid)
{
	HASH_SEQ_STATUS status;
	LogicalRepRelMapEntry *entry = NULL;

	if (LogicalRepRelMap == NULL)
		return NULL;

	/*
	 * Each entry must be checked individually. Because the key of
	 * LogicalRepRelMap is the "remote" relid but we only have the local one.
	 *
	 * Note: This iteration ignores relations that have not yet been registered
	 * in LogicalRepRelMap. It is OK because such relations have not been
	 * modified yet since the subscriber started receiving changes.
	 */
	hash_seq_init(&status, LogicalRepRelMap);
	while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->localreloid == localreloid)
		{
			hash_seq_term(&status);
			return entry;
		}
	}

	return NULL;
}

/*
 * Return true if the FK constraint is always deferred.
 */
static bool
foreign_key_is_always_deferred(Oid conoid)
{
	HeapTuple	tup;
	Form_pg_constraint con;
	bool		deferred;

	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for foreign key %u", conoid);

	con = (Form_pg_constraint) GETSTRUCT(tup);
	deferred = con->condeferrable && con->condeferred;
	ReleaseSysCache(tup);

	return deferred;
}

/*
 * Collect all local foreign keys that can be used for dependency tracking.
 */
static void
collect_local_fkeys(LogicalRepRelMapEntry *entry)
{
	List	   *fkeys;

	if (entry->local_fkeys != NIL)
		free_local_fkeys(entry);

	entry->local_fkeys_collected = true;

	/*
	 * Get the list of foreign keys for the relation.
	 *
	 * XXX: Apart from RelationGetIndexList(), the returned list is a part of
	 * relcache and must be copied before doing anything. See comments atop
	 * RelationGetFKeyList().
	 */
	fkeys = copyObject(RelationGetFKeyList(entry->localrel));

	/* Quick exit if there are no foreign keys */
	if (fkeys == NIL)
		return;

	foreach_ptr(ForeignKeyCacheInfo, fk, fkeys)
	{
		LogicalRepRelMapEntry *refentry;
		Bitmapset  *fkkeys = NULL;
		bool		suitable = true;

		/*
		 * Skip NOT ENFORCED constraints because they won't be checked at any
		 * times.
		 */
		if (!fk->conenforced)
			continue;

		/*
		 * Skip if the foreign key constraint is always deferred. The commit
		 * ordering is always preserved thus the constraint can be checked in
		 * correct order.
		 */
		if (foreign_key_is_always_deferred(fk->conoid))
			continue;

		/* Find the referenced relation by the local OID */
		refentry = logicalrep_get_relentry_by_local_oid(fk->confrelid);

		/*
		 * Skip if the referenced relation is not the target of this
		 * subscription.
		 */
		if (!refentry)
			continue;

		/* Seek each attributes and add to a Bitmap */
		for (int i = 0; i < fk->nkeys; i++)
		{
			AttrNumber	localcol = fk->conkey[i];
			int			remotecol =
				entry->attrmap->attnums[AttrNumberGetAttrOffset(localcol)];

			/* Skip if the column does not exist on publisher node */
			if (remotecol < 0)
			{
				suitable = false;
				break;
			}

			/*
			 * XXX: What if the FK column is specified with different order?
			 * E.g., there is a table (a, b) and other table refers like
			 * REFERENCES (b, a). Will it work correctly?
			 */
			fkkeys = bms_add_member(fkkeys, remotecol);
		}

		/*
		 * One of the columns does not exist on the publisher side, skip such a
		 * constraint.
		 */
		if (!suitable)
		{
			if (fkkeys)
				bms_free(fkkeys);

			continue;
		}

		if (fkkeys)
		{
			MemoryContext			oldctx;
			LogicalRepSubscriberFK *fkinfo;

			oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
			fkinfo = palloc(sizeof(LogicalRepSubscriberFK));
			fkinfo->conoid = fk->conoid;
			fkinfo->ref_remoteid = refentry->remoterel.remoteid;
			fkinfo->conkeys = bms_copy(fkkeys);

			bms_free(fkkeys);
			entry->local_fkeys = lappend(entry->local_fkeys, fkinfo);
			MemoryContextSwitchTo(oldctx);
		}
	}

	list_free_deep(fkeys);
}

/*
 * Check all local triggers for the relation to see the parallelizability.
 *
 * We regard relations as applicable in parallel if all triggers are immutable.
 * Result is directly set to LogicalRepRelMapEntry::parallel_safe.
 */
static void
check_defined_triggers(LogicalRepRelMapEntry *entry)
{
	TriggerDesc *trigdesc;

	/*
	 * Skip if the parallelizability has already been checked. Possilble if the
	 * relation has expression indexes.
	 */
	if (entry->parallel_safe != LOGICALREP_PARALLEL_UNKNOWN)
		return;

	trigdesc = entry->localrel->trigdesc;

	/* Quick exit if triffer is not defined */
	if (trigdesc == NULL)
	{
		entry->parallel_safe = LOGICALREP_PARALLEL_SAFE;
		return;
	}

	/* Seek triggers one by one to see the volatility */
	for (int i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger *trigger = &trigdesc->triggers[i];

		Assert(OidIsValid(trigger->tgfoid));

		/* Skip if the trigger is not enabled for logical replication */
		if (trigger->tgenabled == TRIGGER_DISABLED ||
			trigger->tgenabled == TRIGGER_FIRES_ON_ORIGIN)
			continue;

		/* Check the volatility of the trigger. Exit if it is not immutable */
		if (func_volatile(trigger->tgfoid) != PROVOLATILE_IMMUTABLE)
		{
			entry->parallel_safe = LOGICALREP_PARALLEL_RESTRICTED;
			return;
		}
	}

	/* All triggers are immutable, set as parallel safe */
	entry->parallel_safe = LOGICALREP_PARALLEL_SAFE;
}

/*
 * Actual workhorse for logicalrep_rel_open().
 *
 * Caller must specify *either* entry or key. If the entry is specified, its
 * attributes are filled and returned. The logical relation is kept opening.
 * If the key is given, the corresponding entry is first searched in the hash
 * table and processed as in the above case. At the end, logical replication is
 * closed.
 */
void
logicalrep_rel_load(LogicalRepRelMapEntry *entry, LogicalRepRelId remoteid,
					LOCKMODE lockmode)
{
	LogicalRepRelation *remoterel;

	Assert((entry && !remoteid) || (!entry && remoteid));

	if (!entry)
	{
		bool	found;

		if (LogicalRepRelMap == NULL)
			logicalrep_relmap_init();

		/* Search for existing entry. */
		entry = hash_search(LogicalRepRelMap, &remoteid,
							HASH_FIND, &found);

		if (!found)
			elog(ERROR, "no relation map entry for remote relation ID %u",
				remoteid);
	}

	remoterel = &entry->remoterel;

	/* Ensure we don't leak a relcache refcount. */
	if (entry->localrel)
		elog(ERROR, "remote relation ID %u is already open", remoteid);

	/*
	 * When opening and locking a relation, pending invalidation messages are
	 * processed which can invalidate the relation.  Hence, if the entry is
	 * currently considered valid, try to open the local relation by OID and
	 * see if invalidation ensues.
	 */
	if (entry->localrelvalid)
	{
		entry->localrel = try_table_open(entry->localreloid, lockmode);
		if (!entry->localrel)
		{
			/* Table was renamed or dropped. */
			entry->localrelvalid = false;
		}
		else if (!entry->localrelvalid)
		{
			/* Note we release the no-longer-useful lock here. */
			table_close(entry->localrel, lockmode);
			entry->localrel = NULL;
		}
	}

	/*
	 * If the entry has been marked invalid since we last had lock on it,
	 * re-open the local relation by name and rebuild all derived data.
	 */
	if (!entry->localrelvalid)
	{
		Oid			relid;
		TupleDesc	desc;
		MemoryContext oldctx;
		int			i;
		Bitmapset  *missingatts;
		Bitmapset  *generatedattrs = NULL;

		/* Release the no-longer-useful attrmap, if any. */
		if (entry->attrmap)
		{
			free_attrmap(entry->attrmap);
			entry->attrmap = NULL;
		}

		/* Try to find and lock the relation by name. */
		relid = RangeVarGetRelid(makeRangeVar(remoterel->nspname,
											  remoterel->relname, -1),
								 lockmode, true);
		if (!OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("logical replication target relation \"%s.%s\" does not exist",
							remoterel->nspname, remoterel->relname)));
		entry->localrel = table_open(relid, NoLock);
		entry->localreloid = relid;

		/* Check for supported relkind. */
		CheckSubscriptionRelkind(entry->localrel->rd_rel->relkind,
								 remoterel->relkind,
								 remoterel->nspname, remoterel->relname);

		/*
		 * Build the mapping of local attribute numbers to remote attribute
		 * numbers and validate that we don't miss any replicated columns as
		 * that would result in potentially unwanted data loss.
		 */
		desc = RelationGetDescr(entry->localrel);
		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		entry->attrmap = make_attrmap(desc->natts);
		MemoryContextSwitchTo(oldctx);

		/* check and report missing attrs, if any */
		missingatts = bms_add_range(NULL, 0, remoterel->natts - 1);
		for (i = 0; i < desc->natts; i++)
		{
			int			attnum;
			Form_pg_attribute attr = TupleDescAttr(desc, i);

			if (attr->attisdropped)
			{
				entry->attrmap->attnums[i] = -1;
				continue;
			}

			attnum = logicalrep_rel_att_by_name(remoterel,
												NameStr(attr->attname));

			entry->attrmap->attnums[i] = attnum;
			if (attnum >= 0)
			{
				/* Remember which subscriber columns are generated. */
				if (attr->attgenerated)
					generatedattrs = bms_add_member(generatedattrs, attnum);

				missingatts = bms_del_member(missingatts, attnum);
			}
		}

		logicalrep_report_missing_or_gen_attrs(remoterel, missingatts,
											   generatedattrs);

		/* be tidy */
		bms_free(generatedattrs);
		bms_free(missingatts);

		/*
		 * Set if the table's replica identity is enough to apply
		 * update/delete.
		 */
		logicalrep_rel_mark_updatable(entry);

		/*
		 * Finding a usable index is an infrequent task. It occurs when an
		 * operation is first performed on the relation, or after invalidation
		 * of the relation cache entry (such as ANALYZE or CREATE/DROP index
		 * on the relation).
		 */
		entry->localindexoid = FindLogicalRepLocalIndex(entry->localrel, remoterel,
														entry->attrmap);

		/*
		 * Leader must also collect all local unique indexes for dependency
		 * tracking.
		 */
		if (am_leader_apply_worker())
		{
			entry->parallel_safe = LOGICALREP_PARALLEL_UNKNOWN;
			collect_local_indexes(entry);
			collect_local_fkeys(entry);
			check_defined_triggers(entry);
		}

		entry->localrelvalid = true;
	}

	if (entry->state != SUBREL_STATE_READY)
		entry->state = GetSubscriptionRelState(MySubscription->oid,
											   entry->localreloid,
											   &entry->statelsn);

	if (remoteid)
		logicalrep_rel_close(entry, lockmode);
}

/*
 * Open the local relation associated with the remote one.
 *
 * Rebuilds the Relcache mapping if it was invalidated by local DDL.
 */
LogicalRepRelMapEntry *
logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
{
	LogicalRepRelMapEntry *entry;
	bool		found;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(ERROR, "no relation map entry for remote relation ID %u",
			 remoteid);

	logicalrep_rel_load(entry, 0, lockmode);

	return entry;
}

/*
 * Close the previously opened logical relation.
 */
void
logicalrep_rel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode)
{
	table_close(rel->localrel, lockmode);
	rel->localrel = NULL;
}

/*
 * Partition cache: look up partition LogicalRepRelMapEntry's
 *
 * Unlike relation map cache, this is keyed by partition OID, not remote
 * relation OID, because we only have to use this cache in the case where
 * partitions are not directly mapped to any remote relation, such as when
 * replication is occurring with one of their ancestors as target.
 */

/*
 * Relcache invalidation callback
 */
static void
logicalrep_partmap_invalidate_cb(Datum arg, Oid reloid)
{
	LogicalRepPartMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepPartMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepPartMap);

		/* TODO, use inverse lookup hashtable? */
		while ((entry = (LogicalRepPartMapEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->relmapentry.localreloid == reloid)
			{
				entry->relmapentry.localrelvalid = false;
				hash_seq_term(&status);
				break;
			}
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepPartMap);

		while ((entry = (LogicalRepPartMapEntry *) hash_seq_search(&status)) != NULL)
			entry->relmapentry.localrelvalid = false;
	}
}

/*
 * Reset the entries in the partition map that refer to remoterel.
 *
 * Called when new relation mapping is sent by the publisher to update our
 * expected view of incoming data from said publisher.
 *
 * Note that we don't update the remoterel information in the entry here,
 * we will update the information in logicalrep_partition_open to avoid
 * unnecessary work.
 */
void
logicalrep_partmap_reset_relmap(LogicalRepRelation *remoterel)
{
	HASH_SEQ_STATUS status;
	LogicalRepPartMapEntry *part_entry;
	LogicalRepRelMapEntry *entry;

	if (LogicalRepPartMap == NULL)
		return;

	hash_seq_init(&status, LogicalRepPartMap);
	while ((part_entry = (LogicalRepPartMapEntry *) hash_seq_search(&status)) != NULL)
	{
		entry = &part_entry->relmapentry;

		if (entry->remoterel.remoteid != remoterel->remoteid)
			continue;

		logicalrep_relmap_free_entry(entry);

		memset(entry, 0, sizeof(LogicalRepRelMapEntry));
	}
}

/*
 * Initialize the partition map cache.
 */
static void
logicalrep_partmap_init(void)
{
	HASHCTL		ctl;

	if (!LogicalRepPartMapContext)
		LogicalRepPartMapContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "LogicalRepPartMapContext",
								  ALLOCSET_DEFAULT_SIZES);

	/* Initialize the relation hash table. */
	ctl.keysize = sizeof(Oid);	/* partition OID */
	ctl.entrysize = sizeof(LogicalRepPartMapEntry);
	ctl.hcxt = LogicalRepPartMapContext;

	LogicalRepPartMap = hash_create("logicalrep partition map cache", 64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(logicalrep_partmap_invalidate_cb,
								  (Datum) 0);
}

/*
 * logicalrep_partition_open
 *
 * Returned entry reuses most of the values of the root table's entry, save
 * the attribute map, which can be different for the partition.  However,
 * we must physically copy all the data, in case the root table's entry
 * gets freed/rebuilt.
 *
 * Note there's no logicalrep_partition_close, because the caller closes the
 * component relation.
 */
LogicalRepRelMapEntry *
logicalrep_partition_open(LogicalRepRelMapEntry *root,
						  Relation partrel, AttrMap *map)
{
	LogicalRepRelMapEntry *entry;
	LogicalRepPartMapEntry *part_entry;
	LogicalRepRelation *remoterel = &root->remoterel;
	Oid			partOid = RelationGetRelid(partrel);
	AttrMap    *attrmap = root->attrmap;
	bool		found;
	MemoryContext oldctx;

	if (LogicalRepPartMap == NULL)
		logicalrep_partmap_init();

	/* Search for existing entry. */
	part_entry = (LogicalRepPartMapEntry *) hash_search(LogicalRepPartMap,
														&partOid,
														HASH_ENTER, &found);

	entry = &part_entry->relmapentry;

	/*
	 * We must always overwrite entry->localrel with the latest partition
	 * Relation pointer, because the Relation pointed to by the old value may
	 * have been cleared after the caller would have closed the partition
	 * relation after the last use of this entry.  Note that localrelvalid is
	 * only updated by the relcache invalidation callback, so it may still be
	 * true irrespective of whether the Relation pointed to by localrel has
	 * been cleared or not.
	 */
	if (found && entry->localrelvalid)
	{
		entry->localrel = partrel;
		return entry;
	}

	/* Switch to longer-lived context. */
	oldctx = MemoryContextSwitchTo(LogicalRepPartMapContext);

	if (!found)
	{
		memset(part_entry, 0, sizeof(LogicalRepPartMapEntry));
		part_entry->partoid = partOid;
	}

	/* Release the no-longer-useful attrmap, if any. */
	if (entry->attrmap)
	{
		free_attrmap(entry->attrmap);
		entry->attrmap = NULL;
	}

	if (!entry->remoterel.remoteid)
	{
		int			i;

		/* Remote relation is copied as-is from the root entry. */
		entry->remoterel.remoteid = remoterel->remoteid;
		entry->remoterel.nspname = pstrdup(remoterel->nspname);
		entry->remoterel.relname = pstrdup(remoterel->relname);
		entry->remoterel.natts = remoterel->natts;
		entry->remoterel.attnames = palloc_array(char *, remoterel->natts);
		entry->remoterel.atttyps = palloc_array(Oid, remoterel->natts);
		for (i = 0; i < remoterel->natts; i++)
		{
			entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
			entry->remoterel.atttyps[i] = remoterel->atttyps[i];
		}
		entry->remoterel.replident = remoterel->replident;
		entry->remoterel.attkeys = bms_copy(remoterel->attkeys);
	}

	entry->localrel = partrel;
	entry->localreloid = partOid;

	/*
	 * If the partition's attributes don't match the root relation's, we'll
	 * need to make a new attrmap which maps partition attribute numbers to
	 * remoterel's, instead of the original which maps root relation's
	 * attribute numbers to remoterel's.
	 *
	 * Note that 'map' which comes from the tuple routing data structure
	 * contains 1-based attribute numbers (of the parent relation).  However,
	 * the map in 'entry', a logical replication data structure, contains
	 * 0-based attribute numbers (of the remote relation).
	 */
	if (map)
	{
		AttrNumber	attno;

		entry->attrmap = make_attrmap(map->maplen);
		for (attno = 0; attno < entry->attrmap->maplen; attno++)
		{
			AttrNumber	root_attno = map->attnums[attno];

			/* 0 means it's a dropped attribute.  See comments atop AttrMap. */
			if (root_attno == 0)
				entry->attrmap->attnums[attno] = -1;
			else
				entry->attrmap->attnums[attno] = attrmap->attnums[root_attno - 1];
		}
	}
	else
	{
		/* Lacking copy_attmap, do this the hard way. */
		entry->attrmap = make_attrmap(attrmap->maplen);
		memcpy(entry->attrmap->attnums, attrmap->attnums,
			   attrmap->maplen * sizeof(AttrNumber));
	}

	/* Set if the table's replica identity is enough to apply update/delete. */
	logicalrep_rel_mark_updatable(entry);

	/* state and statelsn are left set to 0. */
	MemoryContextSwitchTo(oldctx);

	/*
	 * Finding a usable index is an infrequent task. It occurs when an
	 * operation is first performed on the relation, or after invalidation of
	 * the relation cache entry (such as ANALYZE or CREATE/DROP index on the
	 * relation).
	 *
	 * We also prefer to run this code on the oldctx so that we do not leak
	 * anything in the LogicalRepPartMapContext (hence CacheMemoryContext).
	 */
	entry->localindexoid = FindLogicalRepLocalIndex(partrel, remoterel,
													entry->attrmap);

	/*
	 * TODO: Parallel apply does not support the parallel apply for now.
	 * Just mark local indexes are collected.
	 */
	entry->local_unique_indexes_collected = true;

	entry->localrelvalid = true;

	return entry;
}

/*
 * Returns the oid of an index that can be used by the apply worker to scan
 * the relation.
 *
 * We expect to call this function when REPLICA IDENTITY FULL is defined for
 * the remote relation.
 *
 * If no suitable index is found, returns InvalidOid.
 */
static Oid
FindUsableIndexForReplicaIdentityFull(Relation localrel, AttrMap *attrmap)
{
	List	   *idxlist = RelationGetIndexList(localrel);

	foreach_oid(idxoid, idxlist)
	{
		bool		isUsableIdx;
		Relation	idxRel;

		idxRel = index_open(idxoid, AccessShareLock);
		isUsableIdx = IsIndexUsableForReplicaIdentityFull(idxRel, attrmap);
		index_close(idxRel, AccessShareLock);

		/* Return the first eligible index found */
		if (isUsableIdx)
			return idxoid;
	}

	return InvalidOid;
}

/*
 * Returns true if the index is usable for replica identity full.
 *
 * The index must have an equal strategy for each key column, be non-partial,
 * and the leftmost field must be a column (not an expression) that references
 * the remote relation column. These limitations help to keep the index scan
 * similar to PK/RI index scans.
 *
 * attrmap is a map of local attributes to remote ones. We can consult this
 * map to check whether the local index attribute has a corresponding remote
 * attribute.
 *
 * Note that the limitations of index scans for replica identity full only
 * adheres to a subset of the limitations of PK/RI. For example, we support
 * columns that are marked as [NULL] or we are not interested in the [NOT
 * DEFERRABLE] aspect of constraints here. It works for us because we always
 * compare the tuples for non-PK/RI index scans. See
 * RelationFindReplTupleByIndex().
 *
 * XXX: To support partial indexes, the required changes are likely to be larger.
 * If none of the tuples satisfy the expression for the index scan, we fall-back
 * to sequential execution, which might not be a good idea in some cases.
 */
bool
IsIndexUsableForReplicaIdentityFull(Relation idxrel, AttrMap *attrmap)
{
	AttrNumber	keycol;
	oidvector  *indclass;

	/* The index must not be a partial index */
	if (!heap_attisnull(idxrel->rd_indextuple, Anum_pg_index_indpred, NULL))
		return false;

	Assert(idxrel->rd_index->indnatts >= 1);

	indclass = (oidvector *) DatumGetPointer(SysCacheGetAttrNotNull(INDEXRELID,
																	idxrel->rd_indextuple,
																	Anum_pg_index_indclass));

	/* Ensure that the index has a valid equal strategy for each key column */
	for (int i = 0; i < idxrel->rd_index->indnkeyatts; i++)
	{
		Oid			opfamily;

		opfamily = get_opclass_family(indclass->values[i]);
		if (IndexAmTranslateCompareType(COMPARE_EQ, idxrel->rd_rel->relam, opfamily, true) == InvalidStrategy)
			return false;
	}

	/*
	 * For indexes other than PK and REPLICA IDENTITY, we need to match the
	 * local and remote tuples.  The equality routine tuples_equal() cannot
	 * accept a data type where the type cache cannot provide an equality
	 * operator.
	 */
	for (int i = 0; i < idxrel->rd_att->natts; i++)
	{
		TypeCacheEntry *typentry;

		typentry = lookup_type_cache(TupleDescAttr(idxrel->rd_att, i)->atttypid, TYPECACHE_EQ_OPR_FINFO);
		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			return false;
	}

	/* The leftmost index field must not be an expression */
	keycol = idxrel->rd_index->indkey.values[0];
	if (!AttributeNumberIsValid(keycol))
		return false;

	/*
	 * And the leftmost index field must reference the remote relation column.
	 * This is because if it doesn't, the sequential scan is favorable over
	 * index scan in most cases.
	 */
	if (attrmap->maplen <= AttrNumberGetAttrOffset(keycol) ||
		attrmap->attnums[AttrNumberGetAttrOffset(keycol)] < 0)
		return false;

	/*
	 * The given index access method must implement "amgettuple", which will
	 * be used later to fetch the tuples.  See RelationFindReplTupleByIndex().
	 */
	if (GetIndexAmRoutineByAmId(idxrel->rd_rel->relam, false)->amgettuple == NULL)
		return false;

	return true;
}

/*
 * Return the OID of the replica identity index if one is defined;
 * the OID of the PK if one exists and is not deferrable;
 * otherwise, InvalidOid.
 */
Oid
GetRelationIdentityOrPK(Relation rel)
{
	Oid			idxoid;

	idxoid = RelationGetReplicaIndex(rel);

	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(rel, false);

	return idxoid;
}

/*
 * Returns the index oid if we can use an index for subscriber. Otherwise,
 * returns InvalidOid.
 */
static Oid
FindLogicalRepLocalIndex(Relation localrel, LogicalRepRelation *remoterel,
						 AttrMap *attrMap)
{
	Oid			idxoid;

	/*
	 * We never need index oid for partitioned tables, always rely on leaf
	 * partition's index.
	 */
	if (localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return InvalidOid;

	/*
	 * Simple case, we already have a primary key or a replica identity index.
	 */
	idxoid = GetRelationIdentityOrPK(localrel);
	if (OidIsValid(idxoid))
		return idxoid;

	if (remoterel->replident == REPLICA_IDENTITY_FULL)
	{
		/*
		 * We are looking for one more opportunity for using an index. If
		 * there are any indexes defined on the local relation, try to pick a
		 * suitable index.
		 *
		 * The index selection safely assumes that all the columns are going
		 * to be available for the index scan given that remote relation has
		 * replica identity full.
		 *
		 * Note that we are not using the planner to find the cheapest method
		 * to scan the relation as that would require us to either use lower
		 * level planner functions which would be a maintenance burden in the
		 * long run or use the full-fledged planner which could cause
		 * overhead.
		 */
		return FindUsableIndexForReplicaIdentityFull(localrel, attrMap);
	}

	return InvalidOid;
}

/*
 * Get the number of entries in the LogicalRepRelMap.
 */
int
logicalrep_get_num_rels(void)
{
	if (LogicalRepRelMap == NULL)
		return 0;

	return hash_get_num_entries(LogicalRepRelMap);
}

/*
 * Write all the remote relation information from the LogicalRepRelMapEntry to
 * the output stream.
 */
void
logicalrep_write_all_rels(StringInfo out)
{
	LogicalRepRelMapEntry *entry;
	HASH_SEQ_STATUS status;

	if (LogicalRepRelMap == NULL)
		return;

	hash_seq_init(&status, LogicalRepRelMap);

	while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		logicalrep_write_internal_rel(out, &entry->remoterel);
}

/*
 * Get the LogicalRepRelMapEntry corresponding to the given relid without
 * opening the local relation.
 */
LogicalRepRelMapEntry *
logicalrep_get_relentry(LogicalRepRelId remoteid)
{
	LogicalRepRelMapEntry *entry;
	bool		found;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, (void *) &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(DEBUG1, "no relation map entry for remote relation ID %u",
			 remoteid);

	return entry;
}
