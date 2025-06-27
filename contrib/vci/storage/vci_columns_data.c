/*-------------------------------------------------------------------------
 *
 * vci_columns_data.c
 *	  Definitions of functions to check which columns are indexed.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/storage/vci_columns_data.c
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "vci.h"
#include "vci_columns.h"
#include "vci_columns_data.h"
#include "vci_ros.h"

static Bitmapset *parseVciColumnsIds(const char *vci_column_ids);
static void updateRelopt(Relation indexRel, char *optName, char *newData);

/* Convert comma-separated column ids to Bitmapset */
static Bitmapset *
parseVciColumnsIds(const char *vci_column_ids)
{
	List	   *columnlist;
	ListCell   *l;

	/* SplitIdentifierString can destroy the first argument. */
	char	   *copied_ids = pstrdup(vci_column_ids);
	Bitmapset  *indexedAttids = NULL;
	int			attid = 0;

	if (!SplitIdentifierString(copied_ids, ',', &columnlist))
		ereport(ERROR, (errmsg("internal error. failed to split")));

	foreach(l, columnlist)
	{
		char	   *number_str = (char *) lfirst(l);

		/* The max id is '1600' -> 4 digits. */
		int			attid_diff = pg_strtoint32(number_str);

		attid += attid_diff;

		if (attid >= MaxHeapAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("invalid attribute number %d", attid + 1)));

		if (bms_is_member(attid, indexedAttids))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 (errmsg("duplicated columns in vci index creation")),
					 errhint("duplicated columns are specified")));

		indexedAttids = bms_add_member(indexedAttids, attid);
	}

	pfree(copied_ids);

	return indexedAttids;
}

/*
 * vci_ConvertAttidBitmap2String -- Convert a Bitmapset that represents which
									attids are target to comma separated string
 */
char *
vci_ConvertAttidBitmap2String(Bitmapset *attid_bitmap)
{
	int			attid;
	int			preAttid = 0;
	StringInfo	buf = makeStringInfo();

	attid = -1;
	while ((attid = bms_next_member(attid_bitmap, attid)) >= 0)
	{
		if (attid >= MaxHeapAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("invalid attribute number %d", attid + 1)));

		if (buf->len == 0)
			appendStringInfo(buf, "%d", attid - preAttid);
		else
			appendStringInfo(buf, ",%d", attid - preAttid);

		preAttid = attid;
	}
	return buf->data;
}

/*
 * Note that this is written by refference to ATExecSetRelOptions in
 * src/backend/commands/tablecmds.c
 */
static void
updateRelopt(Relation indexRel, char *optName, char *newData)
{
	Oid			relid;
	Relation	pgclass;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	bool		isnull;
	List	   *defList;
	Datum		newOptions;
	Datum		repl_val[Natts_pg_class];
	bool		repl_null[Natts_pg_class];
	bool		repl_repl[Natts_pg_class];
	const char *const validnsps[] = HEAP_RELOPT_NAMESPACES;

	pgclass = table_open(RelationRelationId, RowExclusiveLock);

	/* Fetch heap tuple */
	relid = RelationGetRelid(indexRel);
	tuple = SearchSysCacheLocked1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	/* Make new reloptions  */
	newOptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								 &isnull);
	Assert(!isnull);

	defList = list_make1(makeDefElem(optName,
									 (Node *) makeString(newData), -1));
	newOptions = transformRelOptions(newOptions, defList, NULL,
									 validnsps, true, false);

	/* Validate */
	index_reloptions(indexRel->rd_indam->amoptions, newOptions, true);

	/*
	 * All we need do here is update the pg_class row; the new options will be
	 * propagated into relcaches during post-commit cache inval.
	 */
	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));

	Assert(newOptions != (Datum) 0);
	repl_val[Anum_pg_class_reloptions - 1] = newOptions;
	repl_repl[Anum_pg_class_reloptions - 1] = true;

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(pgclass),
								 repl_val, repl_null, repl_repl);

	CatalogTupleUpdate(pgclass, &newtuple->t_self, newtuple);
	UnlockTuple(pgclass, &tuple->t_self, InplaceUpdateTupleLock);

	InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(indexRel), 0);

	ReleaseSysCache(tuple);
	heap_freetuple(newtuple);
	table_close(pgclass, RowExclusiveLock);

	/* Make the updates visible */
	CommandCounterIncrement();
}

/*
 * vci_RegisterDroppedAttnum -- Register attnum as dropped column
 */
void
vci_RegisterDroppedAttnum(Relation indexRel, int attnum)
{
	Bitmapset  *indexRelids = NULL;

	const StdRdOptions *opts;
	const char *ids;
	char	   *addedIds;

	Assert(vci_IsExtendedToMoreThan32Columns(indexRel));
	opts = (StdRdOptions *) indexRel->rd_options;
	ids = opts ? (((char *) opts) + opts->vci_dropped_column_ids_offset) : NULL;
	Assert(ids);

	ereport(DEBUG2,
			(errmsg_internal("Before rewrite vci_dropped_column_ids: %s", ids)));

	indexRelids = parseVciColumnsIds(ids);
	Assert(!bms_is_member(attnum - 1, indexRelids));
	indexRelids = bms_add_member(indexRelids, attnum - 1);

	addedIds = vci_ConvertAttidBitmap2String(indexRelids);
	updateRelopt(indexRel, "vci_dropped_column_ids", addedIds);

	ereport(DEBUG2,
			(errmsg_internal("After rewrite vci_dropped_column_ids: %s", addedIds)));

	bms_free(indexRelids);
}

/*
 * vci_FillDroppedColumnIds -- Fill dropped column ids in indexed column ids
 *
 * This updates vci_column_ids and clears vci_dropped_column_ids options.
 * Ex. When vci_column_ids indecates that the column ids 1,3,5 are indexed and
 * vci_dropped_column_ids indecates that the column ids 2 is dropped, the new ids
 * are caluculated as 1, 2, 4.
 */
void
vci_FillDroppedColumnIds(Relation indexRel)
{
	Bitmapset  *indexedIds = NULL;
	Bitmapset  *droppedIds = NULL;
	Bitmapset  *resultIds = NULL;
	const StdRdOptions *opts;
	int			fillSpace = 0;
	int			indexedAttid;
	int			droppedAttid;

	/*
	 * This case should not occur as far as a user doesn't use ctid directly.
	 * But because such usage is not mentioned in the manual, ereport as
	 * internal.
	 */
	if (!vci_IsExtendedToMoreThan32Columns(indexRel))
		ereport(ERROR,
				(errmsg_internal("\"%s\" should be called using VCI created by 'vci_create()'", __FUNCTION__)));

	opts = (StdRdOptions *) indexRel->rd_options;
	Assert(opts != NULL);

	indexedIds = parseVciColumnsIds(((char *) opts) + opts->vci_column_ids_offset);
	droppedIds = parseVciColumnsIds(((char *) opts) + opts->vci_dropped_column_ids_offset);

	droppedAttid = -1;
	droppedAttid = bms_next_member(droppedIds, droppedAttid);

	indexedAttid = -1;
	while ((indexedAttid = bms_next_member(indexedIds, indexedAttid)) >= 0)
	{
		Assert(indexedAttid != droppedAttid);

		while (droppedAttid >= 0 && indexedAttid > droppedAttid)
		{
			droppedAttid = bms_next_member(droppedIds, droppedAttid);
			fillSpace++;
		}

		Assert(indexedAttid - fillSpace >= 0);

		resultIds = bms_add_member(resultIds, indexedAttid - fillSpace);
	}

	/*
	 * Because updateRelopt opens and closes the index relation, calling it
	 * twice takes some overhead. But it should have small impact in the
	 * process of creating index.
	 */
	updateRelopt(indexRel, "vci_column_ids", vci_ConvertAttidBitmap2String(resultIds));
	updateRelopt(indexRel, "vci_dropped_column_ids", "");

	bms_free(indexedIds);
	bms_free(droppedIds);
	bms_free(resultIds);
}

/*
 * vci_ExtractColumnDataUsingIds -- returns TupleDesc that contains indexed columns
 *							        information.
 *
 * The vci_GetTupleDescr() requires a prebuild vci_MainRelHeaderInfo. So please use
 * this when building a VCI because the structure is in the process of building.
 */
TupleDesc
vci_ExtractColumnDataUsingIds(const char *vci_column_ids, Relation heapRel)
{
	int			i;
	int			attid;
	TupleDesc	heapTupDesc;
	TupleDesc	result;
	Bitmapset  *indexedAttids = NULL;	/* for duplication check */

	heapTupDesc = RelationGetDescr(heapRel);
	indexedAttids = parseVciColumnsIds(vci_column_ids);
	result = CreateTemplateTupleDesc(bms_num_members(indexedAttids));

	attid = -1;
	i = 0;
	while ((attid = bms_next_member(indexedAttids, attid)) >= 0)
	{
		if (attid >= heapTupDesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid attribute number %d", attid + 1)));

		TupleDescCopyEntry(result, i + 1, heapTupDesc, attid + 1);
		i++;
	}

	bms_free(indexedAttids);

	return result;
}

/*
 * vci_IsExtendedToMoreThan32Columns -- Check the VCI relation is the version
 * where we can create VCI on more than 32 columns.
 *
 * 'true': it should be created by 'vci_create()' function.
 * 'false': it should be created by 'CREATE INDEX' clause.
 */
bool
vci_IsExtendedToMoreThan32Columns(Relation indexRel)
{
	const StdRdOptions *opts;
	const char *ids;

	Assert(isVciIndexRelation(indexRel));

	opts = (StdRdOptions *) indexRel->rd_options;
	ids = opts ? (((char *) opts) + opts->vci_column_ids_offset) : NULL;

	return ids != NULL && strlen(ids) > 0;
}

/*
 * vci_GetTupleDescr -- returns TupleDesc that contains indexed columns
 *                      information from vci_MainRelHeaderInfo.
 */
TupleDesc
vci_GetTupleDescr(vci_MainRelHeaderInfo *info)
{
	LOCKMODE	lockmode = AccessShareLock;
	Oid			tableOid = info->rel->rd_index->indrelid;
	Relation	tableRel;
	MemoryContext oldcontext;

	if (info->cached_tupledesc)
		return info->cached_tupledesc;

	oldcontext = MemoryContextSwitchTo(info->initctx);

	if (vci_IsExtendedToMoreThan32Columns(info->rel))
	{
		const char *ids;
		const StdRdOptions *opts;

		opts = (StdRdOptions *) (info->rel)->rd_options;
		Assert(opts != NULL);
		ids = ((char *) opts) + opts->vci_column_ids_offset;

		tableRel = table_open(tableOid, lockmode);
		info->cached_tupledesc = vci_ExtractColumnDataUsingIds(ids, tableRel);
		table_close(tableRel, lockmode);
	}
	else
		info->cached_tupledesc = RelationGetDescr(info->rel);

	MemoryContextSwitchTo(oldcontext);

	return info->cached_tupledesc;
}

Bitmapset *
vci_MakeIndexedColumnBitmap(Oid mainRelationOid,
							MemoryContext sharedMemCtx,
							LOCKMODE lockmode)
{
	Relation	main_rel;
	Bitmapset  *result = NULL;
	vci_MainRelHeaderInfo *info;

	info = MemoryContextAllocZero(sharedMemCtx,
								  sizeof(vci_MainRelHeaderInfo));
	main_rel = relation_open(mainRelationOid, lockmode);
	vci_InitMainRelHeaderInfo(info, main_rel, vci_rc_query);
	vci_KeepMainRelHeader(info);

	{
		int32		indexNumColumns = vci_GetMainRelVar(info,
														vcimrv_num_columns, 0);
		int			aId;

		for (aId = 0; aId < indexNumColumns; ++aId)
		{
			vcis_m_column_t *mColumn = vci_GetMColumn(info, aId);
			LOCKMODE	lockmode_for_meta = AccessShareLock;
			Relation	column_meta_rel = table_open(mColumn->meta_oid, lockmode_for_meta);
			Buffer		buffer;
			vcis_column_meta_t *metaHeader = vci_GetColumnMeta(&buffer, column_meta_rel);

			Assert(metaHeader->pgsql_attnum > InvalidAttrNumber);
			result = bms_add_member(result, metaHeader->pgsql_attnum);
			ReleaseBuffer(buffer);
			table_close(column_meta_rel, lockmode_for_meta);
		}
	}

	vci_ReleaseMainRelHeader(info);
	relation_close(main_rel, lockmode);

	return result;
}

extern Bitmapset *
vci_MakeDroppedColumnBitmap(Relation indexRel)
{
	const StdRdOptions *opts;
	const char *ids;

	Assert(vci_IsExtendedToMoreThan32Columns(indexRel));

	opts = (StdRdOptions *) indexRel->rd_options;
	ids = opts ? (((char *) opts) + opts->vci_dropped_column_ids_offset) : NULL;
	Assert(ids);

	return parseVciColumnsIds(ids);
}

/**
 * @brief Get attribute number from the name.
 * @param[in] desc The tuple descriptor of the relation.
 * @param[in] name The name of attribute.
 * @return The attribute number.
 * If the name is not found in the descriptor, InvalidAttrNumber is returned.
 */
AttrNumber
vci_GetAttNum(TupleDesc desc, const char *name)
{
	int			aId;

	for (aId = 0; aId < desc->natts; ++aId)
	{
		if (strcmp(name, NameStr(TupleDescAttr(desc, aId)->attname)) == 0)
			return aId + 1;
	}

	return InvalidAttrNumber;
}
