/*--------------------------------------------------------------------------
 *
 * test_radixtree_parallel.c
 *		Test module for parallel operations of adaptive radix tree.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_radixtree/test_radixtree_parallel.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/shm_toc.h"
#include "utils/memutils.h"

typedef uint64 TestValueType;

/* define the radix tree implementation to test */
#define RT_PREFIX rt_parallel
#define RT_SCOPE
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE TestValueType
#define RT_SHMEM
#define RT_DEBUG
#include "lib/radixtree.h"

#define RADIXTREE_MAGIC 0x134DB48

typedef struct ParallelRadixTreeWorkerShared
{
	int i;
} ParallelRadixTreeWorkerShared;

PG_FUNCTION_INFO_V1(test_radixtree_parallel);

PGDLLEXPORT void radixtree_worker_main(Datum main_arg);

static bool setup_dsm(void);
static bool launch_worker(BackgroundWorkerHandle *handle, pid_t *pid);

void
radixtree_worker_main(Datum main_arg)
{
	Oid oid = DatumGetInt32(main_arg);

	BackgroundWorkerInitializeConnectionByOid(oid, InvalidOid,
											  BGWORKER_BYPASS_ALLOWCONN);

	elog(LOG, "XXX suceed to connect: bye");
}

static bool
launch_worker(BackgroundWorkerHandle *handle, pid_t *pid)
{
	BackgroundWorker worker;
	BgwHandleStatus status;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "test_radixtree");
	sprintf(worker.bgw_function_name, "radixtree_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "test_radixtree worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "test_radixtree worker");
	worker.bgw_main_arg = Int32GetDatum(MyDatabaseId);

	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return false;

	status = WaitForBackgroundWorkerStartup(handle, pid);

	Assert(status == BGWH_STARTED);

	return true;
}

static bool
setup_dsm(void)
{
	MemoryContext oldcontext;
	shm_toc_estimator e;
	Size		segsize;
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelRadixTreeWorkerShared *shared;

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ParallelRadixTreeWorkerShared));

	shm_toc_estimate_keys(&e, 1);
	segsize = shm_toc_estimate(&e);

	seg = dsm_create(shm_toc_estimate(&e), 0);
	if (!seg)
		return false;

	toc = shm_toc_create(RADIXTREE_MAGIC, dsm_segment_address(seg),
						 segsize);

	shared = shm_toc_allocate(toc, sizeof(ParallelRadixTreeWorkerShared));
	shm_toc_insert(toc, 1, shared);

	MemoryContextSwitchTo(oldcontext);
}


Datum
test_radixtree_parallel(PG_FUNCTION_ARGS)
{
	BackgroundWorkerHandle *handle = NULL;
	pid_t pid;

	setup_dsm();


	launch_worker(handle, &pid);

	elog(LOG, "XXX laucnhed. pid is %u", pid);

	PG_RETURN_VOID();
}
