/*--------------------------------------------------------------------
 * bgworker_internals.h
 *		POSTGRES pluggable background workers internals
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/postmaster/bgworker_internals.h
 *--------------------------------------------------------------------
 */
#ifndef BGWORKER_INTERNALS_H
#define BGWORKER_INTERNALS_H

#include "datatype/timestamp.h"
#include "lib/ilist.h"
#include "postmaster/bgworker.h"

/* GUC options */

/*
 * Maximum possible value of parallel workers.
 */
#define MAX_PARALLEL_WORKER_LIMIT 1024

/*
 * List of background workers, private to postmaster.
 *
 * All workers that are currently running will have rw_backend set, and will
 * be present in BackendList.
 */
typedef struct RegisteredBgWorker
{
	BackgroundWorker rw_worker; /* its registry entry */
	struct bkend *rw_backend;	/* its BackendList entry, or NULL if not
								 * running */
	pid_t		rw_pid;			/* 0 if not running */
	int			rw_child_slot;
	TimestampTz rw_crashed_at;	/* if not 0, time it last crashed */
	int			rw_shmem_slot;
	bool		rw_terminate;
	slist_node	rw_lnode;		/* list link */
} RegisteredBgWorker;

extern PGDLLIMPORT slist_head BackgroundWorkerList;

extern Size BackgroundWorkerShmemSize(void);
extern void BackgroundWorkerShmemInit(void);
extern void BackgroundWorkerStateChange(bool allow_new_workers);
extern void ForgetBackgroundWorker(slist_mutable_iter *cur);
extern void ReportBackgroundWorkerPID(RegisteredBgWorker *);
extern void ReportBackgroundWorkerExit(slist_mutable_iter *cur);
extern void BackgroundWorkerStopNotifications(pid_t pid);
extern void ForgetUnstartedBackgroundWorkers(void);
extern void ResetBackgroundWorkerCrashTimes(void);

/* Entry point for background worker processes */
extern void BackgroundWorkerMain(char *startup_data, size_t startup_data_len) pg_attribute_noreturn();

#endif							/* BGWORKER_INTERNALS_H */
