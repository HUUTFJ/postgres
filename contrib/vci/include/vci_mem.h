/*-------------------------------------------------------------------------
 *
 * vci_mem.h
 *	  Definitions of on-memmory structures
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/include/vci_mem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VCI_MEM_H
#define VCI_MEM_H

#include "postgres.h"

#include "lib/ilist.h"
#include "portability/instr_time.h"
#include "storage/lwlock.h"
#include "utils/palloc.h"

#include "vci.h"
#include "vci_ros.h"
#include "vci_memory_entry.h"

typedef struct VciGucStruct
{
	bool		have_loaded_postgresql_conf;

	bool		enable;

	bool		log_query;

	int			cost_threshold;

	int			table_scan_policy;

	/* GUC parameters read from postgresq.conf */
	int			maintenance_work_mem;
	int			max_devices;	/* max device num for storage */

	/* ROS control worker/daemon */
	int			control_max_workers;
	int			control_naptime;

	/* command thresholds */
	int			wosros_conv_threshold;
	int			cdr_threshold;

	/* for custom plan execution */
	int			max_local_ros_size;

	/* for parallel processing */
	int			table_rows_threshold;

	bool		enable_seqscan;
	bool		enable_indexscan;
	bool		enable_bitmapheapscan;
	bool		enable_sort;
	bool		enable_hashagg;
	bool		enable_sortagg;
	bool		enable_plainagg;
	bool		enable_hashjoin;
	bool		enable_nestloop;

	/* GUC parameters for internal use */
	bool		enable_ros_control_daemon;

} VciGucStruct;

extern PGDLLEXPORT VciGucStruct VciGuc;

/*
 * Data structure on shared memory
 *
 * The instance would be allocated on the shared memory and can be accessed via
 * VciShmemAddr.
 */
typedef struct VciShmemStruct
{
	/* --- ROS Control Daemon --- */

	/* Attributes for passing attributes to a worker */

	vci_wosros_conv_worker_arg_t *worker_args_array;

	/** vci_memory_entries_t is defined in vci_ros.h
	 * That keeps information of VCI indices kept in memory.
	 * The life is the same with PostgreSQL instance.
	 */
	vci_memory_entries_t *memory_entries;

	dlist_head	memory_entry_device_unknown_list;

	/* Standby server controller */
	LWLock	   *standby_exec_loc;
	int			num_standby_exec_queries;

	/* IO statistics */

	vci_devload_t *devload_array;

	vci_memory_entry_list_t *memory_entry_queue_array;

	dlist_head	free_memory_entry_queue_list;	/**list of memory_entry_queue_array */
	int			num_devload_info;	/* monitored device numbers + 1(for
									 * unmonitored devices) */
	int			max_devices;	/* max device num for storage */
	int			translated_dev_pos; /* index of a device VCIs on which is to
									 * be translated */
	LWLock	   *io_load_lock;

	/* Additional Lwlocks used by various modules */
	LWLock	   *vci_memory_entries_lock;
	LWLock	   *vci_query_context_lock;
	LWLock	   *vci_mnt_point2dev_lock;
} VciShmemStruct;

extern PGDLLEXPORT VciShmemStruct *VciShmemAddr;

#endif							/* VCI_MEM_H */
