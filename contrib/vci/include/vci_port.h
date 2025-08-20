/*-------------------------------------------------------------------------
 *
 * vci_port.h
 *	  Header for OS-dependent functions
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/include/vci_port.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VCI_PORT_H
#define VCI_PORT_H

#include <limits.h>
#include <float.h>

#include "lib/ilist.h"

/*
 * key for vci_devload_t
 */
#define VCI_PSEUDO_UNMONITORED_DEVICE ""

#define VCI_DISKSTAT_RECORD_NUM (30)	/* number of records in
										 * /proc/diskstats */

#if defined(__linux__)
#include "vci_port_linux.h"

#elif defined(__sparc__)
#include "vci_port_solaris.h"

#elif defined(WIN32)
#include "vci_port_windows.h"

#else
#error "This system should not be supported."

#endif

#define VCI_FREE_DEVLOAD_INFO_NAME "~"
#define VCI_FREE_DEVLOAD_AWAIT (-1.0)
#define VCI_UNMONITORED_DEV_AWAIT DBL_MAX
#define VCI_INITIAL_AWAIT DBL_MAX

#ifndef WIN32
#define VCI_PATH_MAX	PATH_MAX
#else
#define VCI_PATH_MAX	MAX_PATH
#endif

/*
 * Memory entry on the each device
 *
 * head is the actual list, link is used to track unused entries
 */
typedef struct
{
	dlist_head	head;
	dlist_node	link;
} vci_memory_entry_list_t;

/*
 * IO statistics, mount information, etc for each devices
 */
typedef struct
{
	/**
	 * device name for example, sdb, sdb. Not the form like /dev/sda.
	 * devname[] = "" means that this vci_devload_t type value is used for
	 * non-monitored devices. devname "~" means this is free. Because
	 * any device name starts with an alphabet, "~" is greater than any
	 * device name. Then when we sort vci_devload_t list, free space
	 * is accumulated.
	 */
	char		devname[VCI_PATH_MAX];

	bool		alive;

	/**
	 * The average time(in milliseconds) for I/O requests issued to
	 * this device in the last interval.
	 * DBL_MAX for non monitored device(this is allocated last).
	 * for free object, place on free space
	 */
	double		await;

	/**
	 * await of the interval before last.
	 */
	double		prev_await;

	/**
	 * average of the await of the last VCI_DISKSTAT_RECORD_NUM intervals.
	 * A negative number means that the number of monitoring has not reached
	 * the minimum to calculate IO loads, VCI_DISKSTAT_RECORD_NUM.
	 */
	double		await_avg;

	/**
	 * coefficient of variation of await.
	 */
	double		await_cv;

	/*
	 * How many entries in devstat?
	 *
	 * - If this reaches VCI_DISKSTAT_RECORD_NUM, won't be changed.
	 */
	unsigned int devstat_num;

	bool		contain_vci;

	vci_memory_entry_list_t *memory_entry_queue;

	/*
	 * Next position when memory entry would be traced. NULL means there are
	 * no entries to be seen.
	 */
	dlist_node *memory_entry_pos;
} vci_devload_t;

extern void vci_init_devloadinfo(vci_devload_t *devinfo, const char *devname);
extern void vci_init_tablespace2dev(void);
extern void vci_fini_tablespace2dev(void);
extern const char *vci_convert_tablespace_to_devname(const char *path);
extern vci_devload_t *vci_find_devloadinfo(vci_devload_t *devload_array, int num_devload_info, const char *name);
extern bool vci_IsLowIOLoad(vci_devload_t *dl);

#endif							/* VCI_PORT_H */
