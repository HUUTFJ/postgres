/*-------------------------------------------------------------------------
 *
 * vci_linux.c
 *	  Linux support functions
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/port/vci_linux.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sched.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <sys/vfs.h>
#include <sys/param.h>
#include <string.h>
#include <dirent.h>
#include <ctype.h>
#include <math.h>

#include "utils/elog.h"

#include "vci.h"
#include "vci_mem.h"
#include "vci_port.h"

#define STABLE_CV_THRESHOLD (0.5)	/* a threshold of a coefficient of
									 * variation to judge the stability */

#define RAMFS "ram"

#define LINESIZE		(256)
#define THRESHOLD		(3)

/**
 * the correspondence between a mountpoint and a device
 */
typedef struct
{
	char		mountpoint[PATH_MAX];	/* a path of mountpoint */
	int			len;			/* the length of mountpoint. This value is
								 * stored avoiding multiple computations */
	char		devname[PATH_MAX];	/* device name */
	bool		mounted;		/* used for updating a mountpoint_dev_pair_t
								 * set. true means that this value remains */
	dlist_node	link;
} mountpoint_dev_pair_t;

static LWLock *mntpoint2dev_lock;

/*
 * Descending list of mountpoint_dev_pair_t
 */
static dlist_head mntpoint2dev;

/**
 * @brief initialize vci_devload_t
 *
 * @param[in, out] devinfo to be initialized
 * @param[in] devname the name of device to be stored in devinfo
 */
void
vci_init_devloadinfo(vci_devload_t *devinfo, const char *devname)
{
	strcpy(devinfo->devname, devname);
	devinfo->devstat_num = 0;
	devinfo->await_avg = VCI_FREE_DEVLOAD_AWAIT;
	devinfo->contain_vci = false;
}

/**
 * initialize mapping from a tablespace to a device.
 */
void
vci_init_tablespace2dev(void)
{
	dlist_init(&mntpoint2dev);
	mntpoint2dev_lock = VciShmemAddr->vci_mnt_point2dev_lock;
}

/**
 * finalize mapping from a tablespace to a device
 */
void
vci_fini_tablespace2dev(void)
{
	dlist_mutable_iter miter;

	LWLockAcquire(mntpoint2dev_lock, LW_EXCLUSIVE);
	dlist_foreach_modify(miter, &mntpoint2dev)
	{
		mountpoint_dev_pair_t *pair;

		pair = dlist_container(mountpoint_dev_pair_t, link, miter.cur);
		dlist_delete(miter.cur);
		pfree(pair);
	}
	LWLockRelease(mntpoint2dev_lock);
}

/**
 * convert given tablespace's path name into device name
 *
 * @param[in] path  path name of tablespace
 *
 * @return device name. should not be freed.
 */
const char *
vci_convert_tablespace_to_devname(const char *path)
{
	const char *ret = NULL;

#if 1
	LWLockAcquire(mntpoint2dev_lock, LW_SHARED);
	{
		dlist_iter	iter;

		dlist_foreach(iter, &mntpoint2dev)
		{
			mountpoint_dev_pair_t *pair;

			pair = dlist_container(mountpoint_dev_pair_t, link, iter.cur);
			elog(DEBUG1, "mapping in path2dev: mountpoint %s -> device %s",
				 pair->mountpoint, pair->devname);
		}
	}
	LWLockRelease(mntpoint2dev_lock);
#endif

	LWLockAcquire(mntpoint2dev_lock, LW_EXCLUSIVE);

	if (dlist_is_empty(&mntpoint2dev))
		ret = VCI_PSEUDO_UNMONITORED_DEVICE;
	else
	{
		dlist_node *n;
		mountpoint_dev_pair_t *pair;
		bool		reach_last = false;
		int			comparison_result;

		n = dlist_head_node(&mntpoint2dev);
		pair = dlist_container(mountpoint_dev_pair_t, link, n);

		/*
		 * Not to use dlist_foreach to ensure we compares from the begining.
		 */
		while (!reach_last && (comparison_result = strncmp(pair->mountpoint, path, pair->len)) != 0)
		{
			if (dlist_has_next(&mntpoint2dev, n))
			{
				n = dlist_next_node(&mntpoint2dev, n);
				pair = dlist_container(mountpoint_dev_pair_t, link, n);
			}
			else
				reach_last = true;

		}

		if (!reach_last && comparison_result == 0)
			ret = pair->devname;
		else
			ret = VCI_PSEUDO_UNMONITORED_DEVICE;
	}

	LWLockRelease(mntpoint2dev_lock);

	elog(DEBUG2, ">>> vci_convert_tablespace_to_devname: returning device name (%s)", ret);
	return ret;
}

/*
 * Extract device info from the vci_devload_t array
 *
 * @param[in] devload_array     an array of vci_devload_info_t, which represents a set of devices.
 * @param[in] num_devload_info  the number of elements of devload_array
 * @param[in] devname              a device name
 *
 * @return index of device info (-1 means 'not found')
 */
vci_devload_t *
vci_find_devloadinfo(vci_devload_t *devload_array, int num_devload_info, const char *devname)
{
	vci_devload_t *ret = NULL;
	int			i;

	elog(DEBUG2, "vci_find_devloadinfo: num_devload_info=%d, looking for devname (%s)", num_devload_info, devname);

	for (i = 0; i < num_devload_info; i++)
	{
		vci_devload_t *item;

		item = &(devload_array[i]);
		if (strcmp(item->devname, devname) == 0)
		{
			ret = item;
			break;
		}
	}

	return ret;
}

/**
 * judges WOS->ROS transformation of VCI indexes on the given device can be done or not.
 *
 * @param[in] devinfo device information
 *
 * @return true IO load is low s.t WOS->ROS transformation can be done, false otherwise
 */
bool
vci_IsLowIOLoad(vci_devload_t *devinfo)
{
	bool		ret = devinfo->await_avg < 0.0	/* We do not have enough
												 * statistics */
		|| devinfo->await_cv < STABLE_CV_THRESHOLD	/* await is not so changed */
		|| (devinfo->await < devinfo->await_avg /* await is less than average */
			|| devinfo->prev_await > devinfo->await);	/* await is less than
														 * previous one */

	if (!ret)
		elog(DEBUG1, "high IO load on device %s: await_avg %f await_cv %f await %f prev_await %f",
			 devinfo->devname, devinfo->await_avg, devinfo->await_cv, devinfo->await, devinfo->prev_await);

	return ret;
}
