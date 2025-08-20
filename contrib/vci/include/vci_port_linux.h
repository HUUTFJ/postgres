/*-------------------------------------------------------------------------
 *
 * vci_port_linux.h
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/include/vci_port_linux.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VCI_PORT_LINUX_H
#define VCI_PORT_LINUX_H

#include <sys/stat.h>
#include <unistd.h>

#include "lib/ilist.h"

#define VCI_DISKSTAT_FILE		"/proc/diskstats"
#define VCI_STAT_FILE			"/proc/stat"
#define VCI_MOUNTS_FILE			"/proc/mounts"

#define VCI_PATH_DELIMITER		'/'
#define VCI_FULLPATH_FMT		"%s/%s"
#define VCI_NAME_MAX			NAME_MAX
#define VCI_PATH_MAX			PATH_MAX

/**
 * values in /proc/diskstats
 * For more detail, see https://www.kernel.org/doc/Documentation/iostats.txt
 * each field corresponds to the one in the URL.
 */
typedef struct
{
	unsigned long rd_ios;		/* Field 1 */
	unsigned long rd_merges;	/* Field 2 */
	unsigned long rd_sec;		/* Field 3 */
	unsigned long rd_ticks;		/* Field 4 */
	unsigned long wr_ios;		/* Field 5 */
	unsigned long wr_merges;	/* Field 6 */
	unsigned long wr_sec;		/* Field 7 */
	unsigned int wr_ticks;		/* Field 8 */
	unsigned int ios_pgr;		/* Field 9 */
	unsigned int tot_ticks;		/* Field 10 */
	unsigned int rq_ticks;		/* Field 11 */
} vci_devstat_t;

#endif							/* VCI_PORT_LINUX_H */
