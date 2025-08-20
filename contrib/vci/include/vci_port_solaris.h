/*-------------------------------------------------------------------------
 *
 * vci_port_solaris.h
 *
 * This file have changes required for BIG ENDIAN architecture and previously added for Solaris support.
 * And, needed for IBMz release as well but the name of the file is retained from previous release.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/include/vci_port_solaris.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VCI_PORT_SOLARIS_H
#define VCI_PORT_SOLARIS_H

#include <sys/stat.h>
#include <unistd.h>
#include <c.h>
#include "lib/ilist.h"

#define VCI_PATH_DELIMITER		'/'

#define VCI_FULLPATH_FMT		"%s/%s"
#define VCI_MOUNTS_FILE			"/etc/mnttab"

#endif							/* VCI_PORT_SOLARIS_H */
