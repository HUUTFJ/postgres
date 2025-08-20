/*-------------------------------------------------------------------------
 *
 * vci_port_windows.h
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/include/vci_port_windows.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VCI_PORT_WINDOWS_H
#define VCI_PORT_WINDOWS_H

#define VCI_PATH_DELIMITER		'\\'
#define VCI_FULLPATH_FMT		"%s\\%s"
#define VCI_NAME_MAX			FILENAME_MAX
#define VCI_PATH_MAX			MAX_PATH

#if defined(_MSC_VER) || defined(__BORLANDC__)
#include <intrin.h>
#endif							/* #if defined(_MSC_VER) ||
								 * defined(__BORLANDC__) */

#endif							/* VCI_PORT_WINDOWS_H */
