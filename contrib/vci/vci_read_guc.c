/*-------------------------------------------------------------------------
 *
 * vci_read_guc.c
 *	  GUC parameter settings
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/vci_read_guc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/procnumber.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/palloc.h"

#include "vci.h"

#include "vci_executor.h"
#include "vci_mem.h"
#include "vci_port.h"

/* GUC parameter holder */
VciGucStruct VciGuc;

static void check_max_worker_processes(void);

static struct config_bool VciConfigureNamesBool[] =
{
	/* for internal use */
	{
		{
			"vci.enable",
			PGC_USERSET, RESOURCES_MEM,
			"Enables VCI.",
			NULL,
		},
		&VciGuc.enable,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.log_query",
			PGC_USERSET, RESOURCES_MEM,
			"Logs information when a query fails to be executed by VCI.",
			NULL,
		},
		&VciGuc.log_query,
		false,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_seqscan",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace sequential-scan plans.",
			NULL,
		},
		&VciGuc.enable_seqscan,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_indexscan",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace index-scan plans.",
			NULL,
		},
		&VciGuc.enable_indexscan,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_bitmapheapscan",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace bitmap-scan plans.",
			NULL,
		},
		&VciGuc.enable_bitmapheapscan,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_sort",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace sort plans.",
			NULL,
		},
		&VciGuc.enable_sort,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_hashagg",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace hashed aggregation plans.",
			NULL,
		},
		&VciGuc.enable_hashagg,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_sortagg",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace sorted aggregation plans.",
			NULL,
		},
		&VciGuc.enable_sortagg,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_plainagg",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace plain aggregation plans.",
			NULL,
		},
		&VciGuc.enable_plainagg,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_hashjoin",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace hash join plans.",
			NULL,
		},
		&VciGuc.enable_hashjoin,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_nestloop",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Enables VCI planner to replace nested-loop plans.",
			NULL,
		},
		&VciGuc.enable_nestloop,
		true,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.enable_ros_control_daemon",
			PGC_POSTMASTER, RESOURCES_MEM,
			"Enables the VCI ROS Control Daemon.",
			NULL,
		},
		&VciGuc.enable_ros_control_daemon,
		false,
		NULL, NULL, NULL,
	},

};

static struct config_int VciConfigureNamesInt[] =
{
	{
		{
			"vci.cost_threshold",
			PGC_USERSET, RESOURCES_MEM,
			"Sets the threshold CPU load beyond which the VCI control worker is stopped.",
			NULL,
		},
		&VciGuc.cost_threshold,
		18000, 0, INT_MAX,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.maintenance_work_mem",
			PGC_SIGHUP, RESOURCES_MEM,
			"Sets the maximum memory to be used by each control worker for VCI control operations.",
			NULL,
			GUC_UNIT_KB,
		},
		&VciGuc.maintenance_work_mem,
		256 * 1024, 1024, MAX_KILOBYTES,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.max_devices",
			PGC_SIGHUP, RESOURCES_MEM,
			"Sets the maximum device number which can be attached.",
			NULL,
		},
		&VciGuc.max_devices,
		16, 1, 1048576,
		NULL, NULL, NULL,
	},

	/* **************************************** */
	/* ROS Control Daemon/Worker configurations */
	/* **************************************** */

	/* Daemon setup */
	{
		{
			"vci.control_max_workers",
			PGC_POSTMASTER, RESOURCES_IO,
			"Sets the maximum number of simultaneously running VCI control worker processes.",
			NULL,
		},
		&VciGuc.control_max_workers,
		8, 1, MAX_BACKENDS,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.control_naptime",
			PGC_SIGHUP, RESOURCES_IO,
			"Time to sleep between VCI control worker runs.",
			NULL,
			GUC_UNIT_S,
		},
		&VciGuc.control_naptime,
		1, 1, INT_MAX / 1000,
		NULL, NULL, NULL,
	},

	/* Worker : ROS control command thresholds  */

	{
		{
			"vci.wosros_conv_threshold",
			PGC_SIGHUP, RESOURCES_MEM,
			"Sets the threshold of Data WOS rows to execute WOS->ROS conversion.",
			NULL,
		},
		&VciGuc.wosros_conv_threshold,
		256 * 1024, 1, INT_MAX,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.cdr_threshold",
			PGC_SIGHUP, RESOURCES_MEM,
			"Sets the threshold of deleted rows in ROS to execute collect-deleted-rows command.",
			NULL,
		},
		&VciGuc.cdr_threshold,
		128 * 1024, 1, INT_MAX,
		NULL, NULL, NULL,
	},

	/******************************************/
	/* Custom Plan Execution                  */
	/******************************************/

	{
		{
			"vci.max_local_ros",
			PGC_USERSET, RESOURCES_MEM,
			"Sets the maximum local ROS memory.",
			NULL,
			GUC_UNIT_KB,
		},
		&VciGuc.max_local_ros_size,
		64 * 1024, 64 * 1024, INT_MAX,
		NULL, NULL, NULL,
	},

	{
		{
			"vci.table_rows_threshold",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Sets the threshold of table rows to execute VCI Scan.",
			NULL,
		},
		&VciGuc.table_rows_threshold,
		VCI_MAX_FETCHING_ROWS, 0, INT_MAX,
		NULL, NULL, NULL,
	},

};

static const struct config_enum_entry table_scan_policy_options[] = {

	{"column store only", VCI_TABLE_SCAN_POLICY_COLUMN_ONLY, false},
	{"column only", VCI_TABLE_SCAN_POLICY_COLUMN_ONLY, true},
	{"none", VCI_TABLE_SCAN_POLICY_NONE, false},
	{NULL, 0, false}
};

static struct config_enum VciConfigureNamesEnum[] =
{
	{
		{
			"vci.table_scan_policy",
			PGC_USERSET, DEVELOPER_OPTIONS,
			"Sets the policy that a scan node reads from the column store table(VCI index) or the row store table(original).",
			NULL,
		},
		&VciGuc.table_scan_policy,
		VCI_TABLE_SCAN_POLICY_COLUMN_ONLY,
		table_scan_policy_options,
		NULL, NULL, NULL
	}
};

/*
 * Set GUC parameters
 */
void
vci_read_guc_variables(void)
{
	int			i;

	/*
	 * TODO: Raise warnings or set parameters to default, when the specified
	 * value is out-of-range.
	 */
	for (i = 0; i < (int) lengthof(VciConfigureNamesBool); i++)
	{
		struct config_bool *conf = &VciConfigureNamesBool[i];

		if (IsPostmasterEnvironment)
			DefineCustomBoolVariable(conf->gen.name,
									 conf->gen.short_desc,
									 conf->gen.long_desc,
									 conf->variable,
									 conf->boot_val,
									 conf->gen.context,
									 conf->gen.flags,
									 conf->check_hook,
									 conf->assign_hook,
									 conf->show_hook);
		else
			*(conf->variable) = conf->boot_val;

	}

	for (i = 0; i < (int) lengthof(VciConfigureNamesInt); i++)
	{
		struct config_int *conf = &VciConfigureNamesInt[i];

		if (IsPostmasterEnvironment)
			DefineCustomIntVariable(conf->gen.name,
									conf->gen.short_desc,
									conf->gen.long_desc,
									conf->variable,
									conf->boot_val,
									conf->min,
									conf->max,
									conf->gen.context,
									conf->gen.flags,
									conf->check_hook,
									conf->assign_hook,
									conf->show_hook);
		else
			*(conf->variable) = conf->boot_val;
	}

	/* FIXME: Add initial value to pass Assert() */
	VciGuc.table_scan_policy = VCI_TABLE_SCAN_POLICY_COLUMN_ONLY;

	for (i = 0; i < (int) lengthof(VciConfigureNamesEnum); i++)
	{
		struct config_enum *conf = &VciConfigureNamesEnum[i];

		if (IsPostmasterEnvironment)
			DefineCustomEnumVariable(conf->gen.name,
									 conf->gen.short_desc,
									 conf->gen.long_desc,
									 conf->variable,
									 conf->boot_val,
									 conf->options,
									 conf->gen.context,
									 conf->gen.flags,
									 conf->check_hook,
									 conf->assign_hook,
									 conf->show_hook);
		else
			*(conf->variable) = conf->boot_val;
	}

	VciGuc.have_loaded_postgresql_conf = true;

	check_max_worker_processes();
}

/*
 * Check for max_worker_processes
 */
static void
check_max_worker_processes(void)
{
	int			num_needed_workers;

	num_needed_workers = 1 + VciGuc.control_max_workers;	/* ros control daemon &
															 * workers */
	num_needed_workers += 1;	/* parallel control daemon  */

	if (num_needed_workers > MAX_BACKENDS)
		num_needed_workers = MAX_BACKENDS;

	if (max_worker_processes < num_needed_workers)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg(VCI_STRING " needs to set at least %d to \"max_worker_processes\"",
						num_needed_workers)));
}
