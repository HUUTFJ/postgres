/* -------------------------------------------------------------------------
 *
 * pg_subscription_db.h
 *	  definition of the per-database subscription system catalog
 *	  (pg_subscription_db)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_subscription_db.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_DB_H
#define PG_SUBSCRIPTION_DB_H

#include "access/xlogdefs.h"
#include "catalog/genbki.h"
#include "catalog/pg_subscription_db_d.h"	/* IWYU pragma: export */


/* ----------------
 *		pg_subscription_db definition. cpp turns this into
 *		typedef struct FormData_pg_subscription_db
 * ----------------
 */

/*
 * pg_subscription_db is the per-database part of a subscription's catalog
 * data.  It stores attributes that are only needed after connecting to the
 * subscription's database, including references to database-local objects.
 *
 * CAUTION:  There is a GRANT in system_views.sql to grant public select
 * access on all columns except subconninfo.  When adding a new column here,
 * be sure to update that GRANT or, if the new column should not be publicly
 * readable, update the associated comments and catalogs.sgml documentation.
 */

BEGIN_CATALOG_STRUCT

CATALOG(pg_subscription_db,9801,SubscriptionDbRelationId)
{
	Oid			oid BKI_LOOKUP(pg_subscription);		/* oid */

	XLogRecPtr	subskiplsn;		/* All changes finished at this LSN are
								 * skipped */

	bool		subbinary;		/* True if the subscription wants the
								 * publisher to send data in binary */

	char		substream;		/* Stream in-progress transactions. See
								 * LOGICALREP_STREAM_xxx constants. */

	char		subtwophasestate;	/* Stream two-phase transactions */

	bool		subdisableonerr;	/* True if a worker error should cause the
									 * subscription to be disabled */

	bool		subpasswordrequired;	/* Must connection use a password? */

	bool		subrunasowner;	/* True if replication should execute as the
								 * subscription owner */

	bool		subfailover;	/* True if the associated replication slots
								 * (i.e. the main slot and the table sync
								 * slots) in the upstream database are enabled
								 * to be synchronized to the standbys. */

	int32		submaxretention;	/* The maximum duration (in milliseconds)
									 * for which information useful for
									 * conflict detection can be retained */

	Oid			subserver BKI_LOOKUP_OPT(pg_foreign_server);	/* If connection uses
																 * server */

	Oid			subconflictlogrelid;	/* Relid of the conflict log table. */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/*
	 * Strategy for logging replication conflicts: 'log' - server log only,
	 * 'table' - conflict log table only, 'all' - both log and table.
	 */
	text		subconflictlogdest BKI_FORCE_NOT_NULL;

	/* Connection string to the publisher */
	text		subconninfo;	/* Set if connecting with connection string */

	/* Slot name on publisher */
	NameData	subslotname BKI_FORCE_NULL;

	/* Synchronous commit setting for worker */
	text		subsynccommit BKI_FORCE_NOT_NULL;

	/* wal_receiver_timeout setting for worker */
	text		subwalrcvtimeout BKI_FORCE_NOT_NULL;

	/* List of publications subscribed to */
	text		subpublications[1] BKI_FORCE_NOT_NULL;

	/* Only publish data originating from the specified origin */
	text		suborigin BKI_DEFAULT(LOGICALREP_ORIGIN_ANY);
#endif
} FormData_pg_subscription_db;

END_CATALOG_STRUCT

typedef FormData_pg_subscription_db *Form_pg_subscription_db;

DECLARE_TOAST(pg_subscription_db, 9802, 9803);

DECLARE_UNIQUE_INDEX_PKEY(pg_subscription_db_oid_index, 9804, SubscriptionDbObjectIndexId, pg_subscription_db, btree(oid oid_ops));

MAKE_SYSCACHE(SUBSCRIPTIONDBOID, pg_subscription_db_oid_index, 4);

#ifdef EXPOSE_TO_CLIENT_CODE

/*
 * two_phase tri-state values. See comments atop worker.c to know more about
 * these states.
 */
#define LOGICALREP_TWOPHASE_STATE_DISABLED 'd'
#define LOGICALREP_TWOPHASE_STATE_PENDING 'p'
#define LOGICALREP_TWOPHASE_STATE_ENABLED 'e'

/*
 * The subscription will request the publisher to only send changes that do not
 * have any origin.
 */
#define LOGICALREP_ORIGIN_NONE "none"

/*
 * The subscription will request the publisher to send changes regardless
 * of their origin.
 */
#define LOGICALREP_ORIGIN_ANY "any"

/* Disallow streaming in-progress transactions. */
#define LOGICALREP_STREAM_OFF 'f'

/*
 * Streaming in-progress transactions are written to a temporary file and
 * applied only after the transaction is committed on upstream.
 */
#define LOGICALREP_STREAM_ON 't'

/*
 * Streaming in-progress transactions are applied immediately via a parallel
 * apply worker.
 */
#define LOGICALREP_STREAM_PARALLEL 'p'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_SUBSCRIPTION_DB_H */
