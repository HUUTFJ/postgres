/* -------------------------------------------------------------------------
 *
 * pg_subscription.h
 *	  definition of the shared subscription system catalog (pg_subscription)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_subscription.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_H
#define PG_SUBSCRIPTION_H

#include "access/xlogdefs.h"
#include "catalog/genbki.h"
#include "catalog/pg_subscription_d.h"	/* IWYU pragma: export */
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

/* ----------------
 *		pg_subscription definition. cpp turns this into
 *		typedef struct FormData_pg_subscription
 * ----------------
 */

/*
 * pg_subscription is the shared part of a subscription's catalog data.  It
 * must be shared because the logical replication launcher needs to find
 * subscriptions in all databases without connecting to each database.
 * Therefore, this catalog contains the subscription's identity and the fields
 * needed by cluster-wide processes.  It must not contain references to
 * database-local objects.
 *
 * The remaining subscription properties are stored in pg_subscription_db,
 * which is local to the database identified by subdbid.
 *
 * pg_subscription remains the catalog that represents the subscription as a
 * database object.  Object addresses, ownership dependencies, object locks,
 * and event-trigger notifications therefore use SubscriptionRelationId and
 * the OID of this catalog.
 */

BEGIN_CATALOG_STRUCT

CATALOG(pg_subscription,6100,SubscriptionRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6101,SubscriptionRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid			oid;			/* oid */

	Oid			subdbid BKI_LOOKUP(pg_database);	/* Database the
													 * subscription is in. */

	NameData	subname;		/* Name of the subscription */

	Oid			subowner BKI_LOOKUP(pg_authid); /* Owner of the subscription */

	bool		subenabled;		/* True if the subscription is enabled (the
								 * worker should be running) */

	bool		subretaindeadtuples;	/* True if dead tuples useful for
										 * conflict detection are retained */

	bool		subretentionactive; /* True if retain_dead_tuples is enabled
									 * and the retention duration has not
									 * exceeded max_retention_duration, when
									 * defined */
} FormData_pg_subscription;

END_CATALOG_STRUCT

typedef FormData_pg_subscription *Form_pg_subscription;

DECLARE_UNIQUE_INDEX_PKEY(pg_subscription_oid_index, 6114, SubscriptionObjectIndexId, pg_subscription, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_subscription_subname_index, 6115, SubscriptionNameIndexId, pg_subscription, btree(subdbid oid_ops, subname name_ops));

MAKE_SYSCACHE(SUBSCRIPTIONOID, pg_subscription_oid_index, 4);
MAKE_SYSCACHE(SUBSCRIPTIONNAME, pg_subscription_subname_index, 4);

/*
 * Subscription data structure used in memory. Attributes in pg_subscription
 * and pg_subscription_db are combined into this.
 */
typedef struct Subscription
{
	MemoryContext cxt;			/* mem cxt containing this subscription */

	Oid			oid;			/* Oid of the subscription */
	Oid			dbid;			/* Oid of the database which subscription is
								 * in */
	XLogRecPtr	skiplsn;		/* All changes finished at this LSN are
								 * skipped */
	char	   *name;			/* Name of the subscription */
	Oid			owner;			/* Oid of the subscription owner */
	bool		ownersuperuser; /* Is the subscription owner a superuser? */
	bool		enabled;		/* Indicates if the subscription is enabled */
	bool		binary;			/* Indicates if the subscription wants data in
								 * binary format */
	char		stream;			/* Allow streaming in-progress transactions.
								 * See LOGICALREP_STREAM_xxx constants. */
	char		twophasestate;	/* Allow streaming two-phase transactions */
	bool		disableonerr;	/* Indicates if the subscription should be
								 * automatically disabled if a worker error
								 * occurs */
	bool		passwordrequired;	/* Must connection use a password? */
	bool		runasowner;		/* Run replication as subscription owner */
	bool		failover;		/* True if the associated replication slots
								 * (i.e. the main slot and the table sync
								 * slots) in the upstream database are enabled
								 * to be synchronized to the standbys. */
	bool		retaindeadtuples;	/* True if dead tuples useful for conflict
									 * detection are retained */
	int32		maxretention;	/* The maximum duration (in milliseconds) for
								 * which information useful for conflict
								 * detection can be retained */
	bool		retentionactive;	/* True if retain_dead_tuples is enabled
									 * and the retention duration has not
									 * exceeded max_retention_duration, when
									 * defined */
	Oid			conflictlogrelid;	/* conflict log table Oid */
	char	   *conninfo;		/* Connection string to the publisher */
	char	   *slotname;		/* Name of the replication slot */
	char	   *synccommit;		/* Synchronous commit setting for worker */
	char	   *walrcvtimeout;	/* wal_receiver_timeout setting for worker */
	List	   *publications;	/* List of publication names to subscribe to */
	char	   *origin;			/* Only publish data originating from the
								 * specified origin */
	char	   *conflictlogdest;	/* Conflict log destination */
} Subscription;

extern Subscription *GetSubscription(Oid subid, bool missing_ok,
									 bool conninfo_needed,
									 bool conninfo_aclcheck);
extern void DisableSubscription(Oid subid);

extern int	CountDBSubscriptions(Oid dbid);

extern void GetPublicationsStr(List *publications, StringInfo dest,
							   bool quote_literal);

#endif							/* PG_SUBSCRIPTION_H */
