/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *	  The postgres vacuum cleaner.
 *
 * This file now includes only control and dispatch code for VACUUM and
 * ANALYZE commands.  Regular VACUUM is implemented in vacuumlazy.c,
 * ANALYZE in analyze.c, and VACUUM FULL is a variant of CLUSTER, handled
 * in cluster.c.
 *
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/clog.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/visibilitymap.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_index.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "commands/analyzeutils.h"
#include "commands/cluster.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbdispatchresult.h"      /* CdbDispatchResults */
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"             /* pq_beginmessage() etc. */
#include "miscadmin.h"
#include "pgstat.h"
#include "parser/parse_relation.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "access/distributedlog.h"
#include "catalog/heap.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_inherits_fn.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "nodes/makefuncs.h"     /* makeRangeVar */
#include "pgstat.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"


/*
 * GUC parameters
 */
int			vacuum_freeze_min_age;
int			vacuum_freeze_table_age;
int			vacuum_multixact_freeze_min_age;
int			vacuum_multixact_freeze_table_age;


typedef struct VacuumStatsContext
{
	List	   *updated_stats;
} VacuumStatsContext;


/* A few variables that don't seem worth passing around as parameters */
static MemoryContext vac_context = NULL;

static BufferAccessStrategy vac_strategy;

/* non-export function prototypes */
static List *get_rel_oids(Oid relid, VacuumStmt *vacstmt, int stmttype);
static void vac_truncate_clog(TransactionId frozenXID,
							  MultiXactId minMulti,
							  TransactionId lastSaneFrozenXid,
							  MultiXactId lastSaneMinMulti);
static bool vacuum_rel(Relation onerel, Oid relid, VacuumStmt *vacstmt, LOCKMODE lmode,
		   bool for_wraparound);
static void dispatchVacuum(VacuumStmt *vacstmt, VacuumStatsContext *ctx);
static void vacuumStatement_Relation(VacuumStmt *vacstmt, Oid relid,
						 List *relations, BufferAccessStrategy bstrategy,
						 bool do_toast,
						 bool for_wraparound, bool isTopLevel);

static void
vacuum_combine_stats(VacuumStatsContext *stats_context,
					CdbPgResults* cdb_pgresults);

static void
vac_update_relstats_from_list(List *updated_stats);

/*
 * Primary entry point for VACUUM and ANALYZE commands.
 *
 * relid is normally InvalidOid; if it is not, then it provides the relation
 * OID to be processed, and vacstmt->relation is ignored.  (The non-invalid
 * case is currently only used by autovacuum.)
 *
 * do_toast is passed as FALSE by autovacuum, because it processes TOAST
 * tables separately.
 *
 * for_wraparound is used by autovacuum to let us know when it's forcing
 * a vacuum for wraparound, which should not be auto-canceled.
 *
 * bstrategy is normally given as NULL, but in autovacuum it can be passed
 * in to use the same buffer strategy object across multiple vacuum() calls.
 *
 * isTopLevel should be passed down from ProcessUtility.
 *
 * It is the caller's responsibility that vacstmt and bstrategy
 * (if given) be allocated in a memory context that won't disappear
 * at transaction commit.
 */
void
vacuum(VacuumStmt *vacstmt, Oid relid, bool do_toast,
	   BufferAccessStrategy bstrategy, bool for_wraparound, bool isTopLevel)
{
	const char *stmttype;
	volatile bool in_outer_xact,
				use_own_xacts;
	List	   *vacuum_relations = NIL;
	List	   *analyze_relations = NIL;

	if ((vacstmt->options & VACOPT_VACUUM) &&
		(vacstmt->options & VACOPT_ROOTONLY))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("ROOTPARTITION option cannot be used together with VACUUM, try ANALYZE ROOTPARTITION")));
	static bool in_vacuum = false;

	/* sanity checks on options */
	Assert(vacstmt->options & (VACOPT_VACUUM | VACOPT_ANALYZE));
	Assert((vacstmt->options & VACOPT_VACUUM) ||
		   !(vacstmt->options & (VACOPT_FULL | VACOPT_FREEZE)));
	Assert((vacstmt->options & VACOPT_ANALYZE) || vacstmt->va_cols == NIL);

	stmttype = (vacstmt->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";

	/*
	 * We cannot run VACUUM inside a user transaction block; if we were inside
	 * a transaction, then our commit- and start-transaction-command calls
	 * would not have the intended effect!	There are numerous other subtle
	 * dependencies on this, too.
	 *
	 * ANALYZE (without VACUUM) can run either way.
	 */
	if (vacstmt->options & VACOPT_VACUUM)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
			PreventTransactionChain(isTopLevel, stmttype);
		in_outer_xact = false;
	}
	else
		in_outer_xact = IsInTransactionChain(isTopLevel);

	/*
	 * Due to static variables vac_context, anl_context and vac_strategy,
	 * vacuum() is not reentrant.  This matters when VACUUM FULL or ANALYZE
	 * calls a hostile index expression that itself calls ANALYZE.
	 */
	if (in_vacuum)
		elog(ERROR, "%s cannot be executed from VACUUM or ANALYZE", stmttype);

	/*
	 * Send info about dead objects to the statistics collector, unless we are
	 * in autovacuum --- autovacuum.c does this for itself.
	 */
	if ((vacstmt->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess())
		pgstat_vacuum_stat();

	/*
	 * Create special memory context for cross-transaction storage.
	 *
	 * Since it is a child of PortalContext, it will go away eventually even
	 * if we suffer an error; there's no need for special abort cleanup logic.
	 */
	vac_context = AllocSetContextCreate(PortalContext,
										"Vacuum",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * If caller didn't give us a buffer strategy object, make one in the
	 * cross-transaction memory context.
	 */
	if (bstrategy == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(vac_context);

		bstrategy = GetAccessStrategy(BAS_VACUUM);
		MemoryContextSwitchTo(old_context);
	}
	vac_strategy = bstrategy;

	/*
	 * Build list of relations to process, unless caller gave us one. (If we
	 * build one, we put it in vac_context for safekeeping.)
	 */

	/*
	 * Analyze on midlevel partition is not allowed directly so
	 * vacuum_relations and analyze_relations may be different.  In case of
	 * partitioned tables, vacuum_relation will contain all OIDs of the
	 * partitions of a partitioned table. However, analyze_relation will
	 * contain all the OIDs of partition of a partitioned table except midlevel
	 * partition unless midlevel partition analyze is enabled.
	 */
	if (vacstmt->options & VACOPT_VACUUM)
	{
		vacuum_relations = get_rel_oids(relid, vacstmt, VACOPT_VACUUM);
	}
	if (vacstmt->options & VACOPT_ANALYZE)
		analyze_relations = get_rel_oids(relid, vacstmt, VACOPT_ANALYZE);

	/*
	 * Decide whether we need to start/commit our own transactions.
	 *
	 * For VACUUM (with or without ANALYZE): always do so on the query
	 * dispatcher, so that we can release locks as soon as possible.  On the
	 * query executor we skip this and use the outer transaction when skipping
	 * two phase commit, as the expectation is that it will be a separate
	 * dispatch for every table to be vacuumed.
	 *
	 * For ANALYZE (no VACUUM): if inside a transaction block, we cannot
	 * start/commit our own transactions.  Also, there's no need to do so if
	 * only processing one relation.  For multiple relations when not within a
	 * transaction block, and also in an autovacuum worker, use own
	 * transactions so we can release locks sooner.
	 */
	if (vacstmt->options & VACOPT_VACUUM)
		if (Gp_role == GP_ROLE_EXECUTE && vacstmt->skip_twophase)
			use_own_xacts = false;
		else
			use_own_xacts = true;
	else
	{
		Assert(vacstmt->options & VACOPT_ANALYZE);
		if (IsAutoVacuumWorkerProcess())
			use_own_xacts = true;
		else if (in_outer_xact)
			use_own_xacts = false;
		else if (list_length(analyze_relations) > 1)
			use_own_xacts = true;
		else
			use_own_xacts = false;
	}

	/*
	 * vacuum_rel expects to be entered with no transaction active; it will
	 * start and commit its own transaction.  But we are called by an SQL
	 * command, and so we are executing inside a transaction already. We
	 * commit the transaction started in PostgresMain() here, and start
	 * another one before exiting to match the commit waiting for us back in
	 * PostgresMain().
	 */
	if (use_own_xacts)
	{
		Assert(!in_outer_xact);

		/* ActiveSnapshot is not set by autovacuum */
		if (ActiveSnapshotSet())
			PopActiveSnapshot();

		/* matches the StartTransaction in PostgresMain() */
		if (Gp_role != GP_ROLE_EXECUTE)
			CommitTransactionCommand();
	}

	/* Turn vacuum cost accounting on or off */
	PG_TRY();
	{
		ListCell   *cur;

		in_vacuum = true;
		VacuumCostActive = (VacuumCostDelay > 0);
		VacuumCostBalance = 0;
		VacuumPageHit = 0;
		VacuumPageMiss = 0;
		VacuumPageDirty = 0;

		if (vacstmt->options & VACOPT_VACUUM)
		{
			/*
			 * Loop to process each selected relation which needs to be
			 * vacuumed.
			 */
			foreach(cur, vacuum_relations)
			{
				Oid			relid = lfirst_oid(cur);

				vacuumStatement_Relation(vacstmt, relid, vacuum_relations, bstrategy, do_toast, for_wraparound, isTopLevel);
			}
		}

		if (vacstmt->options & VACOPT_ANALYZE)
		{
			/*
			 * If there are no partition tables in the database and ANALYZE
			 * ROOTPARTITION ALL is executed, report a WARNING as no root
			 * partitions are there to be analyzed
			 */
			if ((vacstmt->options & VACOPT_ROOTONLY) && NIL == analyze_relations && !vacstmt->relation)
			{
				ereport(NOTICE,
						(errmsg("there are no partitioned tables in database to ANALYZE ROOTPARTITION")));
			}

			/*
			 * Loop to process each selected relation which needs to be analyzed.
			 */
			foreach(cur, analyze_relations)
			{
				Oid			relid = lfirst_oid(cur);

				/*
				 * If using separate xacts, start one for analyze. Otherwise,
				 * we can use the outer transaction.
				 */
				if (use_own_xacts)
				{
					StartTransactionCommand();
					/* functions in indexes may want a snapshot set */
					PushActiveSnapshot(GetTransactionSnapshot());
				}

				analyze_rel(relid, vacstmt, in_outer_xact, vac_strategy);

				if (use_own_xacts)
				{
					PopActiveSnapshot();
					CommitTransactionCommand();
				}
				else
				{
					/*
					 * If we're not using separate xacts, better separate the
					 * ANALYZE actions with CCIs.  This avoids trouble if user
					 * says "ANALYZE t, t".
					 */
					CommandCounterIncrement();
				}
			}
		}
	}
	PG_CATCH();
	{
		in_vacuum = false;
		VacuumCostActive = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	in_vacuum = false;
	VacuumCostActive = false;

	/*
	 * Finish up processing.
	 */
	if (use_own_xacts)
	{
		/* here, we are not in a transaction */

		StartTransactionCommand();
	}

	if ((vacstmt->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess())
	{
		/*
		 * Update pg_database.datfrozenxid, and truncate pg_clog if possible.
		 * (autovacuum.c does this for itself.)
		 */
		vac_update_datfrozenxid();
	}

	/*
	 * Clean up working storage --- note we must do this after
	 * StartTransactionCommand, else we might be trying to delete the active
	 * context!
	 */
	MemoryContextDelete(vac_context);
	vac_context = NULL;
}

bool
vacuumStatement_IsTemporary(Relation onerel)
{
	bool bTemp = false;
	/* MPP-7576: don't track internal namespace tables */
	switch (RelationGetNamespace(onerel))
	{
		case PG_CATALOG_NAMESPACE:
			/* MPP-7773: don't track objects in system namespace
			 * if modifying system tables (eg during upgrade)
			 */
			if (allowSystemTableMods)
				bTemp = true;
			break;

		case PG_TOAST_NAMESPACE:
		case PG_BITMAPINDEX_NAMESPACE:
			bTemp = true;
			break;
		default:
			break;
	}

	/* MPP-7572: Don't track metadata if table in any
	 * temporary namespace
	 */
	if (!bTemp)
		bTemp = isAnyTempNamespace(RelationGetNamespace(onerel));
	return bTemp;
}

/*
 * Modify the Vacuum statement to vacuum an individual
 * relation. This ensures that only one relation will be
 * locked for vacuum, when the user issues a "vacuum <db>"
 * command, or a "vacuum <parent_partition_table>"
 * command.
 */
static void
vacuumStatement_AssignRelation(VacuumStmt *vacstmt, Oid relid, List *relations)
{
	if (list_length(relations) > 1 || vacstmt->relation == NULL)
	{
		char	*relname		= get_rel_name(relid);
		char	*namespace_name =
			get_namespace_name(get_rel_namespace(relid));

		if (relname == NULL)
		{
			elog(ERROR, "Relation name does not exist for relation with oid %d", relid);
			return;
		}

		if (namespace_name == NULL)
		{
			elog(ERROR, "Namespace does not exist for relation with oid %d", relid);
			return;
		}

		/* XXX: dispatch OID than name */
		vacstmt->relation = makeRangeVar(namespace_name, relname, -1);
	}
}

/*
 * Processing of the vacuumStatement for given relid.
 *
 * The function is called by vacuumStatement once for each relation to vacuum.
 * In order to connect QD and QE work for vacuum, we employ a little
 * complicated mechanism here; we separate one relation vacuum process
 * to separate steps and perform each step in separate transactions, so that
 * QD can open a distributed transaction and embrace QE work inside it.  As
 * opposed to old postgres code, where one transaction is opened and closed for
 * each auxiliary relation, here a transaction processes them as a set starting
 * from the base relation.
 *
 * For heap VACUUM we disable two-phase commit, because we do not actually make
 * any logical changes to the tables. Even if a VACUUM transaction fails on one
 * of the QE segments, it should not matter, because the data has not logically
 * changed on disk. VACUUM FULL and lazy vacuum are both completed in one
 * transaction.
 *
 * While executing these multiple transactions, we acquire a session
 * lock across transactions, in order to keep concurrent work on the
 * same relation away.  It doesn't look intuitive, though, if you look
 * at QE work, because from its perspective it is always one step, therefore
 * there is no session lock technically (we actually acquire and release
 * it as it's harmless.)  Session lock doesn't work here, because QE
 * is under a distributed transaction and we haven't supported session
 * lock recording in transaction prepare.  This should be ok as far as
 * we are dealing with user table, because other MPP work also tries
 * to take a relation lock, which would conflict with this vacuum work
 * on master.  Be careful with catalog tables, because we take locks on
 * them and release soon much before the end of transaction.  That means
 * QE still needs to deal with concurrent work well.
 */
static void
vacuumStatement_Relation(VacuumStmt *vacstmt, Oid relid,
						 List *relations, BufferAccessStrategy bstrategy,
						 bool do_toast, bool for_wraparound, bool isTopLevel)
{
	LOCKMODE			lmode = NoLock;
	Relation			onerel;
	LockRelId			onerelid;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(vac_context);

	vacstmt = copyObject(vacstmt);
	/* VACUUM, without ANALYZE */
	vacstmt->options &= ~VACOPT_ANALYZE;
	vacstmt->options |= VACOPT_VACUUM;
	vacstmt->va_cols = NIL;		/* A plain VACUUM cannot list columns */

	MemoryContextSwitchTo(oldcontext);

	/*
	 * For each iteration we start/commit our own transactions,
	 * so that we can release resources such as locks and memories,
	 * and we can also safely perform non-transactional work
	 * along with transactional work. If we are a query executor and skipping
	 * a two phase commit, the expectation is that we will vacuum one relation
	 * per dispatch, so we can use the outer transaction for this instead.
	 */
	if (Gp_role != GP_ROLE_EXECUTE || !vacstmt->skip_twophase)
		StartTransactionCommand();

	/*
	 * Functions in indexes may want a snapshot set. Also, setting
	 * a snapshot ensures that RecentGlobalXmin is kept truly recent.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Determine the type of lock we want --- hard exclusive lock for a FULL
	 * vacuum, but just ShareUpdateExclusiveLock for concurrent vacuum. Either
	 * way, we can be sure that no other backend is vacuuming the same table.
	 * For analyze, we use ShareUpdateExclusiveLock.
	 */
	if (!(vacstmt->options & VACOPT_VACUUM))
		lmode = ShareUpdateExclusiveLock;
	else
		lmode = (vacstmt->options & VACOPT_FULL) ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	/*
	 * Open the relation and get the appropriate lock on it.
	 *
	 * There's a race condition here: the rel may have gone away since the
	 * last time we saw it.  If so, we don't need to vacuum it.
	 */
	onerel = try_relation_open(relid, lmode, false /* dontwait */);
	if (!onerel)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}
	SIMPLE_FAULT_INJECTOR("vacuum_hold_lock");
	/*
	 * Check permissions.
	 *
	 * We allow the user to vacuum a table if he is superuser, the table
	 * owner, or the database owner (but in the latter case, only if it's not
	 * a shared relation).	pg_class_ownercheck includes the superuser case.
	 *
	 * Note we choose to treat permissions failure as a WARNING and keep
	 * trying to vacuum the rest of the DB --- is this appropriate?
	 */
	if (!(pg_class_ownercheck(RelationGetRelid(onerel), GetUserId()) ||
		  (pg_database_ownercheck(MyDatabaseId, GetUserId()) && !onerel->rd_rel->relisshared)))
	{
		if (Gp_role != GP_ROLE_EXECUTE)
		{
			if (onerel->rd_rel->relisshared)
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser can vacuum it",
								RelationGetRelationName(onerel))));
			else if (onerel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser or database owner can vacuum it",
								RelationGetRelationName(onerel))));
			else
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
								RelationGetRelationName(onerel))));
		}
		relation_close(onerel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	/*
	 * Check that it's a vacuumable relation; we used to do this in
	 * get_rel_oids() but seems safer to check after we've locked the
	 * relation.
	 */
	if ((onerel->rd_rel->relkind != RELKIND_RELATION &&
		 onerel->rd_rel->relkind != RELKIND_MATVIEW &&
		 onerel->rd_rel->relkind != RELKIND_TOASTVALUE)
		|| RelationIsForeign(onerel))
	{
		ereport(WARNING,
				(errmsg("skipping \"%s\" --- cannot vacuum non-tables, foreign tables or special system tables",
						RelationGetRelationName(onerel))));
		relation_close(onerel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	/*
	 * Silently ignore tables that are temp tables of other backends ---
	 * trying to vacuum these will lead to great unhappiness, since their
	 * contents are probably not up-to-date on disk.  (We don't throw a
	 * warning here; it would just lead to chatter during a database-wide
	 * VACUUM.)
	 */
	if (isOtherTempNamespace(RelationGetNamespace(onerel)))
	{
		relation_close(onerel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	/*
	 * Get a session-level lock too. This will protect our access to the
	 * relation across multiple transactions, so that we can vacuum the
	 * relation's TOAST table (if any) secure in the knowledge that no one is
	 * deleting the parent relation.
	 *
	 * NOTE: this cannot block, even if someone else is waiting for access,
	 * because the lock manager knows that both lock requests are from the
	 * same process.
	 */
	onerelid = onerel->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&onerelid, lmode);

	oldcontext = MemoryContextSwitchTo(vac_context);
	vacuumStatement_AssignRelation(vacstmt, relid, relations);
	MemoryContextSwitchTo(oldcontext);

	if (Gp_role == GP_ROLE_DISPATCH)
		vacstmt->skip_twophase = true;

	vacuum_rel(onerel, relid, vacstmt, lmode, for_wraparound);

	/*
	 * Complete the transaction and free all temporary memory used.
	 */
	PopActiveSnapshot();
	/*
	 * Transaction commit is always executed on QD.
	 */
	if (Gp_role != GP_ROLE_EXECUTE)
		CommitTransactionCommand();

	onerel = NULL;

	if (lmode != NoLock)
	{
		UnlockRelationIdForSession(&onerelid, lmode);
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * We need some transaction to update the catalog.  We could do
		 * it on the outer vacuumStatement, but it is useful to track
		 * relation by relation.
		 */
		//if (!istemp) // FIXME
		{
			char *vsubtype = ""; /* NOFULL */
			bool		start_xact = false;

			if (!onerel)
			{
				StartTransactionCommand();
				start_xact = true;
			}

			if (IsAutoVacuumWorkerProcess())
				vsubtype = "AUTO";
			else
			{
				if ((vacstmt->options & VACOPT_FULL) &&
					(0 == vacstmt->freeze_min_age))
					vsubtype = "FULL FREEZE";
				else if ((vacstmt->options & VACOPT_FULL))
					vsubtype = "FULL";
				else if (0 == vacstmt->freeze_min_age)
					vsubtype = "FREEZE";
			}
			MetaTrackUpdObject(RelationRelationId,
							   relid,
							   GetUserId(),
							   "VACUUM",
							   vsubtype);
			if (start_xact)
				CommitTransactionCommand();
		}
	}

	if (onerel)
	{
		relation_close(onerel, NoLock);
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
}

/*
 * Build a list of Oids for each relation to be processed
 *
 * The list is built in vac_context so that it will survive across our
 * per-relation transactions.
 *
 * 'stmttype' is either VACOPT_VACUUM or VACOPT_ANALYZE, to indicate
 * whether we should fetch the list for VACUUM or ANALYZE. It's
 * passed as a separate argument, so that the caller can build
 * separate lists for a combined "VACUUM ANALYZE".
 */
static List *
get_rel_oids(Oid relid, VacuumStmt *vacstmt, int stmttype)
{
	List	   *oid_list = NIL;
	MemoryContext oldcontext;

	Assert(stmttype == VACOPT_VACUUM || stmttype == VACOPT_ANALYZE);

	/* OID supplied by VACUUM's caller? */
	if (OidIsValid(relid))
	{
		oldcontext = MemoryContextSwitchTo(vac_context);
		oid_list = lappend_oid(oid_list, relid);
		MemoryContextSwitchTo(oldcontext);
	}
	else if (vacstmt->relation)
	{
		if (stmttype == VACOPT_VACUUM)
		{
			/* Process a specific relation */
			Oid			relid;
			List	   *prels = NIL;

			/*
			 * Since we don't take a lock here, the relation might be gone, or the
			 * RangeVar might no longer refer to the OID we look up here.  In the
			 * former case, VACUUM will do nothing; in the latter case, it will
			 * process the OID we looked up here, rather than the new one. Neither
			 * is ideal, but there's little practical alternative, since we're
			 * going to commit this transaction and begin a new one between now
			 * and then.
			 */
			relid = RangeVarGetRelid(vacstmt->relation, NoLock, false);

			if (rel_is_partitioned(relid))
			{
				PartitionNode *pn;

				pn = get_parts(relid, 0, 0, false, true /*includesubparts*/);

				prels = all_partition_relids(pn);
			}
			else if (rel_is_child_partition(relid))
			{
				/* get my children */
				prels = find_all_inheritors(relid, NoLock, NULL);
			}

			/* Make a relation list entry for this relation */
			oldcontext = MemoryContextSwitchTo(vac_context);
			oid_list = lappend_oid(oid_list, relid);
			oid_list = list_concat_unique_oid(oid_list, prels);
			MemoryContextSwitchTo(oldcontext);
		}
		else
		{
			oldcontext = MemoryContextSwitchTo(vac_context);
			/**
			 * ANALYZE one relation (optionally, a list of columns).
			 */
			Oid relationOid = InvalidOid;

			relationOid = RangeVarGetRelid(vacstmt->relation, NoLock, false);
			PartStatus ps = rel_part_status(relationOid);

			if (ps != PART_STATUS_ROOT && (vacstmt->options & VACOPT_ROOTONLY))
			{
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- cannot analyze a non-root partition using ANALYZE ROOTPARTITION",
								get_rel_name(relationOid))));
			}
			else if (ps == PART_STATUS_ROOT)
			{
				PartitionNode *pn = get_parts(relationOid, 0 /*level*/ ,
											  0 /*parent*/, false /* inctemplate */, true /*includesubparts*/);
				Assert(pn);
				if (!(vacstmt->options & VACOPT_ROOTONLY))
				{
					oid_list = all_leaf_partition_relids(pn); /* all leaves */


				}
				oid_list = lappend_oid(oid_list, relationOid); /* root partition */
			}
			else if (ps == PART_STATUS_LEAF)
			{
				Oid root_rel_oid = rel_partition_get_master(relationOid);
				oid_list = list_make1_oid(relationOid);

				List *va_root_attnums = NIL;
				if (vacstmt->va_cols != NIL)
				{
					ListCell *lc;
					int i;
					foreach(lc, vacstmt->va_cols)
					{
						char	   *col = strVal(lfirst(lc));

						i = get_attnum(root_rel_oid, col);
						if (i == InvalidAttrNumber)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_COLUMN),
									 errmsg("column \"%s\" of relation \"%s\" does not exist",
											col, get_rel_name(root_rel_oid))));
						va_root_attnums = lappend_int(va_root_attnums, i);
					}
				}
				else
				{
					Relation onerel = RelationIdGetRelation(root_rel_oid);
					int attr_cnt = onerel->rd_att->natts;
					for (int i = 1; i <= attr_cnt; i++)
					{
						Form_pg_attribute attr = onerel->rd_att->attrs[i-1];
						if (attr->attisdropped || attr->attstattarget == 0)
							continue;
						va_root_attnums = lappend_int(va_root_attnums, i);
					}
					RelationClose(onerel);
				}
				{
					int		elevel = ((vacstmt->options & VACOPT_VERBOSE) ? LOG : DEBUG2);

					if (leaf_parts_analyzed(root_rel_oid, relationOid, va_root_attnums, elevel))
						/* remember to merge the partition stats into the root partition */
						oid_list = lappend_oid(oid_list, root_rel_oid);
				}
			}
			else if (ps == PART_STATUS_INTERIOR) /* analyze an interior partition directly */
			{
				/* disable analyzing mid-level partitions directly since the users are encouraged
				 * to work with the root partition only. To gather stats on mid-level partitions
				 * the user should run ANALYZE or ANALYZE ROOTPARTITION on the
				 * root partition table.
				 * Planner uses the stats on leaf partitions, so it's unnecessary to collect stats on
				 * midlevel partitions.
				 */
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- cannot analyze a mid-level partition. "
								"Please run ANALYZE on the root partition table.",
								get_rel_name(relationOid))));
			}
			else
			{
				oid_list = list_make1_oid(relationOid);
			}
			MemoryContextSwitchTo(oldcontext);
		}
	}
	else
	{
		/*
		 * Process all plain relations and materialized views listed in
		 * pg_class
		 */
		Relation	pgclass;
		HeapScanDesc scan;
		HeapTuple	tuple;
		Oid candidateOid;
		List	   *rootParts = NIL;

		pgclass = heap_open(RelationRelationId, AccessShareLock);

		scan = heap_beginscan_catalog(pgclass, 0, NULL);

		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);

			/*
			 * Don't include non-vacuum-able relations:
			 *   - Foreign tables
			 *   - etc.
			 */
			if (classForm->relkind != RELKIND_RELATION &&
				classForm->relkind != RELKIND_MATVIEW)
				continue;
			if (classForm->relstorage == RELSTORAGE_FOREIGN  ||
				classForm->relstorage == RELSTORAGE_VIRTUAL)
				continue;

			/* Make a relation list entry for this guy */
			candidateOid = HeapTupleGetOid(tuple);

			/*
			 * GPDB: Check permissions.
			 *
			 * We do a permissions check here only for VACUUM FULL because the permissions check
			 * in vacuum_rel() needs AccessExclusiveLock first. Doing it here first prevents
			 * possible blocking issues such as having a user wait for AccessExclusiveLock even
			 * though the user does not have permissions and would have skipped.
			 *
			 * Note: VACUUM ANALYZE can suffer from the same issue. So we could have ran the
			 * permissions check for VACUUM ANALYZE here as well. But we opted not to, as with
			 * ANALYZE the issue is less severe, since ANALYZE grabs a ShareUpdateExclusiveLock
			 * which is less blocking than the AccessExclusiveLock grabbed by VACUUM FULL.
			 */
			if ((vacstmt->options & VACOPT_FULL && Gp_role != GP_ROLE_EXECUTE) &&
				!(pg_class_ownercheck(candidateOid, GetUserId()) ||
					(pg_database_ownercheck(MyDatabaseId, GetUserId()) && !classForm->relisshared)))
			{
				if (classForm->relisshared)
					ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser can vacuum it",
								NameStr(classForm->relname))));
				else if (classForm->relnamespace == PG_CATALOG_NAMESPACE)
					ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser or database owner can vacuum it",
								NameStr(classForm->relname))));
				else
					ereport(WARNING,
						(errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
								NameStr(classForm->relname))));

				continue;
			}

			/* Skip non root partition tables if ANALYZE ROOTPARTITION ALL is executed */
			if ((vacstmt->options & VACOPT_ROOTONLY) && !rel_is_partitioned(candidateOid))
			{
				continue;
			}

			// skip mid-level partition tables if we have disabled collecting statistics for them
			PartStatus ps = rel_part_status(candidateOid);
			if (ps == PART_STATUS_INTERIOR)
			{
				continue;
			}


			oldcontext = MemoryContextSwitchTo(vac_context);
			if (ps == PART_STATUS_ROOT)
				rootParts = lappend_oid(rootParts, candidateOid);
			else
				oid_list = lappend_oid(oid_list, candidateOid);
			MemoryContextSwitchTo(oldcontext);
		}

		/*
		 * Schedule the root partitions to be analyzed after all the leaves.
		 * A root partition can often be analyzed by combining the HLL
		 * counters from all the leaves, which is much cheaper than scanning
		 * the whole partitioned table, but that only works if the leaves
		 * have already been analyzed.
		 */
		oldcontext = MemoryContextSwitchTo(vac_context);
		oid_list = list_concat(oid_list, rootParts);
		MemoryContextSwitchTo(oldcontext);

		heap_endscan(scan);
		heap_close(pgclass, AccessShareLock);
	}

	return oid_list;
}

/*
 * vacuum_set_xid_limits() -- compute oldest-Xmin and freeze cutoff points
 *
 * The output parameters are:
 * - oldestXmin is the cutoff value used to distinguish whether tuples are
 *	 DEAD or RECENTLY_DEAD (see HeapTupleSatisfiesVacuum).
 * - freezeLimit is the Xid below which all Xids are replaced by
 *	 FrozenTransactionId during vacuum.
 * - xidFullScanLimit (computed from table_freeze_age parameter)
 *	 represents a minimum Xid value; a table whose relfrozenxid is older than
 *	 this will have a full-table vacuum applied to it, to freeze tuples across
 *	 the whole table.  Vacuuming a table younger than this value can use a
 *	 partial scan.
 * - multiXactCutoff is the value below which all MultiXactIds are removed from
 *	 Xmax.
 * - mxactFullScanLimit is a value against which a table's relminmxid value is
 *	 compared to produce a full-table vacuum, as with xidFullScanLimit.
 *
 * xidFullScanLimit and mxactFullScanLimit can be passed as NULL if caller is
 * not interested.
 */
void
vacuum_set_xid_limits(Relation rel,
					  int freeze_min_age,
					  int freeze_table_age,
					  int multixact_freeze_min_age,
					  int multixact_freeze_table_age,
					  TransactionId *oldestXmin,
					  TransactionId *freezeLimit,
					  TransactionId *xidFullScanLimit,
					  MultiXactId *multiXactCutoff,
					  MultiXactId *mxactFullScanLimit)
{
	int			freezemin;
	int			mxid_freezemin;
	int			effective_multixact_freeze_max_age;
	TransactionId limit;
	TransactionId safeLimit;
	MultiXactId oldestMxact;
	MultiXactId mxactLimit;
	MultiXactId safeMxactLimit;

	/*
	 * We can always ignore processes running lazy vacuum.  This is because we
	 * use these values only for deciding which tuples we must keep in the
	 * tables.  Since lazy vacuum doesn't write its XID anywhere, it's safe to
	 * ignore it.  In theory it could be problematic to ignore lazy vacuums in
	 * a full vacuum, but keep in mind that only one vacuum process can be
	 * working on a particular table at any time, and that each vacuum is
	 * always an independent transaction.
	 */
	*oldestXmin = GetOldestXmin(rel, true);

	Assert(TransactionIdIsNormal(*oldestXmin));

	/*
	 * Determine the minimum freeze age to use: as specified by the caller, or
	 * vacuum_freeze_min_age, but in any case not more than half
	 * autovacuum_freeze_max_age, so that autovacuums to prevent XID
	 * wraparound won't occur too frequently.
	 */
	freezemin = freeze_min_age;
	if (freezemin < 0)
		freezemin = vacuum_freeze_min_age;
	freezemin = Min(freezemin, autovacuum_freeze_max_age / 2);
	Assert(freezemin >= 0);

	/*
	 * Compute the cutoff XID, being careful not to generate a "permanent" XID
	 */
	limit = *oldestXmin - freezemin;
	if (!TransactionIdIsNormal(limit))
		limit = FirstNormalTransactionId;

	/*
	 * If oldestXmin is very far back (in practice, more than
	 * autovacuum_freeze_max_age / 2 XIDs old), complain and force a minimum
	 * freeze age of zero.
	 */
	safeLimit = ReadNewTransactionId() - autovacuum_freeze_max_age;
	if (!TransactionIdIsNormal(safeLimit))
		safeLimit = FirstNormalTransactionId;

	if (TransactionIdPrecedes(limit, safeLimit))
	{
		ereport(WARNING,
				(errmsg("oldest xmin is far in the past"),
				 errhint("Close open transactions soon to avoid wraparound problems.")));
		limit = *oldestXmin;
	}

	*freezeLimit = limit;

	/*
	 * Compute the multixact age for which freezing is urgent.  This is
	 * normally autovacuum_multixact_freeze_max_age, but may be less if we
	 * are short of multixact member space.
	 */
	effective_multixact_freeze_max_age = MultiXactMemberFreezeThreshold();

	/*
	 * Determine the minimum multixact freeze age to use: as specified by
	 * caller, or vacuum_multixact_freeze_min_age, but in any case not more
	 * than half effective_multixact_freeze_max_age, so that autovacuums to
	 * prevent MultiXact wraparound won't occur too frequently.
	 */
	mxid_freezemin = multixact_freeze_min_age;
	if (mxid_freezemin < 0)
		mxid_freezemin = vacuum_multixact_freeze_min_age;
	mxid_freezemin = Min(mxid_freezemin,
						 effective_multixact_freeze_max_age / 2);
	Assert(mxid_freezemin >= 0);

	/* compute the cutoff multi, being careful to generate a valid value */
	oldestMxact = GetOldestMultiXactId();
	mxactLimit = oldestMxact - mxid_freezemin;
	if (mxactLimit < FirstMultiXactId)
		mxactLimit = FirstMultiXactId;

	safeMxactLimit =
		ReadNextMultiXactId() - effective_multixact_freeze_max_age;
	if (safeMxactLimit < FirstMultiXactId)
		safeMxactLimit = FirstMultiXactId;

	if (MultiXactIdPrecedes(mxactLimit, safeMxactLimit))
	{
		ereport(WARNING,
				(errmsg("oldest multixact is far in the past"),
				 errhint("Close open transactions with multixacts soon to avoid wraparound problems.")));
		/* Use the safe limit, unless an older mxact is still running */
		if (MultiXactIdPrecedes(oldestMxact, safeMxactLimit))
			mxactLimit = oldestMxact;
		else
			mxactLimit = safeMxactLimit;
	}

	*multiXactCutoff = mxactLimit;

	if (xidFullScanLimit != NULL)
	{
		int			freezetable;

		Assert(mxactFullScanLimit != NULL);

		/*
		 * Determine the table freeze age to use: as specified by the caller,
		 * or vacuum_freeze_table_age, but in any case not more than
		 * autovacuum_freeze_max_age * 0.95, so that if you have e.g nightly
		 * VACUUM schedule, the nightly VACUUM gets a chance to freeze tuples
		 * before anti-wraparound autovacuum is launched.
		 */
		freezetable = freeze_table_age;
		if (freezetable < 0)
			freezetable = vacuum_freeze_table_age;
		freezetable = Min(freezetable, autovacuum_freeze_max_age * 0.95);
		Assert(freezetable >= 0);

		/*
		 * Compute XID limit causing a full-table vacuum, being careful not to
		 * generate a "permanent" XID.
		 */
		limit = ReadNewTransactionId() - freezetable;
		if (!TransactionIdIsNormal(limit))
			limit = FirstNormalTransactionId;

		*xidFullScanLimit = limit;

		/*
		 * Similar to the above, determine the table freeze age to use for
		 * multixacts: as specified by the caller, or
		 * vacuum_multixact_freeze_table_age, but in any case not more than
		 * autovacuum_multixact_freeze_table_age * 0.95, so that if you have
		 * e.g. nightly VACUUM schedule, the nightly VACUUM gets a chance to
		 * freeze multixacts before anti-wraparound autovacuum is launched.
		 */
		freezetable = multixact_freeze_table_age;
		if (freezetable < 0)
			freezetable = vacuum_multixact_freeze_table_age;
		freezetable = Min(freezetable,
						  effective_multixact_freeze_max_age * 0.95);
		Assert(freezetable >= 0);

		/*
		 * Compute MultiXact limit causing a full-table vacuum, being careful
		 * to generate a valid MultiXact value.
		 */
		mxactLimit = ReadNextMultiXactId() - freezetable;
		if (mxactLimit < FirstMultiXactId)
			mxactLimit = FirstMultiXactId;

		*mxactFullScanLimit = mxactLimit;
	}
	else
	{
		Assert(mxactFullScanLimit == NULL);
	}
}

/*
 * vac_estimate_reltuples() -- estimate the new value for pg_class.reltuples
 *
 *		If we scanned the whole relation then we should just use the count of
 *		live tuples seen; but if we did not, we should not blindly extrapolate
 *		from that number, since VACUUM may have scanned a quite nonrandom
 *		subset of the table.  When we have only partial information, we take
 *		the old value of pg_class.reltuples as a measurement of the
 *		tuple density in the unscanned pages.
 *
 *		The is_analyze argument is historical.
 */
double
vac_estimate_reltuples(Relation relation, bool is_analyze,
					   BlockNumber total_pages,
					   BlockNumber scanned_pages,
					   double scanned_tuples)
{
	BlockNumber old_rel_pages = relation->rd_rel->relpages;
	double		old_rel_tuples = relation->rd_rel->reltuples;
	double		old_density;
	double		unscanned_pages;
	double		total_tuples;

	/* If we did scan the whole table, just use the count as-is */
	if (scanned_pages >= total_pages)
		return scanned_tuples;

	/*
	 * If scanned_pages is zero but total_pages isn't, keep the existing value
	 * of reltuples.  (Note: callers should avoid updating the pg_class
	 * statistics in this situation, since no new information has been
	 * provided.)
	 */
	if (scanned_pages == 0)
		return old_rel_tuples;

	/*
	 * If old value of relpages is zero, old density is indeterminate; we
	 * can't do much except scale up scanned_tuples to match total_pages.
	 */
	if (old_rel_pages == 0)
		return floor((scanned_tuples / scanned_pages) * total_pages + 0.5);

	/*
	 * Okay, we've covered the corner cases.  The normal calculation is to
	 * convert the old measurement to a density (tuples per page), then
	 * estimate the number of tuples in the unscanned pages using that figure,
	 * and finally add on the number of tuples in the scanned pages.
	 */
	old_density = old_rel_tuples / old_rel_pages;
	unscanned_pages = (double) total_pages - (double) scanned_pages;
	total_tuples = old_density * unscanned_pages + scanned_tuples;
	return floor(total_tuples + 0.5);
}


/*
 * Update relpages/reltuples of all the relations in the list.
 */
static void
vac_update_relstats_from_list(List *updated_stats)
{
	ListCell *lc;

	/*
	 * This function is only called in the context of the QD, so let's be
	 * explicit about that given the assumptions taken.
	 */
	Assert(Gp_role == GP_ROLE_DISPATCH);

	/*
	 * To read latest version of pg_class tuple below, as it was most likely
	 * updated earlier within same command by earlier call to
	 * vac_update_relstats().
	 */
	CommandCounterIncrement();

	foreach (lc, updated_stats)
	{
		VPgClassStats *stats = (VPgClassStats *) lfirst(lc);
		Relation	rel;

		rel = relation_open(stats->relid, AccessShareLock);

		if (GpPolicyIsReplicated(rel->rd_cdbpolicy))
		{
			stats->rel_pages = stats->rel_pages / rel->rd_cdbpolicy->numsegments;
			stats->rel_tuples = stats->rel_tuples / rel->rd_cdbpolicy->numsegments;
			stats->relallvisible = stats->relallvisible / rel->rd_cdbpolicy->numsegments;
		}

		/*
		 * Pass 'false' for isvacuum, so that the stats are
		 * actually updated.
		 */
		vac_update_relstats(rel,
							stats->rel_pages, stats->rel_tuples,
							stats->relallvisible,
							rel->rd_rel->relhasindex,
							InvalidTransactionId,
							InvalidMultiXactId,
							false,
							false /* isvacuum */);
		relation_close(rel, AccessShareLock);
	}
}

/*
 * CDB: Build a special message, to send the number of tuples
 * and the number of pages in pg_class located at QEs through
 * the dispatcher.
 */
void
vac_send_relstats_to_qd(Relation relation,
						BlockNumber num_pages,
						double num_tuples,
						BlockNumber num_all_visible_pages)
{

	StringInfoData buf;
	VPgClassStats stats;

	Oid relid = RelationGetRelid(relation);
	Assert(relid != InvalidOid);

	pq_beginmessage(&buf, 'y');
	pq_sendstring(&buf, "VACUUM");
	stats.relid = relid;
	stats.rel_pages = num_pages;
	stats.rel_tuples = num_tuples;
	stats.relallvisible = num_all_visible_pages;
	pq_sendint(&buf, sizeof(VPgClassStats), sizeof(int));
	pq_sendbytes(&buf, (char *) &stats, sizeof(VPgClassStats));
	pq_endmessage(&buf);
}

/*
 *	vac_update_relstats() -- update statistics for one relation
 *
 *		Update the whole-relation statistics that are kept in its pg_class
 *		row.  There are additional stats that will be updated if we are
 *		doing ANALYZE, but we always update these stats.  This routine works
 *		for both index and heap relation entries in pg_class.
 *
 *		We violate transaction semantics here by overwriting the rel's
 *		existing pg_class tuple with the new values.  This is reasonably
 *		safe as long as we're sure that the new values are correct whether or
 *		not this transaction commits.  The reason for doing this is that if
 *		we updated these tuples in the usual way, vacuuming pg_class itself
 *		wouldn't work very well --- by the time we got done with a vacuum
 *		cycle, most of the tuples in pg_class would've been obsoleted.  Of
 *		course, this only works for fixed-size not-null columns, but these are.
 *
 *		Another reason for doing it this way is that when we are in a lazy
 *		VACUUM and have PROC_IN_VACUUM set, we mustn't do any regular updates.
 *		Somebody vacuuming pg_class might think they could delete a tuple
 *		marked with xmin = our xid.
 *
 *		In addition to fundamentally nontransactional statistics such as
 *		relpages and relallvisible, we try to maintain certain lazily-updated
 *		DDL flags such as relhasindex, by clearing them if no longer correct.
 *		It's safe to do this in VACUUM, which can't run in parallel with
 *		CREATE INDEX/RULE/TRIGGER and can't be part of a transaction block.
 *		However, it's *not* safe to do it in an ANALYZE that's within an
 *		outer transaction, because for example the current transaction might
 *		have dropped the last index; then we'd think relhasindex should be
 *		cleared, but if the transaction later rolls back this would be wrong.
 *		So we refrain from updating the DDL flags if we're inside an outer
 *		transaction.  This is OK since postponing the flag maintenance is
 *		always allowable.
 *
 *		This routine is shared by VACUUM and ANALYZE.
 */
void
vac_update_relstats(Relation relation,
					BlockNumber num_pages, double num_tuples,
					BlockNumber num_all_visible_pages,
					bool hasindex, TransactionId frozenxid,
					MultiXactId minmulti,
					bool in_outer_xact,
					bool isvacuum)
{
	Oid			relid = RelationGetRelid(relation);
	Relation	rd;
	HeapTuple	ctup;
	Form_pg_class pgcform;
	bool		dirty;

	Assert(relid != InvalidOid);

	/*
	 * In GPDB, all the data is stored in the segments, and the
	 * relpages/reltuples in the master reflect the sum of the values in
	 * all the segments. In VACUUM, don't overwrite relpages/reltuples with
	 * the values we counted in the QD node itself. We will dispatch the
	 * VACUUM to the segments after processing the QD node, and we will
	 * update relpages/reltuples then.
	 *
	 * Update stats for system tables normally, though (it'd better say
	 * "non-distributed" tables than system relations here, but for now
	 * it's effectively the same.)
	 */
	if (!IsSystemRelation(relation) && isvacuum)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			num_pages = relation->rd_rel->relpages;
			num_tuples = relation->rd_rel->reltuples;
			num_all_visible_pages = relation->rd_rel->relallvisible;
		}
		else if (Gp_role == GP_ROLE_EXECUTE)
		{
			vac_send_relstats_to_qd(relation,
									num_pages,
									num_tuples,
									num_all_visible_pages);
		}
	}

	/*
	 * We need a way to distinguish these 2 cases:
	 * a) ANALYZEd/VACUUMed table is empty
	 * b) Table has never been ANALYZEd/VACUUMed
	 * To do this, in case (a), we set relPages = 1. For case (b), relPages = 0.
	 */
	if (num_pages < 1.0)
	{
		Assert(num_tuples < 1.0);
		num_pages = 1.0;
	}

	/*
	 * update number of tuples and number of pages in pg_class
	 */
	rd = heap_open(RelationRelationId, RowExclusiveLock);

	/* Fetch a copy of the tuple to scribble on */
	ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for relid %u vanished during vacuuming",
			 relid);
	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	/* Apply statistical updates, if any, to copied tuple */

	/* GPDB-specific not allow change relpages and reltuples when vacuum in utility mode on QD
	 * Because there's a chance that we overwrite perfectly good stats with zeros
	 */

	bool ifUpdate = ! (IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_UTILITY);

	dirty = false;
	if (pgcform->relpages != (int32) num_pages && ifUpdate)
	{
		pgcform->relpages = (int32) num_pages;
		dirty = true;
	}
	if (pgcform->reltuples != (float4) num_tuples && ifUpdate)
	{
		pgcform->reltuples = (float4) num_tuples;
		dirty = true;
	}
	if (pgcform->relallvisible != (int32) num_all_visible_pages)
	{
		pgcform->relallvisible = (int32) num_all_visible_pages;
		dirty = true;
	}

	elog(DEBUG2, "Vacuum oid=%u pages=%d tuples=%f",
		 relid, pgcform->relpages, pgcform->reltuples);
	/*
	 * If we have discovered that there are no indexes, then there's no
	 * primary key either.  This could be done more thoroughly...
	 */
	if (pgcform->relhaspkey && !hasindex)
	{
		pgcform->relhaspkey = false;
		dirty = true;
	}

	if (!in_outer_xact)
	{
		/*
		 * If we didn't find any indexes, reset relhasindex.
		 */
		if (pgcform->relhasindex && !hasindex)
		{
			pgcform->relhasindex = false;
			dirty = true;
		}

		/*
		 * If we have discovered that there are no indexes, then there's no
		 * primary key either.  This could be done more thoroughly...
		 */
		if (pgcform->relhaspkey && !hasindex)
		{
			pgcform->relhaspkey = false;
			dirty = true;
		}

		/* We also clear relhasrules and relhastriggers if needed */
		if (pgcform->relhasrules && relation->rd_rules == NULL)
		{
			pgcform->relhasrules = false;
			dirty = true;
		}
		if (pgcform->relhastriggers && relation->trigdesc == NULL)
		{
			pgcform->relhastriggers = false;
			dirty = true;
		}
	}

	/*
	 * Update relfrozenxid, unless caller passed InvalidTransactionId
	 * indicating it has no new data.
	 *
	 * Ordinarily, we don't let relfrozenxid go backwards: if things are
	 * working correctly, the only way the new frozenxid could be older would
	 * be if a previous VACUUM was done with a tighter freeze_min_age, in
	 * which case we don't want to forget the work it already did.  However,
	 * if the stored relfrozenxid is "in the future", then it must be corrupt
	 * and it seems best to overwrite it with the cutoff we used this time.
	 * This should match vac_update_datfrozenxid() concerning what we consider
	 * to be "in the future".
	 */
	if (TransactionIdIsNormal(frozenxid) &&
		TransactionIdIsValid(pgcform->relfrozenxid) &&
		pgcform->relfrozenxid != frozenxid &&
		(TransactionIdPrecedes(pgcform->relfrozenxid, frozenxid) ||
		 TransactionIdPrecedes(ReadNewTransactionId(),
							   pgcform->relfrozenxid)))
	{
		pgcform->relfrozenxid = frozenxid;
		dirty = true;
	}

	/* Similarly for relminmxid */
	if (MultiXactIdIsValid(minmulti) &&
		pgcform->relminmxid != minmulti &&
		(MultiXactIdPrecedes(pgcform->relminmxid, minmulti) ||
		 MultiXactIdPrecedes(ReadNextMultiXactId(), pgcform->relminmxid)))
	{
		pgcform->relminmxid = minmulti;
		dirty = true;
	}

	/* If anything changed, write out the tuple. */
	if (dirty)
		heap_inplace_update(rd, ctup);

	heap_close(rd, RowExclusiveLock);
}


/*
 *	vac_update_datfrozenxid() -- update pg_database.datfrozenxid for our DB
 *
 *		Update pg_database's datfrozenxid entry for our database to be the
 *		minimum of the pg_class.relfrozenxid values.
 *
 *		Similarly, update our datminmxid to be the minimum of the
 *		pg_class.relminmxid values.
 *
 *		If we are able to advance either pg_database value, also try to
 *		truncate pg_clog and pg_multixact.
 *
 *		We violate transaction semantics here by overwriting the database's
 *		existing pg_database tuple with the new values.  This is reasonably
 *		safe since the new values are correct whether or not this transaction
 *		commits.  As with vac_update_relstats, this avoids leaving dead tuples
 *		behind after a VACUUM.
 */
void
vac_update_datfrozenxid(void)
{
	HeapTuple	tuple;
	Form_pg_database dbform;
	Relation	relation;
	SysScanDesc scan;
	HeapTuple	classTup;
	TransactionId newFrozenXid;
	MultiXactId newMinMulti;
	TransactionId lastSaneFrozenXid;
	MultiXactId lastSaneMinMulti;
	bool		bogus = false;
	bool		dirty = false;

	/*
	 * Initialize the "min" calculation with GetOldestXmin, which is a
	 * reasonable approximation to the minimum relfrozenxid for not-yet-
	 * committed pg_class entries for new tables; see AddNewRelationTuple().
	 * So we cannot produce a wrong minimum by starting with this.
	 *
	 * GPDB: Use GetLocalOldestXmin here, rather than GetOldestXmin. We don't
	 * want to include effects of distributed transactions in this. If a
	 * database's datfrozenxid is past the oldest XID as determined by
	 * distributed transactions, we will nevertheless never encounter such
	 * XIDs on disk.
	 */
	newFrozenXid = GetLocalOldestXmin(NULL, true);

	/*
	 * Similarly, initialize the MultiXact "min" with the value that would be
	 * used on pg_class for new tables.  See AddNewRelationTuple().
	 */
	newMinMulti = GetOldestMultiXactId();

	/*
	 * Identify the latest relfrozenxid and relminmxid values that we could
	 * validly see during the scan.  These are conservative values, but it's
	 * not really worth trying to be more exact.
	 */
	lastSaneFrozenXid = ReadNewTransactionId();
	lastSaneMinMulti = ReadNextMultiXactId();

	/*
	 * We must seqscan pg_class to find the minimum Xid, because there is no
	 * index that can help us here.
	 */
	relation = heap_open(RelationRelationId, AccessShareLock);

	scan = systable_beginscan(relation, InvalidOid, false,
							  NULL, 0, NULL);

	while ((classTup = systable_getnext(scan)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTup);

#if 0
		/*
		 * Only consider relations able to hold unfrozen XIDs (anything else
		 * should have InvalidTransactionId in relfrozenxid anyway.)
		 */
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW &&
			classForm->relkind != RELKIND_TOASTVALUE)
			continue;
#endif

		/* GPDB_94_MERGE_FIXME: We have had this check here, instead of the above
		 * check that upstream has. I would be more comfortable if we would list
		 * the relkinds here explicitly, like in upstream..
		 */
		if (!TransactionIdIsValid(classForm->relfrozenxid))
			continue;

		Assert(TransactionIdIsNormal(classForm->relfrozenxid));
		Assert(MultiXactIdIsValid(classForm->relminmxid));
		/*
		 * Don't know partition parent or not here but passing false is perfect
		 * for assertion, as valid relfrozenxid means it shouldn't be parent.
		 */
		Assert(should_have_valid_relfrozenxid(classForm->relkind,
											  classForm->relstorage, false));

		/*
		 * If things are working properly, no relation should have a
		 * relfrozenxid or relminmxid that is "in the future".  However, such
		 * cases have been known to arise due to bugs in pg_upgrade.  If we
		 * see any entries that are "in the future", chicken out and don't do
		 * anything.  This ensures we won't truncate clog before those
		 * relations have been scanned and cleaned up.
		 */
		if (TransactionIdPrecedes(lastSaneFrozenXid, classForm->relfrozenxid) ||
			MultiXactIdPrecedes(lastSaneMinMulti, classForm->relminmxid))
		{
			bogus = true;
			break;
		}

		if (TransactionIdPrecedes(classForm->relfrozenxid, newFrozenXid))
			newFrozenXid = classForm->relfrozenxid;

		if (MultiXactIdPrecedes(classForm->relminmxid, newMinMulti))
			newMinMulti = classForm->relminmxid;
	}

	/* we're done with pg_class */
	systable_endscan(scan);
	heap_close(relation, AccessShareLock);

	/* chicken out if bogus data found */
	if (bogus)
		return;

	Assert(TransactionIdIsNormal(newFrozenXid));
	Assert(MultiXactIdIsValid(newMinMulti));

	/* Now fetch the pg_database tuple we need to update. */
	relation = heap_open(DatabaseRelationId, RowExclusiveLock);

	/* Fetch a copy of the tuple to scribble on */
	tuple = SearchSysCacheCopy1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for database %u", MyDatabaseId);
	dbform = (Form_pg_database) GETSTRUCT(tuple);

	/*
	 * As in vac_update_relstats(), we ordinarily don't want to let
	 * datfrozenxid go backward; but if it's "in the future" then it must be
	 * corrupt and it seems best to overwrite it.
	 */
	if (dbform->datfrozenxid != newFrozenXid &&
		(TransactionIdPrecedes(dbform->datfrozenxid, newFrozenXid) ||
		 TransactionIdPrecedes(lastSaneFrozenXid, dbform->datfrozenxid)))
	{
		dbform->datfrozenxid = newFrozenXid;
		dirty = true;
	}
	else
		newFrozenXid = dbform->datfrozenxid;

	/* Ditto for datminmxid */
	if (dbform->datminmxid != newMinMulti &&
		(MultiXactIdPrecedes(dbform->datminmxid, newMinMulti) ||
		 MultiXactIdPrecedes(lastSaneMinMulti, dbform->datminmxid)))
	{
		dbform->datminmxid = newMinMulti;
		dirty = true;
	}
	else
		newMinMulti = dbform->datminmxid;

	if (dirty)
	{
		heap_inplace_update(relation, tuple);
		SIMPLE_FAULT_INJECTOR("vacuum_update_dat_frozen_xid");
	}

	heap_freetuple(tuple);
	heap_close(relation, RowExclusiveLock);

	/*
	 * If we were able to advance datfrozenxid or datminmxid, see if we can
	 * truncate pg_clog and/or pg_multixact.  Also do it if the shared
	 * XID-wrap-limit info is stale, since this action will update that too.
	 */
	if (dirty || ForceTransactionIdLimitUpdate())
		vac_truncate_clog(newFrozenXid, newMinMulti,
						  lastSaneFrozenXid, lastSaneMinMulti);
}


/*
 *	vac_truncate_clog() -- attempt to truncate the commit log
 *
 *		Scan pg_database to determine the system-wide oldest datfrozenxid,
 *		and use it to truncate the transaction commit log (pg_clog).
 *		Also update the XID wrap limit info maintained by varsup.c.
 *		Likewise for datminmxid.
 *
 *		The passed frozenXID and minMulti are the updated values for my own
 *		pg_database entry. They're used to initialize the "min" calculations.
 *		The caller also passes the "last sane" XID and MXID, since it has
 *		those at hand already.
 *
 *		This routine is only invoked when we've managed to change our
 *		DB's datfrozenxid/datminmxid values, or we found that the shared
 *		XID-wrap-limit info is stale.
 */
static void
vac_truncate_clog(TransactionId frozenXID,
				  MultiXactId minMulti,
				  TransactionId lastSaneFrozenXid,
				  MultiXactId lastSaneMinMulti)
{
	TransactionId nextXID = ReadNewTransactionId();
	Relation	relation;
	HeapScanDesc scan;
	HeapTuple	tuple;
	Oid			oldestxid_datoid;
	Oid			minmulti_datoid;
	bool		bogus = false;
	bool		frozenAlreadyWrapped = false;

	/* init oldest datoids to sync with my frozenXID/minMulti values */
	oldestxid_datoid = MyDatabaseId;
	minmulti_datoid = MyDatabaseId;

	/*
	 * Scan pg_database to compute the minimum datfrozenxid/datminmxid
	 *
	 * Since vac_update_datfrozenxid updates datfrozenxid/datminmxid in-place,
	 * the values could change while we look at them.  Fetch each one just
	 * once to ensure sane behavior of the comparison logic.  (Here, as in
	 * many other places, we assume that fetching or updating an XID in shared
	 * storage is atomic.)
	 *
	 * Note: we need not worry about a race condition with new entries being
	 * inserted by CREATE DATABASE.  Any such entry will have a copy of some
	 * existing DB's datfrozenxid, and that source DB cannot be ours because
	 * of the interlock against copying a DB containing an active backend.
	 * Hence the new entry will not reduce the minimum.  Also, if two VACUUMs
	 * concurrently modify the datfrozenxid's of different databases, the
	 * worst possible outcome is that pg_clog is not truncated as aggressively
	 * as it could be.
	 */
	relation = heap_open(DatabaseRelationId, AccessShareLock);

	scan = heap_beginscan_catalog(relation, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		volatile FormData_pg_database *dbform = (Form_pg_database) GETSTRUCT(tuple);
		TransactionId datfrozenxid = dbform->datfrozenxid;
		TransactionId datminmxid = dbform->datminmxid;

		Assert(TransactionIdIsNormal(datfrozenxid));
		Assert(MultiXactIdIsValid(datminmxid));

		/*
		 * If things are working properly, no database should have a
		 * datfrozenxid or datminmxid that is "in the future".  However, such
		 * cases have been known to arise due to bugs in pg_upgrade.  If we
		 * see any entries that are "in the future", chicken out and don't do
		 * anything.  This ensures we won't truncate clog before those
		 * databases have been scanned and cleaned up.  (We will issue the
		 * "already wrapped" warning if appropriate, though.)
		 */
		if (TransactionIdPrecedes(lastSaneFrozenXid, datfrozenxid) ||
			MultiXactIdPrecedes(lastSaneMinMulti, datminmxid))
			bogus = true;

		if (TransactionIdPrecedes(nextXID, datfrozenxid))
			frozenAlreadyWrapped = true;
		else if (TransactionIdPrecedes(datfrozenxid, frozenXID))
		{
			frozenXID = datfrozenxid;
			oldestxid_datoid = HeapTupleGetOid(tuple);
		}

		if (MultiXactIdPrecedes(datminmxid, minMulti))
		{
			minMulti = datminmxid;
			minmulti_datoid = HeapTupleGetOid(tuple);
		}
	}

	heap_endscan(scan);

	heap_close(relation, AccessShareLock);

	/*
	 * Do not truncate CLOG if we seem to have suffered wraparound already;
	 * the computed minimum XID might be bogus.  This case should now be
	 * impossible due to the defenses in GetNewTransactionId, but we keep the
	 * test anyway.
	 */
	if (frozenAlreadyWrapped)
	{
		ereport(WARNING,
				(errmsg("some databases have not been vacuumed in over 2 billion transactions"),
				 errdetail("You might have already suffered transaction-wraparound data loss.")));
		return;
	}

	/* chicken out if data is bogus in any other way */
	if (bogus)
		return;

	/*
	 * Truncate CLOG to the oldest computed value.  Note we don't truncate
	 * multixacts; that will be done by the next checkpoint.
	 */
	TruncateCLOG(frozenXID);

	/*
	 * Update the wrap limit for GetNewTransactionId and creation of new
	 * MultiXactIds.  Note: these functions will also signal the postmaster
	 * for an(other) autovac cycle if needed.   XXX should we avoid possibly
	 * signalling twice?
	 */
	SetTransactionIdLimit(frozenXID, oldestxid_datoid);
	SetMultiXactIdLimit(minMulti, minmulti_datoid);
}

/*
 *	vacuum_rel() -- vacuum one heap relation
 *
 *		Doing one heap at a time incurs extra overhead, since we need to
 *		check that the heap exists again just before we vacuum it.  The
 *		reason that we do this is so that vacuuming can be spread across
 *		many small transactions.  Otherwise, two-phase locking would require
 *		us to lock the entire database during one pass of the vacuum cleaner.
 *
 * GPDB: On entry, we should already hold a session-level lock on the table.
 * If 'onerel' is valid, then we should also hold an appropriate regular lock on
 * the table, and have a transaction open.
 * On exit, the 'onerel' will be closed, and the transaction is closed.
 */
static bool
vacuum_rel(Relation onerel, Oid relid, VacuumStmt *vacstmt, LOCKMODE lmode,
		   bool for_wraparound)
{
	Oid			toast_relid;
	RangeVar	*toast_rangevar = NULL;
	bool		is_heap;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	MemoryContext oldcontext;
	bool		dispatch;

	if (Gp_role == GP_ROLE_DISPATCH && onerel)
		dispatch = true;
	else
		dispatch = false;

	if (!onerel)
	{

		if (!(vacstmt->options & VACOPT_FULL))
		{
			/*
			 * PostgreSQL does this:
			 * During a lazy VACUUM we can set the PROC_IN_VACUUM flag, which lets other
			 * concurrent VACUUMs know that they can ignore this one while
			 * determining their OldestXmin.  (The reason we don't set it during a
			 * full VACUUM is exactly that we may have to run user- defined
			 * functions for functional indexes, and we want to make sure that if
			 * they use the snapshot set above, any tuples it requires can't get
			 * removed from other tables.  An index function that depends on the
			 * contents of other tables is arguably broken, but we won't break it
			 * here by violating transaction semantics.)
			 *
			 * GPDB doesn't use PROC_IN_VACUUM, as lazy vacuum for bitmap
			 * indexed tables performs reindex causing updates to pg_class
			 * tuples for index entries.
			 *
			 * We also set the VACUUM_FOR_WRAPAROUND flag, which is passed down
			 * by autovacuum; it's used to avoid cancelling a vacuum that was
			 * invoked in an emergency.
			 *
			 * Note: this flag remains set until CommitTransaction or
			 * AbortTransaction.  We don't want to clear it until we reset
			 * MyProc->xid/xmin, else OldestXmin might appear to go backwards,
			 * which is probably Not Good.
			 */
			LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
#if 0 /* Upstream code not applicable to GPDB */
			MyProc->vacuumFlags |= PROC_IN_VACUUM;
#endif
			if (for_wraparound)
				MyPgXact->vacuumFlags |= PROC_VACUUM_FOR_WRAPAROUND;
			LWLockRelease(ProcArrayLock);
		}

		/*
		 * Check for user-requested abort.  Note we want this to be inside a
		 * transaction, so xact.c doesn't issue useless WARNING.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Open the relation and get the appropriate lock on it.
		 *
		 * There's a race condition here: the rel may have gone away since the
		 * last time we saw it.  If so, we don't need to vacuum it.
		 *
		 * If we've been asked not to wait for the relation lock, acquire it first
		 * in non-blocking mode, before calling try_relation_open().
		 */
		if (!(vacstmt->options & VACOPT_NOWAIT))
			onerel = try_relation_open(relid, lmode, false /* nowait */);
		else if (ConditionalLockRelationOid(relid, lmode))
			onerel = try_relation_open(relid, NoLock, false /* nowait */);
		else
		{
			onerel = NULL;
			if (IsAutoVacuumWorkerProcess() && Log_autovacuum_min_duration >= 0)
				ereport(LOG,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("skipping vacuum of \"%s\" --- lock not available",
								vacstmt->relation->relname)));
		}

		if (!onerel)
		{
			return false;
		}
	}

	/*
	 * Remember the relation's TOAST relation for later
	 */
	toast_relid = onerel->rd_rel->reltoastrelid;
	is_heap = RelationIsHeap(onerel);
	oldcontext = MemoryContextSwitchTo(vac_context);
	toast_rangevar = makeRangeVar(get_namespace_name(get_rel_namespace(toast_relid)),
								  get_rel_name(toast_relid),
								  -1);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(onerel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * If we are in the dispatch mode, dispatch this modified
	 * vacuum statement to QEs, and wait for them to finish.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		int 		i, nindexes;
		bool 		has_bitmap = false;
		Relation   *i_rel = NULL;

		vac_open_indexes(onerel, AccessShareLock, &nindexes, &i_rel);
		if (i_rel != NULL)
		{
			for (i = 0; i < nindexes; i++)
			{
				if (RelationIsBitmapIndex(i_rel[i]))
				{
					has_bitmap = true;
					break;
				}
			}
		}
		vac_close_indexes(nindexes, i_rel, AccessShareLock);

		/*
		 * We have to acquire a ShareLock for the relation which has bitmap
		 * indexes, since reindex is used later. Otherwise, concurrent
		 * vacuum and inserts may cause deadlock. MPP-5960
		 */
		if (has_bitmap)
			LockRelation(onerel, ShareLock);
	}

	/*
	 * Do the actual work --- either FULL or "lazy" vacuum
	 */
	if (is_heap && (vacstmt->options & VACOPT_FULL))
	{
		Oid			relid = RelationGetRelid(onerel);

		/* close relation before vacuuming, but hold lock until commit */
		relation_close(onerel, NoLock);
		onerel = NULL;

		/* VACUUM FULL is now a variant of CLUSTER; see cluster.c */
		cluster_rel(relid, InvalidOid, false,
					(vacstmt->options & VACOPT_VERBOSE) != 0,
					true /* printError */);
	}
	else
	{
		lazy_vacuum_rel(onerel, vacstmt, vac_strategy);
	}

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/*
	 * If the relation has a secondary toast rel, vacuum that too while we
	 * still hold the session lock on the master table.
	 */
	if (is_heap)
	{
		if (toast_relid != InvalidOid && toast_rangevar != NULL)
		{
			VacuumStmt *vacstmt_toast = makeNode(VacuumStmt);
			vacstmt_toast->options = vacstmt->options;
			vacstmt_toast->freeze_min_age = vacstmt->freeze_min_age;
			vacstmt_toast->freeze_table_age = vacstmt->freeze_table_age;
			vacstmt_toast->skip_twophase = vacstmt->skip_twophase;

			vacstmt_toast->relation = toast_rangevar;
			vacuum_rel(NULL, toast_relid, vacstmt_toast, lmode, for_wraparound);
		}
	}

	if (dispatch)
	{
		VacuumStatsContext stats_context;

		stats_context.updated_stats = NIL;
		GetUserIdAndSecContext(&save_userid, &save_sec_context);

		SetUserIdAndSecContext(
				save_userid,
				save_sec_context | SECURITY_RESTRICTED_OPERATION);

		dispatchVacuum(vacstmt, &stats_context);
		vac_update_relstats_from_list(stats_context.updated_stats);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);
	}


	/* all done with this class, but hold lock until commit */
	if (onerel)
		relation_close(onerel, NoLock);

	/* Report that we really did it. */
	return true;
}


/****************************************************************************
 *																			*
 *			Code for VACUUM FULL (only)										*
 *																			*
 ****************************************************************************
 */


/* GPDB_91_MERGE_FIXME: 'amindexnulls' is gone. Do we need this function anymore? */
#if 0
/*
 * Is an index partial (ie, could it contain fewer tuples than the heap?)
 */
static bool
vac_is_partial_index(Relation indrel)
{
	/*
	 * If the index's AM doesn't support nulls, it's partial for our purposes
	 */
	if (!indrel->rd_am->amindexnulls)
		return true;

	/* Otherwise, look to see if there's a partial-index predicate */
	if (!heap_attisnull(indrel->rd_indextuple, Anum_pg_index_indpred))
		return true;

	return false;
}
#endif

/*
 * Open all the vacuumable indexes of the given relation, obtaining the
 * specified kind of lock on each.  Return an array of Relation pointers for
 * the indexes into *Irel, and the number of indexes into *nindexes.
 *
 * We consider an index vacuumable if it is marked insertable (IndexIsReady).
 * If it isn't, probably a CREATE INDEX CONCURRENTLY command failed early in
 * execution, and what we have is too corrupt to be processable.  We will
 * vacuum even if the index isn't indisvalid; this is important because in a
 * unique index, uniqueness checks will be performed anyway and had better not
 * hit dangling index pointers.
 */
void
vac_open_indexes(Relation relation, LOCKMODE lockmode,
				 int *nindexes, Relation **Irel)
{
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int			i;

	Assert(lockmode != NoLock);

	indexoidlist = RelationGetIndexList(relation);

	/* allocate enough memory for all indexes */
	i = list_length(indexoidlist);

	if (i > 0)
		*Irel = (Relation *) palloc(i * sizeof(Relation));
	else
		*Irel = NULL;

	/* collect just the ready indexes */
	i = 0;
	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indrel;

		indrel = index_open(indexoid, lockmode);
		if (IndexIsReady(indrel->rd_index))
			(*Irel)[i++] = indrel;
		else
			index_close(indrel, lockmode);
	}

	*nindexes = i;

	list_free(indexoidlist);
}

/*
 * Release the resources acquired by vac_open_indexes.  Optionally release
 * the locks (say NoLock to keep 'em).
 */
void
vac_close_indexes(int nindexes, Relation *Irel, LOCKMODE lockmode)
{
	if (Irel == NULL)
		return;

	while (nindexes--)
	{
		Relation	ind = Irel[nindexes];

		index_close(ind, lockmode);
	}
	pfree(Irel);
}

/*
 * vacuum_delay_point --- check for interrupts and cost-based delay.
 *
 * This should be called in each major loop of VACUUM processing,
 * typically once per page processed.
 */
void
vacuum_delay_point(void)
{
	/* Always check for interrupts */
	CHECK_FOR_INTERRUPTS();

	/* Nap if appropriate */
	if (VacuumCostActive && !InterruptPending &&
		VacuumCostBalance >= VacuumCostLimit)
	{
		int			msec;

		msec = VacuumCostDelay * VacuumCostBalance / VacuumCostLimit;
		if (msec > VacuumCostDelay * 4)
			msec = VacuumCostDelay * 4;

		pg_usleep(msec * 1000L);

		VacuumCostBalance = 0;

		/* update balance values for workers */
		AutoVacuumUpdateDelay();

		/* Might have gotten an interrupt while sleeping */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Dispatch a Vacuum command.
 */
static void
dispatchVacuum(VacuumStmt *vacstmt, VacuumStatsContext *ctx)
{
	CdbPgResults cdb_pgresults;

	int flags = DF_CANCEL_ON_ERROR | DF_WITH_SNAPSHOT;

	/* should these be marked volatile ? */

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(vacstmt);
	Assert(vacstmt->options & VACOPT_VACUUM);
	Assert(!(vacstmt->options & VACOPT_ANALYZE));

	if (!vacstmt->skip_twophase)
		flags |= DF_NEED_TWO_PHASE;

	/* XXX: Some kinds of VACUUM assign a new relfilenode. bitmap indexes maybe? */
	CdbDispatchUtilityStatement((Node *) vacstmt, flags,
								GetAssignedOidsForDispatch(),
								&cdb_pgresults);

	vacuum_combine_stats(ctx, &cdb_pgresults);

	cdbdisp_clearCdbPgResults(&cdb_pgresults);
}

/*
 * vacuum_combine_stats
 * This function combine the stats information sent by QEs to generate
 * the final stats for QD relations.
 *
 * Note that the mirrorResults is ignored by this function.
 */
static void
vacuum_combine_stats(VacuumStatsContext *stats_context, CdbPgResults* cdb_pgresults)
{
	int result_no;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	if (cdb_pgresults == NULL || cdb_pgresults->numResults <= 0)
		return;

	/*
	 * Process the dispatch results from the primary. Note that the QE
	 * processes also send back the new stats info, such as stats on
	 * pg_class, for the relevant table and its
	 * indexes. We parse this information, and compute the final stats
	 * for the QD.
	 *
	 * For pg_class stats, we compute the maximum number of tuples and
	 * maximum number of pages after processing the stats from each QE.
	 *
	 */
	for(result_no = 0; result_no < cdb_pgresults->numResults; result_no++)
	{

		VPgClassStats *pgclass_stats = NULL;
		ListCell *lc = NULL;
		struct pg_result *pgresult = cdb_pgresults->pg_results[result_no];

		if (pgresult->extras == NULL)
			continue;

		Assert(pgresult->extraslen > sizeof(int));

		/*
		 * Process the stats for pg_class. We simply compute the maximum
		 * number of rel_tuples and rel_pages.
		 */
		pgclass_stats = (VPgClassStats *) pgresult->extras;
		foreach (lc, stats_context->updated_stats)
		{
			VPgClassStats *tmp_stats = (VPgClassStats *) lfirst(lc);

			if (tmp_stats->relid == pgclass_stats->relid)
			{
				tmp_stats->rel_pages += pgclass_stats->rel_pages;
				tmp_stats->rel_tuples += pgclass_stats->rel_tuples;
				tmp_stats->relallvisible += pgclass_stats->relallvisible;
				break;
			}
		}

		if (lc == NULL)
		{
			Assert(pgresult->extraslen == sizeof(VPgClassStats));

			pgclass_stats = palloc(sizeof(VPgClassStats));
			memcpy(pgclass_stats, pgresult->extras, pgresult->extraslen);

			stats_context->updated_stats =
					lappend(stats_context->updated_stats, pgclass_stats);
		}
	}
}
