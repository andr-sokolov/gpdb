/*--------------------------------------------------------------------
 * guc_gp.c
 *
 * Additional Greenplum-specific GUCs are defined in this file, to
 * avoid adding so much stuff to guc.c. This makes it easier to diff
 * and merge with upstream.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Copyright (c) 2000-2009, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc_gp.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <sys/unistd.h>

#include "access/transam.h"
#include "access/xlog_internal.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "libpq/password_hash.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "pgstat.h"
#include "parser/scansup.h"
#include "postmaster/syslogger.h"
#include "postmaster/fts.h"
#include "replication/walsender.h"
#include "storage/buffile.h"
#include "storage/proc.h"
#include "tcop/idle_resource_cleaner.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"
#include "utils/inval.h"
#include "utils/resscheduler.h"
#include "utils/resgroup.h"
#include "utils/resource_manager.h"
#include "utils/vmem_tracker.h"
#include "utils/gdd.h"

/*
 * These constants are copied from guc.c. They should not bitrot when we
 * merge guc.c with upstream, as these are natural constants that never
 * change. guc.c might acquire more of these, though. In that case, we'll
 * just copy the new ones too, as needed.
 */
#define KB_PER_MB (1024)
#define KB_PER_GB (1024*1024)

#define MS_PER_S 1000
#define S_PER_MIN 60
#define MS_PER_MIN (1000 * 60)
#define MIN_PER_H 60
#define S_PER_H (60 * 60)
#define MS_PER_H (1000 * 60 * 60)
#define MIN_PER_D (60 * 24)
#define S_PER_D (60 * 60 * 24)
#define MS_PER_D (1000 * 60 * 60 * 24)

/*
 * Assign/Show hook functions defined in this module
 */
static bool check_dispatch_log_stats(bool *newval, void **extra, GucSource source);
static bool check_gp_hashagg_default_nbatches(int *newval, void **extra, GucSource source);
static bool check_gp_workfile_compression(bool *newval, void **extra, GucSource source);

/* Helper function for guc setter */
bool gpvars_check_gp_resqueue_priority_default_value(char **newval,
													void **extra,
													GucSource source);

static bool check_pljava_classpath_insecure(bool *newval, void **extra, GucSource source);
static void assign_pljava_classpath_insecure(bool newval, void *extra);
static bool check_gp_resource_group_bypass(bool *newval, void **extra, GucSource source);
static int guc_array_compare(const void *a, const void *b);

extern struct config_generic *find_option(const char *name, bool create_placeholders, int elevel);

extern int listenerBacklog;

/* GUC lists for gp_guc_list_show().  (List of struct config_generic) */
List	   *gp_guc_list_for_explain;
List	   *gp_guc_list_for_no_plan;

/* For synchornized GUC value is cache in HashTable,
 * dispatch value along with query when some guc changed
 */
List       *gp_guc_restore_list = NIL;
bool        gp_guc_need_restore = false;

char	   *Debug_dtm_action_sql_command_tag;

bool		dev_opt_unsafe_truncate_in_subtransaction = false;
bool		Debug_print_full_dtm = false;
bool		Debug_print_snapshot_dtm = false;
bool		Debug_disable_distributed_snapshot = false;
bool		Debug_abort_after_distributed_prepared = false;
bool		Debug_bitmap_print_insert = false;
bool		Test_print_direct_dispatch_info = false;
bool        Test_print_prefetch_joinqual = false;
bool		Test_copy_qd_qe_split = false;
bool		gp_permit_relation_node_change = false;
int			gp_max_local_distributed_cache = 1024;
bool		gp_heap_require_relhasoids_match = true;
bool		gp_local_distributed_cache_stats = false;
bool		debug_xlog_record_read = false;
bool		Debug_cancel_print = false;
bool		gp_create_table_random_default_distribution = true;
bool		gp_allow_non_uniform_partitioning_ddl = true;
bool		gp_enable_exchange_default_partition = false;
int			dtx_phase2_retry_count = 0;

bool		log_dispatch_stats = false;

int			explain_memory_verbosity = 0;
char	   *memory_profiler_run_id = "none";
char	   *memory_profiler_dataset_id = "none";
char	   *memory_profiler_query_id = "none";
int			memory_profiler_dataset_size = 0;

/* WAL based replication debug GUCs */
bool		debug_walrepl_snd = false;
bool		debug_walrepl_syncrep = false;
bool		debug_walrepl_rcv = false;
bool		debug_basebackup = false;

int rep_lag_avoidance_threshold = 0;

/* Latch mechanism debug GUCs */
bool		debug_latch = false;

bool		gp_keep_all_xlog = false;

#define DEBUG_DTM_ACTION_PRIMARY_DEFAULT true
bool		Debug_dtm_action_primary = DEBUG_DTM_ACTION_PRIMARY_DEFAULT;

bool		gp_log_optimization_time = false;

int			Debug_dtm_action = DEBUG_DTM_ACTION_NONE;

#define DEBUG_DTM_ACTION_TARGET_DEFAULT DEBUG_DTM_ACTION_TARGET_NONE

int			Debug_dtm_action_target = DEBUG_DTM_ACTION_TARGET_DEFAULT;

#define DEBUG_DTM_ACTION_PROTOCOL_DEFAULT DTX_PROTOCOL_COMMAND_COMMIT_PREPARED

int			Debug_dtm_action_protocol = DEBUG_DTM_ACTION_PROTOCOL_DEFAULT;

#define DEBUG_DTM_ACTION_SEGMENT_DEFAULT -2
#define DEBUG_DTM_ACTION_NESTINGLEVEL_DEFAULT 0

int			Debug_dtm_action_segment = DEBUG_DTM_ACTION_SEGMENT_DEFAULT;
int			Debug_dtm_action_nestinglevel = DEBUG_DTM_ACTION_NESTINGLEVEL_DEFAULT;

int			gp_connection_send_timeout;

bool create_restartpoint_on_ckpt_record_replay = false;

/*
 * This variable is a dummy that doesn't do anything, except in some
 * cases provide the value for SHOW to display.  The real state is elsewhere
 * and is kept in sync by assign_hooks.
 */
static char *gp_resource_manager_str;

/* Backoff-related GUCs */
bool		gp_enable_resqueue_priority;
int			gp_resqueue_priority_local_interval;
int			gp_resqueue_priority_sweeper_interval;
int			gp_resqueue_priority_inactivity_timeout;
int			gp_resqueue_priority_grouping_timeout;
double		gp_resqueue_priority_cpucores_per_segment;
char	   *gp_resqueue_priority_default_value;
bool		gp_debug_resqueue_priority = false;

/* Resource group GUCs */
int			gp_resource_group_cpu_priority;
double		gp_resource_group_cpu_limit;
bool		gp_resource_group_cpu_ceiling_enforcement;
double		gp_resource_group_memory_limit;
bool		gp_resource_group_bypass;

/* Perfmon segment GUCs */
int			gp_perfmon_segment_interval;

/* Perfmon debug GUC */
bool		gp_perfmon_print_packet_info;

bool		vmem_process_interrupt = false;
bool		execute_pruned_plan = false;

/* partitioning GUC */
bool		gp_partitioning_dynamic_selection_log;
int			gp_max_partition_level;

/* Upgrade & maintenance GUCs */
bool		gp_maintenance_mode;
bool		gp_maintenance_conn;
bool		allow_segment_DML;
bool		gp_allow_rename_relation_without_lock = false;

/* ignore EXCLUDE clauses in window spec for backwards compatibility */
bool		gp_ignore_window_exclude = false;

/* Time based authentication GUC */
char	   *gp_auth_time_override_str = NULL;

/* Password hashing */
int			password_hash_algorithm = PASSWORD_HASH_MD5;

/* include file/line information to stack traces */
bool		gp_log_stack_trace_lines;

/*
 * If set to true, we will silently insert into the correct leaf
 * part even if the user specifies a wrong leaf part as a insert target
 */
bool		dml_ignore_target_partition_check = false;

/* Planner gucs */
bool		gp_enable_hashjoin_size_heuristic = false;
bool		gp_enable_predicate_propagation = false;
bool		gp_enable_minmax_optimization = true;
bool		gp_enable_multiphase_agg = true;
bool		gp_enable_preunique = TRUE;
bool		gp_eager_preunique = FALSE;
bool		gp_hashagg_streambottom = true;
bool		gp_enable_agg_distinct = true;
bool		gp_enable_dqa_pruning = true;
bool		gp_eager_dqa_pruning = FALSE;
bool		gp_eager_one_phase_agg = FALSE;
bool		gp_eager_two_phase_agg = FALSE;
bool		gp_enable_groupext_distinct_pruning = true;
bool		gp_enable_groupext_distinct_gather = true;
bool		gp_dynamic_partition_pruning = true;
bool		gp_log_dynamic_partition_pruning = false;
bool		gp_cte_sharing = false;
bool		gp_enable_relsize_collection = false;
bool		gp_recursive_cte = true;

/* GUCs for slice table*/
int			gp_max_slices;

/* System Information */
static int	gp_server_version_num;
static char *gp_server_version_string;

/* Query Metrics */
bool		gp_enable_query_metrics = false;
int			gp_instrument_shmem_size = 5120;

/* Security */
bool		gp_reject_internal_tcp_conn = true;

/* copy */
bool		gp_enable_segment_copy_checking = true;

bool		gp_add_column_inherits_table_setting = false;

/* Executor */
bool		gp_enable_mk_sort = true;
bool		gp_enable_motion_mk_sort = true;

/* Enable GDD */
bool		gp_enable_global_deadlock_detector = false;

static const struct config_enum_entry gp_log_format_options[] = {
	{"text", 0},
	{"csv", 1},
	{NULL, 0}
};

static const struct config_enum_entry debug_dtm_action_protocol_options[] = {
	{"none", DTX_PROTOCOL_COMMAND_NONE},
	{"abort_no_prepared", DTX_PROTOCOL_COMMAND_ABORT_NO_PREPARED},
	{"prepare", DTX_PROTOCOL_COMMAND_PREPARE},
	{"abort_some_prepared", DTX_PROTOCOL_COMMAND_ABORT_SOME_PREPARED},
	{"commit_onephase", DTX_PROTOCOL_COMMAND_COMMIT_ONEPHASE},
	{"commit_prepared", DTX_PROTOCOL_COMMAND_COMMIT_PREPARED},
	{"abort_prepared", DTX_PROTOCOL_COMMAND_ABORT_PREPARED},
	{"retry_commit_prepared", DTX_PROTOCOL_COMMAND_RETRY_COMMIT_PREPARED},
	{"retry_abort_prepared", DTX_PROTOCOL_COMMAND_RETRY_ABORT_PREPARED},
	{"recovery_commit_prepared", DTX_PROTOCOL_COMMAND_RECOVERY_COMMIT_PREPARED},
	{"recovery_abort_prepared", DTX_PROTOCOL_COMMAND_RECOVERY_ABORT_PREPARED},
	{"subtransaction_begin", DTX_PROTOCOL_COMMAND_SUBTRANSACTION_BEGIN_INTERNAL},
	{"subtransaction_release", DTX_PROTOCOL_COMMAND_SUBTRANSACTION_RELEASE_INTERNAL},
	{"subtransaction_rollback", DTX_PROTOCOL_COMMAND_SUBTRANSACTION_ROLLBACK_INTERNAL},
	{NULL, 0}
};

static const struct config_enum_entry explain_memory_verbosity_options[] = {
	{"suppress", EXPLAIN_MEMORY_VERBOSITY_SUPPRESS},
	{"summary", EXPLAIN_MEMORY_VERBOSITY_SUMMARY},
	{"detail", EXPLAIN_MEMORY_VERBOSITY_DETAIL},
	{"debug", EXPLAIN_MEMORY_VERBOSITY_DEBUG},
	{NULL, 0}
};

static const struct config_enum_entry debug_dtm_action_options[] = {
	{"none", DEBUG_DTM_ACTION_NONE},
	{"delay", DEBUG_DTM_ACTION_DELAY},
	{"fail_begin_command", DEBUG_DTM_ACTION_FAIL_BEGIN_COMMAND},
	{"fail_end_command", DEBUG_DTM_ACTION_FAIL_END_COMMAND},
	{"panic_begin_command", DEBUG_DTM_ACTION_PANIC_BEGIN_COMMAND},
	{NULL, 0}
};

static const struct config_enum_entry debug_dtm_action_target_options[] = {
	{"none", DEBUG_DTM_ACTION_TARGET_NONE},
	{"protocol", DEBUG_DTM_ACTION_TARGET_PROTOCOL},
	{"sql", DEBUG_DTM_ACTION_TARGET_SQL},
	{NULL, 0}
};

static const struct config_enum_entry gp_autostats_modes[] = {
	{"none", GP_AUTOSTATS_NONE},
	{"on_change", GP_AUTOSTATS_ON_CHANGE},
	{"onchange", GP_AUTOSTATS_ON_CHANGE},
	{"on_no_stats", GP_AUTOSTATS_ON_NO_STATS},
	{NULL, 0}
};

static const struct config_enum_entry gp_interconnect_fc_methods[] = {
	{"loss", INTERCONNECT_FC_METHOD_LOSS},
	{"capacity", INTERCONNECT_FC_METHOD_CAPACITY},
	{NULL, 0}
};

static const struct config_enum_entry gp_interconnect_types[] = {
	{"udpifc", INTERCONNECT_TYPE_UDPIFC},
	{"tcp", INTERCONNECT_TYPE_TCP},
#ifdef ENABLE_IC_PROXY
	{"proxy", INTERCONNECT_TYPE_PROXY},
#endif  /* ENABLE_IC_PROXY */
	{NULL, 0}
};

static const struct config_enum_entry gp_log_verbosity[] = {
	{"terse", GPVARS_VERBOSITY_TERSE},
	{"off", GPVARS_VERBOSITY_OFF},
	{"verbose", GPVARS_VERBOSITY_VERBOSE},
	{"debug", GPVARS_VERBOSITY_DEBUG},
	{NULL, 0}
};

static const struct config_enum_entry gp_resqueue_memory_policies[] = {
	{"none", RESMANAGER_MEMORY_POLICY_NONE},
	{"auto", RESMANAGER_MEMORY_POLICY_AUTO},
	{"eager_free", RESMANAGER_MEMORY_POLICY_EAGER_FREE},
	{NULL, 0}
};


static const struct config_enum_entry password_hash_algorithm_options[] = {
	/* {"none", PASSWORD_HASH_NONE}, * this option is not exposed */
	{"MD5", PASSWORD_HASH_MD5},
	{"SHA-256", PASSWORD_HASH_SHA_256},
	{NULL, 0}
};

IndexCheckType gp_indexcheck_insert = INDEX_CHECK_NONE;
IndexCheckType gp_indexcheck_vacuum = INDEX_CHECK_NONE;

struct config_bool ConfigureNamesBool_gp[] =
{
	{
		{"maintenance_mode", PGC_POSTMASTER, CUSTOM_OPTIONS,
			gettext_noop("Maintenance Mode"),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_maintenance_mode,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_maintenance_conn", PGC_BACKEND, CUSTOM_OPTIONS,
			gettext_noop("Maintenance Connection"),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_maintenance_conn,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_use_legacy_hashops", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
			gettext_noop("If set, new tables will use legacy distribution hashops by default"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_use_legacy_hashops,
		false,
		NULL, NULL, NULL
	},

	{
		{"allow_segment_DML", PGC_USERSET, CUSTOM_OPTIONS,
			gettext_noop("Allow DML on segments"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&allow_segment_DML,
		false,
		NULL, NULL, NULL
	},
	{
		{"gp_allow_rename_relation_without_lock", PGC_USERSET, CUSTOM_OPTIONS,
			gettext_noop("Allow ALTER TABLE RENAME without AccessExclusiveLock"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_allow_rename_relation_without_lock,
		false,
		NULL, NULL, NULL
	},
	{
		{"enable_groupagg", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the planner's use of grouping aggregation plans."),
			NULL
		},
		&enable_groupagg,
		true,
		NULL, NULL, NULL
	},
	{
		{"gp_enable_hashjoin_size_heuristic", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("In hash join plans, the smaller of the two inputs "
						 "(as estimated) is used to build the hash table."),
			gettext_noop("If false, either input could be used to build the "
					  "hash table; the choice depends on the estimated hash "
						 "join cost, which the planner computes for both "
						 "alternatives.  Has no effect on outer or adaptive "
						 "joins."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_enable_hashjoin_size_heuristic,
		false,
		NULL, NULL, NULL
	},
	{
		{"gp_enable_direct_dispatch", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable dispatch for single-row-insert targeted mirror-pairs."),
			gettext_noop("Don't involve the whole cluster if it isn't needed.")
		},
		&gp_enable_direct_dispatch,
		true,
		NULL, NULL, NULL
	},
	{
		{"gp_enable_predicate_propagation", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("When two expressions are equivalent (such as with "
					  "equijoined keys) then the planner applies predicates "
						 "on one expression to the other expression."),
			gettext_noop("If false, planner does not copy predicates.")
		},
		&gp_enable_predicate_propagation,
		true,
		NULL, NULL, NULL
	},
	{
		{"debug_print_prelim_plan", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints the preliminary execution plan to server log."),
			NULL
		},
		&Debug_print_prelim_plan,
		false,
		NULL, NULL, NULL
	},
	{
		{"debug_print_slice_table", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints the slice table to server log."),
			NULL
		},
		&Debug_print_slice_table,
		false,
		NULL, NULL, NULL
	},
	{
		{"log_dispatch_stats", PGC_SUSET, STATS_MONITORING,
			gettext_noop("Writes dispatcher performance statistics to the server log."),
			NULL
		},
		&log_dispatch_stats,
		false,
		check_dispatch_log_stats, NULL, NULL
	},

	{
		{"gp_enable_minmax_optimization", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the planner's use of index scans with limit to implement MIN/MAX."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&gp_enable_minmax_optimization,
		true, NULL, NULL
	},

	{
		{"gp_enable_multiphase_agg", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enables the planner's use of two- or three-stage parallel aggregation plans."),
			gettext_noop("Allows partial aggregation before motion.")
		},
		&gp_enable_multiphase_agg,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_preunique", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable 2-phase duplicate removal."),
			gettext_noop("If true, planner may choose to remove duplicates in "
						 "two phases--before and after redistribution.")
		},
		&gp_enable_preunique,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_eager_preunique", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Experimental feature: 2-phase duplicate removal - cost override."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_eager_preunique,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_agg_distinct", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable 2-phase aggregation to compute a single distinct-qualified aggregate."),
			NULL,
		},
		&gp_enable_agg_distinct,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_agg_distinct_pruning", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable 3-phase aggregation and join to compute distinct-qualified aggregates."),
			NULL,
		},
		&gp_enable_dqa_pruning,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_groupext_distinct_pruning", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable 3-phase aggregation and join to compute distinct-qualified aggregates"
						 " on grouping extension queries."),
			NULL,
		},
		&gp_enable_groupext_distinct_pruning,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_groupext_distinct_gather", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable gathering data to a single node to compute distinct-qualified aggregates"
						 " on grouping extension queries."),
			NULL,
		},
		&gp_enable_groupext_distinct_gather,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_eager_agg_distinct_pruning", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Prefer 3-phase aggregation [and join] to compute distinct-qualified aggregates."),
			gettext_noop("The planner will prefer to use 3-phase aggregation and join to compute "
				"distinct-qualified aggregates whenever enabled and possible"
						 "regardless of cost estimates."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_eager_dqa_pruning,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_eager_one_phase_agg", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Prefer 1-phase aggregation."),
			gettext_noop("The planner will prefer to use 1-phase aggregation whenever possible"
						 "regardless of cost estimates."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_eager_one_phase_agg,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_eager_two_phase_agg", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Prefer 2-phase aggregation."),
			gettext_noop("The planner will prefer to use 2-phase aggregation whenever"
					   "enabled and possible regardless of cost estimates."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_eager_two_phase_agg,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_explain_allstat", PGC_USERSET, CLIENT_CONN_OTHER,
			gettext_noop("Experimental feature: dump stats for all segments in EXPLAIN ANALYZE."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_enable_explain_allstat,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_sort_limit", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable LIMIT operation to be performed while sorting."),
			gettext_noop("Sort more efficiently when plan requires the first <n> rows at most.")
		},
		&gp_enable_sort_limit,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_sort_distinct", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable duplicate removal to be performed while sorting."),
			gettext_noop("Reduces data handling when plan calls for removing duplicates from sorted rows.")
		},
		&gp_enable_sort_distinct,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_mk_sort", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable multi-key sort."),
			gettext_noop("A faster sort."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE

		},
		&gp_enable_mk_sort,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_motion_mk_sort", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable multi-key sort in sorted motion recv."),
			gettext_noop("A faster sort for recv motion"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE

		},
		&gp_enable_motion_mk_sort,
		true,
		NULL, NULL, NULL
	},


#ifdef USE_ASSERT_CHECKING
	{
		{"gp_mk_sort_check", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Extensive check mk_sort"),
			gettext_noop("Expensive debug checking"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_mk_sort_check,
		false,
		NULL, NULL, NULL
	},
#endif

	{
		{"gp_hashagg_streambottom", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Stream the bottom stage of two stage hashagg"),
			gettext_noop("Avoid spilling at the bottom stage of two stage hashagg"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_hashagg_streambottom,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_motion_deadlock_sanity", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable verbose check at planning time."),
			NULL,
			GUC_NO_RESET_ALL | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_enable_motion_deadlock_sanity,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_adjust_selectivity_for_outerjoins", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Adjust selectivity of null tests over outer joins."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&gp_adjust_selectivity_for_outerjoins,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_selectivity_damping_for_scans", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Damping of selectivities for clauses over the same base relation."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_selectivity_damping_for_scans,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_selectivity_damping_for_joins", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Damping of selectivities in join clauses."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_selectivity_damping_for_joins,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_selectivity_damping_sigsort", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Sort selectivities by ascending significance, i.e. smallest first"),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_selectivity_damping_sigsort,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_interconnect_aggressive_retry", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable application-level fast-track interconnect retries"),
			NULL,
			GUC_NO_RESET_ALL | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_interconnect_aggressive_retry,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_select_invisible", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Use dummy snapshot for MVCC visibility calculation."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_select_invisible,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_slow_writer_testmode", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Slow down writer gangs -- to facilitate race-condition testing."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_enable_slow_writer_testmode,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_debug_pgproc", PGC_POSTMASTER, DEVELOPER_OPTIONS,
			gettext_noop("Print debug info relevant to PGPROC."),
			NULL /* long description */ ,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_debug_pgproc,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_heap_require_relhasoids_match", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Issue an error on discovery of a mismatch between relhasoids and a tuple header."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_heap_require_relhasoids_match,
		true,
		NULL, NULL, NULL
	},

	{
		{"debug_burn_xids", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Consume a lot of XIDs, for testing purposes."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_burn_xids,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_cost_hashjoin_chainwalk", PGC_USERSET, QUERY_TUNING_COST,
			gettext_noop("Enable the cost for walking the chain in the hash join"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_cost_hashjoin_chainwalk,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_set_proc_affinity", PGC_POSTMASTER, RESOURCES_KERNEL,
			gettext_noop("On postmaster startup, attempt to bind postmaster to a processor"),
			NULL
		},
		&gp_set_proc_affinity,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_is_writer", PGC_BACKEND, GP_WORKER_IDENTITY,
			gettext_noop("True in a worker process which can directly update its local database segment."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&Gp_is_writer,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_write_shared_snapshot", PGC_USERSET, UNGROUPED,
			gettext_noop("Forces the writer gang to set the shared snapshot."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&Gp_write_shared_snapshot,
		false,
		NULL, assign_gp_write_shared_snapshot, NULL
	},

	{
		{"gp_workfile_compression", PGC_USERSET, RESOURCES_DISK,
			gettext_noop("Enables compression of temporary files."),
			NULL
		},
		&gp_workfile_compression,
		false,
		check_gp_workfile_compression, NULL, NULL
	},

	{
		{"gp_reraise_signal", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Do we attempt to dump core when a serious problem occurs."),
			NULL,
			GUC_NO_RESET_ALL
		},
		&gp_reraise_signal,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_fast_sri", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Enable single-slice single-row inserts."),
			NULL
		},
		&gp_enable_fast_sri,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_full_crc", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Sanity check incoming data stream."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_interconnect_full_crc,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_log_stats", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Emit statistics from the UDP-IC at the end of every statement."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_interconnect_log_stats,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_cache_future_packets", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Control whether future packets are cached."),
			NULL,
		},
		&gp_interconnect_cache_future_packets,
		true,
		NULL, NULL, NULL
	},

	{
		{"resource_scheduler", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Enable resource scheduling."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&ResourceScheduler,
		true,
		NULL, NULL, NULL
	},
	{
		{"resource_select_only", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Enable resource locking of SELECT only."),
			NULL
		},
		&ResourceSelectOnly,
		false,
		NULL, NULL, NULL
	},
	{
		{"resource_cleanup_gangs_on_wait", PGC_USERSET, RESOURCES_MGM,
			gettext_noop("Enable idle gang cleanup before resource lockwait."),
			NULL
		},
		&ResourceCleanupIdleGangs,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_debug_resqueue_priority", PGC_USERSET, RESOURCES_MGM,
			gettext_noop("Print out debugging information about backoff calls."),
			NULL,
			GUC_NO_SHOW_ALL
		},
		&gp_debug_resqueue_priority,
		false,
		NULL, NULL, NULL
	},

	{
		{"dev_opt_unsafe_truncate_in_subtransaction", PGC_USERSET, DEVELOPER_OPTIONS,
		 gettext_noop("Pick unsafe truncate instead of safe truncate inside sub-transaction."),
		 gettext_noop("Usage of this GUC is strongly discouraged and only "
					  "should be used after understanding the impact of using "
					  "the same. Setting the GUC comes with cost of losing "
					  "table data on truncate command despite sub-transaction "
					  "rollback for table created within transaction."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_DISALLOW_IN_AUTO_FILE
		},
		&dev_opt_unsafe_truncate_in_subtransaction,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_print_full_dtm", PGC_SUSET, LOGGING_WHAT,
			gettext_noop("Prints full DTM information to server log."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_print_full_dtm,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_print_snapshot_dtm", PGC_SUSET, LOGGING_WHAT,
			gettext_noop("Prints snapshot DTM information to server log."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_print_snapshot_dtm,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_disable_distributed_snapshot", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Disables distributed snapshots."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_disable_distributed_snapshot,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_abort_after_distributed_prepared", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Cause an abort after all segments are prepared but before the distributed commit is written."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_abort_after_distributed_prepared,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_xlog_record_read", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print debug information for xlog record read."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&debug_xlog_record_read,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_cancel_print", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print cancel detail information."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_cancel_print,
		false,
		NULL, NULL, NULL
	},

	{
		{"test_print_direct_dispatch_info", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("For testing purposes, print information about direct dispatch decisions."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Test_print_direct_dispatch_info,
		false,
		NULL, NULL, NULL
	},

	{
		{"test_print_prefetch_joinqual", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("For testing purposes, print information about if we prefetch join qual."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Test_print_prefetch_joinqual,
		false,
		NULL, NULL, NULL
	},

	{
		{"test_copy_qd_qe_split", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("For testing purposes, print information about which columns are parsed in QD and which in QE."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Test_copy_qd_qe_split,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_bitmap_print_insert", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print log messages for bitmap index insert routines (caution-- generate a lot of logs!)"),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_bitmap_print_insert,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action_primary", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Specify if the primary or mirror segment is the target of the debug DTM action."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action_primary,
		DEBUG_DTM_ACTION_PRIMARY_DEFAULT, NULL, NULL, NULL
	},

	{
		{"gp_disable_tuple_hints", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Specify if reader should set hint bits on tuples."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_disable_tuple_hints,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_local_distributed_cache_stats", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Prints local-distributed cache statistics at end of commit / prepare."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_local_distributed_cache_stats,
		false,
		NULL, NULL, NULL
	},


	{
		{"gp_enable_query_metrics", PGC_POSTMASTER, UNGROUPED,
			gettext_noop("Enable all query metrics collection."),
			NULL
		},
		&gp_enable_query_metrics,
		false,
		NULL, NULL, NULL
	},

	{
		{"coredump_on_memerror", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Generate core dump on memory error."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&coredump_on_memerror,
		false,
		NULL, NULL, NULL
	},
	{
		{"log_autostats", PGC_SUSET, LOGGING_WHAT,
			gettext_noop("Logs details of auto-stats issued ANALYZEs."),
			NULL
		},
		&log_autostats,
		true,
		NULL, NULL, NULL
	},
	{
		{"gp_statistics_pullup_from_child_partition", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("This guc enables the planner to utilize statistics from partitions in planning queries on the parent."),
			NULL
		},
		&gp_statistics_pullup_from_child_partition,
		true,
		NULL, NULL, NULL
	},
	{
		{"gp_statistics_use_fkeys", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("This guc enables the planner to utilize statistics derived from foreign key relationships."),
			NULL
		},
		&gp_statistics_use_fkeys,
		true,
		NULL, NULL, NULL
	},
	{
		{"gp_resqueue_priority", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Enables priority scheduling."),
			NULL
		},
		&gp_enable_resqueue_priority,
		true,
		NULL, NULL, NULL
	},

	{
		{"debug_resource_group", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Prints resource groups debug logs."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_resource_group,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_walrepl_snd", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print debug messages for WAL sender in WAL based replication (Master Mirroring)."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&debug_walrepl_snd,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_walrepl_syncrep", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print debug messages for synchronous behavior in WAL based replication (Master Mirroring)."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&debug_walrepl_syncrep,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_walrepl_rcv", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print debug messages for WAL receiver in WAL based replication (Master Mirroring)."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&debug_walrepl_rcv,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_basebackup", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print debug messages for basebackup mechanism (Master Mirroring)."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&debug_basebackup,
		false,
		NULL, NULL, NULL
	},

	{
		{"debug_latch", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Print debug messages for latch mechanism."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&debug_latch,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_encoding_check_locale_compatibility", PGC_POSTMASTER, CLIENT_CONN_LOCALE,
			gettext_noop("Enable check for compatibility of encoding and locale in createdb"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_encoding_check_locale_compatibility,
		true,
		NULL, NULL, NULL
	},

	/* for pljava */
	{
		{"pljava_release_lingering_savepoints", PGC_SUSET, CUSTOM_OPTIONS,
			gettext_noop("If true, lingering savepoints will be released on function exit; if false, they will be rolled back"),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY
		},
		&pljava_release_lingering_savepoints,
		false,
		NULL, NULL, NULL
	},
	{
		{"pljava_debug", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Stop the backend to attach a debugger"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY
		},
		&pljava_debug,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_keep_all_xlog", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Do not remove old xlog files."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_keep_all_xlog,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_partitioning_dynamic_selection_log", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Print out debugging info for GPDB dynamic partition selection"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_partitioning_dynamic_selection_log,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_perfmon_print_packet_info", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Print out debugging info for a Perfmon packet"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_perfmon_print_packet_info,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_log_stack_trace_lines", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Control if file/line information is included in stack traces"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_stack_trace_lines,
		true,
		NULL, NULL, NULL
	},

	{

		{"gp_log_resqueue_memory", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints out messages related to resource queue's memory management."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_resqueue_memory,
		false,
		NULL, NULL, NULL
	},

	{

		{"gp_log_resgroup_memory", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints out messages related to resource group's memory management."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_resgroup_memory,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_resqueue_print_operator_memory_limits", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints out the memory limit for operators (in explain) assigned by resource queue's "
						 "memory management."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_resqueue_print_operator_memory_limits,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_resgroup_print_operator_memory_limits", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Prints out the memory limit for operators (in explain) assigned by resource group's "
						 "memory management."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_resgroup_print_operator_memory_limits,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_resgroup_debug_wait_queue", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Enable the debugging check on the wait queue of resource group."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_resgroup_debug_wait_queue,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_dynamic_partition_pruning", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("This guc enables plans that can dynamically eliminate scanning of partitions."),
			NULL
		},
		&gp_dynamic_partition_pruning,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_cte_sharing", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("This guc enables sharing of plan fragments for common table expressions."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_cte_sharing,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_relsize_collection", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("This guc enables relsize collection when stats are not present. If disabled and stats are not present a default "
					     "value is used."),
			NULL
		},
		&gp_enable_relsize_collection,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_log_dynamic_partition_pruning", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("This guc enables debug messages related to dynamic partition pruning."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_dynamic_partition_pruning,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_ignore_window_exclude", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
			gettext_noop("Ignore EXCLUDE in window frame specifications."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_ignore_window_exclude,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_create_table_random_default_distribution", PGC_USERSET, COMPAT_OPTIONS,
			gettext_noop("Set the default distribution of a table to RANDOM."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&gp_create_table_random_default_distribution,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_allow_non_uniform_partitioning_ddl", PGC_USERSET, COMPAT_OPTIONS,
			gettext_noop("Allow DDL that will create multi-level partition table with non-uniform hierarchy."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_allow_non_uniform_partitioning_ddl,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_enable_exchange_default_partition", PGC_USERSET, COMPAT_OPTIONS,
			gettext_noop("Allow DDL that will exchange default partitions."),
			NULL
		},
		&gp_enable_exchange_default_partition,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_recursive_cte_prototype", PGC_USERSET, DEPRECATED_OPTIONS,
			gettext_noop("Enable RECURSIVE clauses in CTE queries (deprecated option, use \"gp_recursive_cte\" instead)."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_recursive_cte,
		true, NULL, NULL
	},

	{
		{"gp_recursive_cte", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Enable RECURSIVE clauses in CTE queries."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_recursive_cte,
		true, NULL, NULL
	},

	{
		{"gp_log_optimization_time", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Writes time spent producing a plan to the server log"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_optimization_time,
		false,
		NULL, NULL, NULL
	},

	{
		{"gp_reject_internal_tcp_connection", PGC_POSTMASTER,
			DEVELOPER_OPTIONS,
			gettext_noop("Permit internal TCP connections to the master."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_reject_internal_tcp_conn,
		true,
		NULL, NULL, NULL
	},

	{
		{"dml_ignore_target_partition_check", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Ignores checking whether the user provided correct partition during a direct insert to a leaf partition"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&dml_ignore_target_partition_check,
		false,
		NULL, NULL, NULL
	},

	{
		{"vmem_process_interrupt", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Checks for interrupts before reserving VMEM"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&vmem_process_interrupt,
		false,
		NULL, NULL, NULL
	},

	{
		{"execute_pruned_plan", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Prune plan to discard unwanted plan nodes for each slice before execution"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&execute_pruned_plan,
		true,
		NULL, NULL, NULL
	},

	{
		{"pljava_classpath_insecure", PGC_POSTMASTER, CUSTOM_OPTIONS,
			gettext_noop("Allow pljava_classpath to be set by user per session"),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NOT_IN_SAMPLE
		},
		&pljava_classpath_insecure,
		false,
		check_pljava_classpath_insecure, assign_pljava_classpath_insecure, NULL
	},

	{
		{"gp_enable_segment_copy_checking", PGC_USERSET, CUSTOM_OPTIONS,
			gettext_noop("Enable check the distribution key restriction on segment for command \"COPY FROM ON SEGMENT\"."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_enable_segment_copy_checking,
		true,
		NULL, NULL, NULL
	},

	{
		{"gp_resource_group_bypass", PGC_USERSET, RESOURCES,
			gettext_noop("If the value is true, the query in this session will not be limited by resource group."),
			NULL
		},
		&gp_resource_group_bypass,
		false,
		check_gp_resource_group_bypass, NULL, NULL
	},

	{
		{"gp_resource_group_cpu_ceiling_enforcement", PGC_POSTMASTER, RESOURCES,
			gettext_noop("If the value is true, ceiling enforcement of CPU will be enabled"),
			NULL
		},
		&gp_resource_group_cpu_ceiling_enforcement,
		false, NULL, NULL
	},

	{
		{"stats_queue_level", PGC_SUSET, STATS_COLLECTOR,
			gettext_noop("Collects resource queue-level statistics on database activity."),
			NULL
		},
		&pgstat_collect_queuelevel,
		false, NULL, NULL
	},

	{
		{"create_restartpoint_on_ckpt_record_replay", PGC_SIGHUP, DEVELOPER_OPTIONS,
			gettext_noop("create a restartpoint only on mirror immediately after replaying a checkpoint record."),
			NULL
		},
		&create_restartpoint_on_ckpt_record_replay,
		false, NULL, NULL
	},

	{
		{"gp_enable_global_deadlock_detector", PGC_POSTMASTER, CUSTOM_OPTIONS,
			gettext_noop("Enables the Global Deadlock Detector."),
			NULL
		},
		&gp_enable_global_deadlock_detector,
		false, NULL, NULL
    },

	{
		{"gp_add_column_inherits_table_setting", PGC_USERSET, CUSTOM_OPTIONS,
			gettext_noop("Alter table add column inherits storage setting from the table."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_add_column_inherits_table_setting,
		false,
		NULL, NULL, NULL
	},
	{
		{"gp_autostats_allow_nonowner", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Allow automatic stats collection on tables even for users who are not the owner of the relation."),
			gettext_noop("If disabled, table statistics will be updated only when tables are modified by the owners of the relations.")
		},
		&gp_autostats_allow_nonowner,
		false,
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, false, NULL, NULL
	}
};

struct config_int ConfigureNamesInt_gp[] =
{
	{
		{"gp_max_local_distributed_cache", PGC_POSTMASTER, RESOURCES_MEM,
			gettext_noop("Sets the number of local-distributed transactions to cache for optimizing visibility processing by backends."),
			NULL
		},
		&gp_max_local_distributed_cache,
		1024, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action_segment", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the debug DTM action segment."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action_segment,
		DEBUG_DTM_ACTION_SEGMENT_DEFAULT, -2, 1000,
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action_nestinglevel", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the debug DTM action transaction nesting level."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action_nestinglevel,
		DEBUG_DTM_ACTION_NESTINGLEVEL_DEFAULT, 0, 1000,
		NULL, NULL, NULL
	},

	{
		{"gp_safefswritesize", PGC_BACKEND, RESOURCES,
			gettext_noop("Minimum FS safe write size."),
			NULL
		},
		&gp_safefswritesize,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"planner_work_mem", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Sets the maximum memory to be used for query workspaces, "
						 "used in the planner only."),
			gettext_noop("The planner considers this much memory may be used by each internal "
						 "sort operation and hash table before switching to "
						 "temporary disk files."),
			GUC_UNIT_KB | GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&planner_work_mem,
		32768, 2 * BLCKSZ / 1024, MAX_KILOBYTES,
		NULL, NULL, NULL
	},

	{
		{"statement_mem", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Sets the memory to be reserved for a statement."),
			NULL,
			GUC_UNIT_KB
		},
		&statement_mem,
#ifdef USE_ASSERT_CHECKING
		/** Allow lower values for testing */
		128000, 50, INT_MAX,
#else
		128000, 1000, INT_MAX,
#endif
		gpvars_check_statement_mem, NULL, NULL
	},

	{
		{"memory_spill_ratio", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Sets the memory_spill_ratio for resource group."),
			NULL
		},
		&memory_spill_ratio,
		20, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_resource_group_cpu_priority", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Sets the cpu priority for postgres processes when resource group is enabled."),
			NULL
		},
		&gp_resource_group_cpu_priority,
		10, 1, 256,
		NULL, NULL, NULL
	},

	{
		{"max_statement_mem", PGC_SUSET, RESOURCES_MEM,
			gettext_noop("Sets the maximum value for statement_mem setting."),
			NULL,
			GUC_UNIT_KB
		},
		&max_statement_mem,
		2048000, 32768, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_vmem_limit_per_query", PGC_POSTMASTER, RESOURCES_MEM,
			gettext_noop("Sets the maximum allowed memory per-statement on each segment."),
			NULL,
			GUC_UNIT_KB | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_vmem_limit_per_query,
		0, 0, INT_MAX / 2,
		NULL, NULL, NULL
	},

	{
		{"gp_max_plan_size", PGC_SUSET, RESOURCES_MEM,
			gettext_noop("Sets the maximum size of a plan to be dispatched."),
			NULL,
			GUC_UNIT_KB
		},
		&gp_max_plan_size,
		0, 0, MAX_KILOBYTES,
		NULL, NULL, NULL
	},

	{
		{"gp_max_partition_level", PGC_SUSET, PRESET_OPTIONS,
			gettext_noop("Sets the maximum number of levels allowed when creating a partitioned table."),
			gettext_noop("Use 0 for no limit."),
			GUC_SUPERUSER_ONLY | GUC_NOT_IN_SAMPLE
		},
		&gp_max_partition_level,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_workfile_max_entries", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Sets the maximum number of entries that can be stored in the workfile directory"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_workfile_max_entries,
		8192, 32, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_workfile_limit_files_per_query", PGC_USERSET, RESOURCES,
			gettext_noop("Maximum number of workfiles allowed per query per segment."),
			gettext_noop("0 for no limit. Current query is terminated when limit is exceeded.")
		},
		&gp_workfile_limit_files_per_query,
		100000, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_workfile_limit_per_segment", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Maximum disk space (in KB) used for workfiles per segment."),
			gettext_noop("0 for no limit. Current query is terminated when limit is exceeded."),
			GUC_UNIT_KB
		},
		&gp_workfile_limit_per_segment,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_workfile_limit_per_query", PGC_USERSET, RESOURCES,
			gettext_noop("Maximum disk space (in KB) used for workfiles per query per segment."),
			gettext_noop("0 for no limit. Current query is terminated when limit is exceeded."),
			GUC_UNIT_KB
		},
		&gp_workfile_limit_per_query,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_vmem_idle_resource_timeout", PGC_USERSET, CLIENT_CONN_OTHER,
			gettext_noop("Sets the time a session can be idle (in milliseconds) before we release gangs on the segment DBs to free resources."),
			gettext_noop("A value of 0 turns off the timeout."),
			GUC_UNIT_MS
		},
		&IdleSessionGangTimeout,
#ifdef USE_ASSERT_CHECKING
		600000, 0, INT_MAX,	/* 10 minutes by default on debug
										 * builds. */
#else
		18000, 0, INT_MAX,
#endif
		NULL, NULL, NULL
	},

	{
		{"xid_stop_limit", PGC_POSTMASTER, WAL,
			gettext_noop("Sets the number of XIDs before XID wraparound at which we will no longer allow the system to be started."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&xid_stop_limit,
		100000000, 10000000, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"xid_warn_limit", PGC_POSTMASTER, WAL,
			gettext_noop("Sets the number of XIDs before xid_stop_limit at which we will begin emitting warnings regarding XID wraparound."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&xid_warn_limit,
		500000000, 10000000, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"gp_dbid", PGC_POSTMASTER, PRESET_OPTIONS,
			gettext_noop("The dbid used by this server."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&GpIdentity.dbid,
		UNINITIALIZED_GP_IDENTITY_VALUE, INT_MIN, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_contentid", PGC_POSTMASTER, PRESET_OPTIONS,
			gettext_noop("The contentid used by this server."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&GpIdentity.segindex,
		UNINITIALIZED_GP_IDENTITY_VALUE, INT_MIN, INT_MAX,
		NULL, NULL, NULL
	},

	/*
	 * Solaris doesn't support setting SO_SNDTIMEO, so setting this won't work
	 * on Solaris (MPP-22526)
	 */
	{
		{"gp_connection_send_timeout", PGC_SIGHUP, CLIENT_CONN_OTHER,
			gettext_noop("Timeout for sending data to unresponsive clients (seconds)"),
			gettext_noop("A value of 0 uses the system default."),
		},
		&gp_connection_send_timeout,
		3600, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"max_resource_queues", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Maximum number of resource queues."),
			NULL
		},
		&MaxResourceQueues,
		9, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"max_resource_portals_per_transaction", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Maximum number of resource queues."),
			NULL
		},
		&MaxResourcePortalsPerXact,
		64, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_workfile_max_entries", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Sets the max packet size for the Interconnect."),
			NULL
		},
		&Gp_max_packet_size,
		DEFAULT_PACKET_SIZE, MIN_PACKET_SIZE, MAX_PACKET_SIZE,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_queue_depth", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the maximum size of the receive queue for each connection in the UDP interconnect"),
			NULL
		},
		&Gp_interconnect_queue_depth,
		4, 1, 4096,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_snd_queue_depth", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the maximum size of the send queue for each connection in the UDP interconnect"),
			NULL
		},
		&Gp_interconnect_snd_queue_depth,
		2, 1, 4096,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_timer_period", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the timer period (in ms) for UDP interconnect"),
			NULL,
			GUC_UNIT_MS
		},
		&Gp_interconnect_timer_period,
		5, 1, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_timer_checking_period", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the timer checking period (in ms) for UDP interconnect"),
			NULL,
			GUC_UNIT_MS
		},
		&Gp_interconnect_timer_checking_period,
		20, 1, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_default_rtt", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the default rtt (in ms) for UDP interconnect"),
			NULL,
			GUC_UNIT_MS
		},
		&Gp_interconnect_default_rtt,
		20, 1, 1000,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_min_rto", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the min rto (in ms) for UDP interconnect"),
			NULL,
			GUC_UNIT_MS
		},
		&Gp_interconnect_min_rto,
		20, 1, 1000,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_transmit_timeout", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Timeout (in seconds) on interconnect to transmit a packet"),
			gettext_noop("Used by Interconnect to timeout packet transmission."),
			GUC_UNIT_S
		},
		&Gp_interconnect_transmit_timeout,
		3600, 1, 7200,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_min_retries_before_timeout", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the min retries before reporting a transmit timeout in the interconnect."),
			NULL
		},
		&Gp_interconnect_min_retries_before_timeout,
		100, 1, 4096,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_debug_retry_interval", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the interval by retry times to record a debug message for retry."),
			NULL
		},
		&Gp_interconnect_debug_retry_interval,
		10, 1, 4096,
		NULL, NULL, NULL
	},

	{
		{"gp_udp_bufsize_k", PGC_BACKEND, GP_ARRAY_TUNING,
			gettext_noop("Sets recv buf size of UDP interconnect, for testing."),
			NULL
		},
		&Gp_udp_bufsize_k,
		0, 0, 32768,
		NULL, NULL, NULL
	},

#ifdef USE_ASSERT_CHECKING
	{
		{"gp_udpic_dropseg", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Specifies a segment to which the dropacks, and dropxmit settings will be applied, for testing. (The default is to apply the dropacks and dropxmit settings to all segments)"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_udpic_dropseg,
		UNDEF_SEGMENT, UNDEF_SEGMENT, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_udpic_dropacks_percent", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the percentage of correctly-received acknowledgment packets to synthetically drop, for testing. (affected by gp_udpic_dropseg)"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_udpic_dropacks_percent,
		0, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_udpic_dropxmit_percent", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the percentage of correctly-received data packets to synthetically drop, for testing. (affected by gp_udpic_dropseg)"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_udpic_dropxmit_percent,
		0, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_udpic_fault_inject_percent", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the percentage of fault injected into system calls, for testing. (affected by gp_udpic_dropseg)"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_udpic_fault_inject_percent,
		0, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_udpic_fault_inject_bitmap", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the bitmap for faults injection, for testing. (affected by gp_udpic_dropseg)"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_udpic_fault_inject_bitmap,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_udpic_network_disable_ipv6", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the address info hint to disable the ipv6, for testing. (affected by gp_udpic_dropseg)"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_udpic_network_disable_ipv6,
		0, 0, 1,
		NULL, NULL, NULL
	},
#endif

	{
		{"gp_command_count", PGC_INTERNAL, CLIENT_CONN_OTHER,
			gettext_noop("Shows the number of commands received from the client in this session."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&gp_command_count,
		0, 0, INT_MAX, NULL, NULL
	},

	{
		{"gp_subtrans_warn_limit", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Sets the warning limit on number of subtransactions in a transaction."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&gp_subtrans_warn_limit,
		16777216, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_cached_segworkers_threshold", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the maximum number of segment workers to cache between statements."),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&gp_cached_gang_threshold,
		5, 1, INT_MAX,
		NULL, NULL, NULL
	},


	{
#ifdef USE_ASSERT_CHECKING
		{"gp_debug_linger", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Number of seconds for QD/QE process to linger upon fatal internal error."),
			gettext_noop("Allows an opportunity to debug the backend process before it terminates."),
			GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL | GUC_UNIT_S
		},
		&gp_debug_linger,
		120, 0, 3600,
#else
		{"gp_debug_linger", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Number of seconds for QD/QE process to linger upon fatal internal error."),
			gettext_noop("Allows an opportunity to debug the backend process before it terminates."),
			GUC_NOT_IN_SAMPLE | GUC_NO_RESET_ALL | GUC_UNIT_S
		},
		&gp_debug_linger,
		0, 0, 3600,
#endif
		NULL, NULL, NULL
	},

	{
		{"gp_qd_port", PGC_BACKEND, GP_WORKER_IDENTITY,
			gettext_noop("Shows the Master Postmaster port."),
			gettext_noop("0 for a session's entry process (qDisp)"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&qdPostmasterPort,
		0, INT_MIN, INT_MAX,
		NULL, NULL, NULL
	},


	{
		{"gp_sort_flags", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Experimental feature: Generic sort flags."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_sort_flags,
		10000, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_sort_max_distinct", PGC_USERSET, QUERY_TUNING_OTHER,
			gettext_noop("Experimental feature: max number of distinct values for sort."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_sort_max_distinct,
		20000, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_setup_timeout", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Timeout (in seconds) on interconnect setup that occurs at query start"),
			gettext_noop("Used by Interconnect to timeout the setup of the communication fabric."),
			GUC_UNIT_S
		},
		&interconnect_setup_timeout,
		7200, 0, 7200,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_tcp_listener_backlog", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Size of the listening queue for each TCP interconnect socket"),
			gettext_noop("Cooperate with kernel parameter net.core.somaxconn and net.ipv4.tcp_max_syn_backlog to tune network performance."),
			GUC_NOT_IN_SAMPLE
		},
		&listenerBacklog,
		128, 0, 65535,
		NULL, NULL, NULL
	},

	{
		{"gp_snapshotadd_timeout", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Timeout (in seconds) on setup of new connection snapshot"),
			gettext_noop("Used by the transaction manager."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_UNIT_S
		},
		&gp_snapshotadd_timeout,
		10, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_segment_connect_timeout", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Maximum time (in seconds) allowed for a new worker process to start or a mirror to respond."),
			gettext_noop("0 indicates 'wait forever'."),
			GUC_UNIT_S
		},
		&gp_segment_connect_timeout,
		180, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_fts_probe_retries", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("Number of retries for FTS to complete probing a segment."),
			gettext_noop("Used by the fts-probe process."),
		},
		&gp_fts_probe_retries,
		5, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_fts_probe_timeout", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("Maximum time (in seconds) allowed for FTS to complete probing a segment."),
			gettext_noop("Used by the fts-probe process."),
			GUC_UNIT_S
		},
		&gp_fts_probe_timeout,
		20, 0, 3600,
		NULL, NULL, NULL
	},

	{
		{"gp_fts_probe_interval", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("A complete probe of all segments starts each time a timer with this period expires."),
			gettext_noop("Used by the fts-probe process. "),
			GUC_UNIT_S
		},
		&gp_fts_probe_interval,
		60, 10, 3600,
		NULL, NULL, NULL
	},

	{
		{"gp_fts_mark_mirror_down_grace_period", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("Time (in seconds) allowed to mirror after disconnection, to reconnect before being marked as down in configuration by FTS."),
			gettext_noop("Used by the fts-probe process."),
			GUC_UNIT_S
		},
		&gp_fts_mark_mirror_down_grace_period,
		30, 0, 3600,
		NULL, NULL, NULL
	},

	{
		{"gp_fts_replication_attempt_count", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("Primary-mirror replication connection max continuously attempt count for FTS"),
			gettext_noop("Used by the fts-probe process.")
		},
		&gp_fts_replication_attempt_count,
		10, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_dtx_recovery_interval", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("A complete checking in dtx recovery process starts each time a timer with this period expires."),
			gettext_noop("Used by the dtx recovery process. "),
			GUC_UNIT_S
		},
		&gp_dtx_recovery_interval,
		60, 5, 3600,
		NULL, NULL, NULL
	},

	{
		{"gp_dtx_recovery_prepared_period", PGC_SIGHUP, GP_ARRAY_TUNING,
			gettext_noop("Gather prepared transactions before the time (in seconds) to find potential orphaned ones."),
			gettext_noop("Used by the dtx recovery process. "),
			GUC_UNIT_S
		},
		&gp_dtx_recovery_prepared_period,
		120, 0, 3600,
		NULL, NULL, NULL
	},

	{
		{"gp_gang_creation_retry_count", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("After a gang-creation fails, retry the number of times if failure is retryable."),
			gettext_noop("A value of zero disables retries."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_gang_creation_retry_count,
		5, 0, 128,
		NULL, NULL, NULL
	},

	{
		{"gp_gang_creation_retry_timer", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Wait this many milliseconds between gang-creation-retries."),
			NULL,
			GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_gang_creation_retry_timer,
		2000, 100, 120000,
		NULL, NULL, NULL
	},

	{
		{"gp_session_id", PGC_BACKEND, CLIENT_CONN_OTHER,
			gettext_noop("Global ID used to uniquely identify a particular session in an Greenplum Database array"),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&gp_session_id,
		-1, INT_MIN, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_segments_for_planner", PGC_USERSET, QUERY_TUNING_COST,
			gettext_noop("If >0, number of segment dbs for the planner to assume in its cost and size estimates."),
			gettext_noop("If 0, estimates are based on the actual number of segment dbs.")
		},
		&gp_segments_for_planner,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_hashjoin_tuples_per_bucket", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Target density of hashtable used by Hashjoin during execution"),
			gettext_noop("A smaller value will tend to produce larger hashtables, which increases join performance"),
			GUC_NOT_IN_SAMPLE
		},
		&gp_hashjoin_tuples_per_bucket,
		5, 1, 25,
		NULL, NULL, NULL
	},

	{
		{"gp_hashagg_groups_per_bucket", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Target density of hashtable used by Hashagg during execution"),
			gettext_noop("A smaller value will tend to produce larger hashtables, which increases agg performance"),
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_hashagg_groups_per_bucket,
		5, 1, 25,
		NULL, NULL, NULL
	},

	{
		{"gp_hashagg_default_nbatches", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Default number of batches for hashagg's (re-)spilling phases."),
			gettext_noop("Must be a power of two."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_hashagg_default_nbatches,
		32, 4, 1048576,
		check_gp_hashagg_default_nbatches, NULL, NULL
	},

	{
		{"gp_motion_slice_noop", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Make motion nodes in certain slices noop"),
			gettext_noop("Make motion nodes noop, to help analyze performance"),
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_motion_slice_noop,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_reject_percent_threshold", PGC_USERSET, GP_ERROR_HANDLING,
			gettext_noop("Reject limit in percent starts calculating after this number of rows processed"),
			NULL
		},
		&gp_reject_percent_threshold,
		300, 0, INT_MAX,
		NULL, NULL, NULL
	},


	{
		{"gp_instrument_shmem_size", PGC_POSTMASTER, UNGROUPED,
			gettext_noop("Sets the size of shmem allocated for instrumentation."),
			NULL,
			GUC_UNIT_KB
		},
		&gp_instrument_shmem_size,
		5120, 0, 131072,
		NULL, NULL, NULL
	},

	{
		{"gp_vmem_protect_limit", PGC_POSTMASTER, RESOURCES_MEM,
			gettext_noop("Virtual memory limit (in MB) of Greenplum memory protection."),
			NULL,
		},
		&gp_vmem_protect_limit,
		8192, 0, INT_MAX / 2,
		NULL, NULL, NULL
	},

	{
		{"runaway_detector_activation_percent", PGC_POSTMASTER, RESOURCES_MEM,
			gettext_noop("The runaway detector activates if the used vmem exceeds this percentage of the vmem quota. Set to 100 to disable runaway detection."),
			NULL,
		},
		&runaway_detector_activation_percent,
		90, 0, 100,
		NULL, NULL, NULL
	},

	{
		{"gp_vmem_protect_segworker_cache_limit", PGC_POSTMASTER, RESOURCES_MEM,
			gettext_noop("Max virtual memory limit (in MB) for a segworker to be cachable."),
			NULL,
		},
		&gp_vmem_protect_gang_cache_limit,
		500, 1, INT_MAX / 2,
		NULL, NULL, NULL
	},

	{
		{"gp_autostats_on_change_threshold", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Threshold for number of tuples added to table by CTAS or Insert-to to trigger autostats in on_change mode. See gp_autostats_mode."),
			NULL
		},
		&gp_autostats_on_change_threshold,
		INT_MAX, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_distinct_grouping_sets_threshold", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Threshold for the number of grouping sets whose distinct-qualified "
						 "aggregate computation will be rewritten based on the multi-phrase aggregation. "
						 "The rest of grouping sets in the same query will not be rewritten."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_distinct_grouping_sets_threshold,
		32, 0, 1024,
		NULL, NULL, NULL
	},

	{
		{"gp_resqueue_priority_local_interval", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("A measure of how often a backend process must consider backing off."),
			NULL,
			GUC_NO_SHOW_ALL
		},
		&gp_resqueue_priority_local_interval,
		100000, 500, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"gp_resqueue_priority_sweeper_interval", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Frequency (in ms) at which sweeper process re-evaluates CPU shares."),
			NULL
		},
		&gp_resqueue_priority_sweeper_interval,
		1000, 500, 15000,
		NULL, NULL, NULL
	},
	{
		{"gp_resqueue_priority_inactivity_timeout", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("If a backend does not report progress in this time (in ms), it is deemed inactive."),
			NULL,
			GUC_NO_SHOW_ALL
		},
		&gp_resqueue_priority_inactivity_timeout,
		2000, 500, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"gp_resqueue_priority_grouping_timeout", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("A backend gives up on finding a better group leader after this timeout (in ms)."),
			NULL,
			GUC_NO_SHOW_ALL
		},
		&gp_resqueue_priority_grouping_timeout,
		1000, 1000, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"gp_resource_group_queuing_timeout", PGC_USERSET, RESOURCES_MGM,
			gettext_noop("A transaction gives up on queuing on a resource group after this timeout (in ms)."),
			NULL,
			GUC_UNIT_MS
		},
		&gp_resource_group_queuing_timeout,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"gp_perfmon_segment_interval", PGC_POSTMASTER, STATS,
			gettext_noop("Interval (in ms) between sending segment statistics to perfmon."),
			NULL,
			GUC_NO_SHOW_ALL
		},
		&gp_perfmon_segment_interval,
		1000, 500, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_segworker_relative_priority", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Priority for the segworkers relative to the postmaster's priority."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_segworker_relative_priority,
		PRIO_MAX, 0, PRIO_MAX,
		NULL, NULL, NULL
	},

	/* for pljava */
	{
		{"pljava_statement_cache_size", PGC_SUSET, CUSTOM_OPTIONS,
			gettext_noop("Size of the prepared statement MRU cache"),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY
		},
		&pljava_statement_cache_size,
		0, 0, 512,
		NULL, NULL, NULL
	},

	{
		{"gp_resqueue_memory_policy_auto_fixed_mem", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Sets the fixed amount of memory reserved for non-memory intensive operators in the AUTO policy."),
			NULL,
			GUC_UNIT_KB | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_resqueue_memory_policy_auto_fixed_mem,
		100, 50, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_resgroup_memory_policy_auto_fixed_mem", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Sets the fixed amount of memory reserved for non-memory intensive operators in the AUTO policy."),
			NULL,
			GUC_UNIT_KB | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_resgroup_memory_policy_auto_fixed_mem,
		100, 50, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_global_deadlock_detector_period", PGC_SIGHUP, LOCK_MANAGEMENT,
			gettext_noop("Sets the executing period of global deadlock detector backend."),
			NULL,
			GUC_UNIT_S
		},
		&gp_global_deadlock_detector_period,
		120, 5, INT_MAX, NULL, NULL
	},

	{
		{"memory_profiler_dataset_size", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set the size in GB"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&memory_profiler_dataset_size,
		0, 0, INT_MAX,
		NULL, NULL, NULL
	},
	{
		{"repl_catchup_within_range", PGC_SUSET, REPLICATION_STANDBY,
			gettext_noop("Sets the maximum number of xlog segments allowed to lag"
					  " when the backends can start blocking despite the WAL"
					   " sender being in catchup phase. (Master Mirroring)"),
			NULL,
			GUC_SUPERUSER_ONLY
		},
		&repl_catchup_within_range,
		1, 0, UINT_MAX / XLogSegSize,
		NULL, NULL, NULL
	},

	{
		{"wait_for_replication_threshold", PGC_SIGHUP, REPLICATION_MASTER,
			gettext_noop("Maximum amount of WAL written by a transaction prior to waiting for replication."),
			gettext_noop("This is used just to prevent primary from racing too ahead "
						 "and avoid huge replication lag. A value of 0 disables "
						 "the behavior"),
			GUC_UNIT_KB
		},
		&rep_lag_avoidance_threshold,
		1024, 0, MAX_KILOBYTES,
		NULL, NULL, NULL
	},

	{
		{"gp_initial_bad_row_limit", PGC_USERSET, UNGROUPED,
			gettext_noop("Stops processing when number of the first bad rows exceeding this value"),
			NULL
		},
		&gp_initial_bad_row_limit,
		1000, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_indexcheck_insert", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Validate that a unique index does not already have the new tid during insert."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		(int *) &gp_indexcheck_insert,
		INDEX_CHECK_NONE, 0, INDEX_CHECK_ALL,
		NULL, NULL, NULL
	},

	{
		{"gp_indexcheck_vacuum", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Validate index after lazy vacuum."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		(int *) &gp_indexcheck_vacuum,
		INDEX_CHECK_NONE, 0, INDEX_CHECK_ALL,
		NULL, NULL, NULL
	},

	{
		{"dtx_phase2_retry_count", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Maximum number of attempts to finish a prepared transaction."),
			gettext_noop("The coordinator PANICs if a prepared transaction cannot be"
						 " committed after this number of attempts."),
			GUC_SUPERUSER_ONLY |  GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&dtx_phase2_retry_count,
		10, 0, INT_MAX,
		NULL, NULL, NULL
	},

	{
		/* Can't be set in postgresql.conf */
		{"gp_server_version_num", PGC_INTERNAL, PRESET_OPTIONS,
			gettext_noop("Shows the Greenplum server version as an integer."),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&gp_server_version_num,
		GP_VERSION_NUM, GP_VERSION_NUM, GP_VERSION_NUM,
		NULL, NULL, NULL
	},

	{
		{"gp_max_slices", PGC_USERSET, PRESET_OPTIONS,
			gettext_noop("Maximum slices for a single query"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&gp_max_slices,
		0, 0, INT_MAX, NULL, NULL
	},

	{
		{"gp_dispatch_keepalives_idle", PGC_POSTMASTER, GP_ARRAY_TUNING,
			gettext_noop("Time between issuing TCP keepalives from GPDB QD to its QEs."),
			gettext_noop("A value of 0 uses the system default."),
			GUC_UNIT_S | GUC_NOT_IN_SAMPLE
		},
		&gp_dispatch_keepalives_idle,
		0, 0, MAX_GP_DISPATCH_KEEPALIVES_IDLE,
		NULL, NULL, NULL
	},

	{
		{"gp_dispatch_keepalives_interval", PGC_POSTMASTER, GP_ARRAY_TUNING,
			gettext_noop("Time between TCP keepalive retransmits from GPDB QD to its QEs."),
			gettext_noop("A value of 0 uses the system default."),
			GUC_UNIT_S | GUC_NOT_IN_SAMPLE
		},
		&gp_dispatch_keepalives_interval,
		0, 0, MAX_GP_DISPATCH_KEEPALIVES_INTERVAL,
		NULL, NULL, NULL
	},

	{
		{"gp_dispatch_keepalives_count", PGC_POSTMASTER, GP_ARRAY_TUNING,
			gettext_noop("Maximum number of TCP keepalive retransmits from GPDB QD to its QEs."),
			gettext_noop("This controls the number of consecutive keepalive retransmits that can be "
						 "lost before a QD/QE connection is considered dead. A value of 0 uses the "
						 "system default."),
			GUC_NOT_IN_SAMPLE
		},
		&gp_dispatch_keepalives_count,
		0, 0, MAX_GP_DISPATCH_KEEPALIVES_COUNT,
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, 0, 0, 0, NULL, NULL
	}
};

struct config_real ConfigureNamesReal_gp[] =
{
	{
		{"disable_cost", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the planner's cost of a disabled path."),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&disable_cost,
		1.0e10, 1.0e10, 1.0e30,
		NULL, NULL, NULL
	},

	{
		{"gp_motion_cost_per_row", PGC_USERSET, QUERY_TUNING_COST,
			gettext_noop("Sets the planner's estimate of the cost of "
						 "moving a row between worker processes."),
			gettext_noop("If >0, the planner uses this value -- instead of double the "
					"cpu_tuple_cost -- for Motion operator cost estimation.")
		},
		&gp_motion_cost_per_row,
		0, 0, DBL_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_selectivity_damping_factor", PGC_USERSET, QUERY_TUNING_METHOD,
			gettext_noop("Factor used in selectivity damping."),
			gettext_noop("Values 1..N, 1 = basic damping, greater values emphasize damping"),
			GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL
		},
		&gp_selectivity_damping_factor,
		1.0, 1.0, DBL_MAX,
		NULL, NULL, NULL
	},

	{
		{"gp_resqueue_priority_cpucores_per_segment", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Number of processing units associated with a segment."),
			NULL
		},
		&gp_resqueue_priority_cpucores_per_segment,
		4.0, 0.1, 512.0,
		NULL, NULL, NULL
	},

	{
		{"gp_resource_group_cpu_limit", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Maximum percentage of CPU resources assigned to a cluster."),
			NULL
		},
		&gp_resource_group_cpu_limit,
		0.9, 0.1, 1.0,
		NULL, NULL, NULL
	},

	{
		{"gp_resource_group_memory_limit", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Maximum percentage of memory resources assigned to a cluster."),
			NULL
		},
		&gp_resource_group_memory_limit,
		0.7, 0.0001, 1.0,
		NULL, NULL, NULL
	},

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, 0.0, 0.0, 0.0, NULL, NULL
	}
};

struct config_string ConfigureNamesString_gp[] =
{
	{
		{"memory_profiler_run_id", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set the unique run ID for memory profiling"),
			gettext_noop("Any string is acceptable"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&memory_profiler_run_id,
		"none",
		NULL, NULL, NULL
	},

	{
		{"memory_profiler_dataset_id", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set the dataset ID for memory profiling"),
			gettext_noop("Any string is acceptable"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&memory_profiler_dataset_id,
		"none",
		NULL, NULL, NULL
	},

	{
		{"memory_profiler_query_id", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Set the query ID for memory profiling"),
			gettext_noop("Any string is acceptable"),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&memory_profiler_query_id,
		"none",
		NULL, NULL, NULL
	},

	{
		{"gp_session_role", PGC_BACKEND, GP_WORKER_IDENTITY,
			gettext_noop("Reports the default role for the session."),
			gettext_noop("Valid values are DISPATCH, EXECUTE, and UTILITY."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&gp_session_role_string,
		"dispatch",
		check_gp_session_role, assign_gp_session_role, show_gp_session_role
	},

	{
		{"gp_role", PGC_SUSET, CLIENT_CONN_OTHER,
			gettext_noop("Sets the role for the session."),
			gettext_noop("Valid values are DISPATCH, EXECUTE, and UTILITY."),
			GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&gp_role_string,
		"dispatch",
		NULL, assign_gp_role, show_gp_role
	},

	{
		{"gp_qd_hostname", PGC_BACKEND, GP_WORKER_IDENTITY,
			gettext_noop("Shows the QD Hostname. Blank when run on the QD"),
			NULL,
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&qdHostname,
		"",
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action_sql_command_tag", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the debug DTM action sql command tag."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action_sql_command_tag,
		"",
		NULL, NULL, NULL
	},

	{
		{"gp_resqueue_priority_default_value", PGC_POSTMASTER, RESOURCES_MGM,
			gettext_noop("Default weight when one cannot be associated with a statement."),
			NULL,
			GUC_NO_SHOW_ALL
		},
		&gp_resqueue_priority_default_value,
		"MEDIUM",
		gpvars_check_gp_resqueue_priority_default_value, NULL, NULL
	},

	{
		{"gp_resource_manager", PGC_POSTMASTER, RESOURCES,
			gettext_noop("Sets the type of resource manager."),
			gettext_noop("Only support \"queue\" and \"group\" for now.")
		},
		&gp_resource_manager_str,
		"queue",
		gpvars_check_gp_resource_manager_policy,
		gpvars_assign_gp_resource_manager_policy,
		gpvars_show_gp_resource_manager_policy,
	},

	/* for pljava */
	{
		{"pljava_vmoptions", PGC_SUSET, CUSTOM_OPTIONS,
			gettext_noop("Options sent to the JVM when it is created"),
			NULL,
			GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY
		},
		&pljava_vmoptions,
		"",
		NULL, NULL, NULL
	},
	{
		{"pljava_classpath", PGC_SUSET, CUSTOM_OPTIONS,
			gettext_noop("classpath used by the JVM"),
			NULL,
			GUC_NOT_IN_SAMPLE
		},
		&pljava_classpath,
		"",
		NULL, NULL, NULL
	},

	{
		{"gp_auth_time_override", PGC_SIGHUP, DEVELOPER_OPTIONS,
			gettext_noop("The timestamp used for enforcing time constraints."),
			gettext_noop("For testing purposes only."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_auth_time_override_str,
		"",
		NULL, NULL, NULL
	},

	{
		/* Can't be set in postgresql.conf */
		{"gp_server_version", PGC_INTERNAL, PRESET_OPTIONS,
			gettext_noop("Shows the Greenplum server version."),
			NULL,
			GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE
		},
		&gp_server_version_string,
		GP_VERSION,
		NULL, NULL, NULL
	},

#ifdef ENABLE_IC_PROXY
	{
		{"gp_interconnect_proxy_addresses", PGC_SIGHUP, GP_ARRAY_CONFIGURATION,
			gettext_noop("Sets the ic-proxy addresses as \"content:ip:port ...\", must be ordered by content, the port is ignored at the moment."),
			gettext_noop("e.g. \"-1:10.0.0.1:2000 0:10.0.0.2:2000 1:10.0.0.2:2001\""),
			GUC_NO_SHOW_ALL | GUC_GPDB_NO_SYNC
		},
		&gp_interconnect_proxy_addresses,
		"",
		NULL, NULL, NULL
	},
#endif  /* ENABLE_IC_PROXY */

	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, NULL, NULL, NULL
	}
};

struct config_enum ConfigureNamesEnum_gp[] =
{
	{
		{"gp_workfile_caching_loglevel", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the logging level for workfile caching debugging messages"),
			gettext_noop("Valid values are DEBUG5, DEBUG4, DEBUG3, DEBUG2, "
						 "DEBUG1, LOG, NOTICE, WARNING, and ERROR. Each level includes all the "
						 "levels that follow it. The later the level, the fewer messages are "
						 "sent."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_workfile_caching_loglevel,
		DEBUG1, server_message_level_options,
		NULL, NULL, NULL
	},

	{
		{"gp_sessionstate_loglevel", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the logging level for session state debugging messages"),
			gettext_noop("Valid values are DEBUG5, DEBUG4, DEBUG3, DEBUG2, "
						 "DEBUG1, LOG, NOTICE, WARNING, and ERROR. Each level includes all the "
						 "levels that follow it. The later the level, the fewer messages are "
						 "sent."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_sessionstate_loglevel,
		DEBUG1, server_message_level_options,
		NULL, NULL, NULL
	},

	{
		{"gp_log_format", PGC_POSTMASTER, LOGGING_WHERE,
			gettext_noop("Sets the format for log files."),
			gettext_noop("Valid values are TEXT, CSV.")
		},
		&gp_log_format,
		1, gp_log_format_options,
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action_protocol", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the debug DTM action protocol."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action_protocol,
		DTX_PROTOCOL_COMMAND_NONE, debug_dtm_action_protocol_options,
		NULL, NULL, NULL
	},

	{
		{"password_hash_algorithm", PGC_SUSET, CONN_AUTH_SECURITY,
			gettext_noop("The cryptograph hash algorithm to apply to passwords before storing them."),
			gettext_noop("Valid values are MD5 or SHA-256."),
			GUC_SUPERUSER_ONLY
		},
		&password_hash_algorithm,
		PASSWORD_HASH_MD5, password_hash_algorithm_options,
		NULL, NULL, NULL
	},

	{
		{"explain_memory_verbosity", PGC_USERSET, RESOURCES_MEM,
			gettext_noop("Experimental feature: show memory account usage in EXPLAIN ANALYZE."),
			gettext_noop("Valid values are SUPPRESS, SUMMARY, DETAIL, and DEBUG.")
		},
		&explain_memory_verbosity,
		EXPLAIN_MEMORY_VERBOSITY_SUPPRESS, explain_memory_verbosity_options,
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the debug DTM action."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action,
		DEBUG_DTM_ACTION_NONE, debug_dtm_action_options,
		NULL, NULL, NULL
	},

	{
		{"debug_dtm_action_target", PGC_SUSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the debug DTM action target."),
			NULL,
			GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&Debug_dtm_action_target,
		DEBUG_DTM_ACTION_TARGET_NONE, debug_dtm_action_target_options,
		NULL, NULL, NULL
	},

	{
		{"gp_autostats_mode", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the autostats mode."),
			gettext_noop("Valid values are NONE, ON_CHANGE, ON_NO_STATS. ON_CHANGE requires setting gp_autostats_on_change_threshold.")
		},
		&gp_autostats_mode,
		GP_AUTOSTATS_NONE, gp_autostats_modes,
		NULL, NULL, NULL
	},

	{
		{"gp_autostats_mode_in_functions", PGC_USERSET, DEVELOPER_OPTIONS,
			gettext_noop("Sets the autostats mode for statements in procedural language functions."),
			gettext_noop("Valid values are NONE, ON_CHANGE, ON_NO_STATS. ON_CHANGE requires setting gp_autostats_on_change_threshold.")
		},
		&gp_autostats_mode_in_functions,
		GP_AUTOSTATS_NONE, gp_autostats_modes,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_fc_method", PGC_USERSET, GP_ARRAY_TUNING,
			gettext_noop("Sets the flow control method used for UDP interconnect."),
			gettext_noop("Valid values are \"capacity\" and \"loss\".")
		},
		&Gp_interconnect_fc_method,
		INTERCONNECT_FC_METHOD_LOSS, gp_interconnect_fc_methods,
		NULL, NULL, NULL
	},

	{
		{"gp_interconnect_type", PGC_BACKEND, GP_ARRAY_TUNING,
			gettext_noop("Sets the protocol used for inter-node communication."),
			gettext_noop("Valid values are \"tcp\", \"udpifc\""
#ifdef ENABLE_IC_PROXY
						 " and \"proxy\""
#endif  /* ENABLE_IC_PROXY */
						 ".")
		},
		&Gp_interconnect_type,
		INTERCONNECT_TYPE_UDPIFC, gp_interconnect_types,
		NULL, NULL, NULL
	},

	{
		{"gp_log_fts", PGC_SIGHUP, LOGGING_WHAT,
			gettext_noop("Sets the verbosity of logged messages pertaining to fault probing."),
			gettext_noop("Valid values are \"off\", \"terse\", \"verbose\" and \"debug\"."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_fts,
		GPVARS_VERBOSITY_TERSE, gp_log_verbosity,
		NULL, NULL, NULL
	},

	{
		{"gp_log_gang", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Sets the verbosity of logged messages pertaining to worker process creation and management."),
			gettext_noop("Valid values are \"off\", \"terse\", \"verbose\" and \"debug\"."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_gang,
		GPVARS_VERBOSITY_OFF, gp_log_verbosity,
		NULL, NULL, NULL
	},

	{
		{"gp_log_interconnect", PGC_USERSET, LOGGING_WHAT,
			gettext_noop("Sets the verbosity of logged messages pertaining to connections between worker processes."),
			gettext_noop("Valid values are \"off\", \"terse\", \"verbose\" and \"debug\"."),
			GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE
		},
		&gp_log_interconnect,
		GPVARS_VERBOSITY_TERSE, gp_log_verbosity,
		NULL, NULL, NULL
	},

	{
		{"gp_resqueue_memory_policy", PGC_SUSET, RESOURCES_MGM,
			gettext_noop("Sets the policy for memory allocation of queries."),
			gettext_noop("Valid values are NONE, AUTO, EAGER_FREE.")
		},
		&gp_resqueue_memory_policy,
		RESMANAGER_MEMORY_POLICY_NONE, gp_resqueue_memory_policies,
		NULL, NULL, NULL
	},

	{
		{"gp_resgroup_memory_policy", PGC_SUSET, RESOURCES_MGM,
			gettext_noop("Sets the policy for memory allocation of queries."),
			gettext_noop("Valid values are AUTO, EAGER_FREE.")
		},
		&gp_resgroup_memory_policy,
		RESMANAGER_MEMORY_POLICY_EAGER_FREE, gp_resqueue_memory_policies, NULL, NULL
	},


	/* End-of-list marker */
	{
		{NULL, 0, 0, NULL, NULL}, NULL, 0, NULL, NULL, NULL
	}
};

/*
 * For system defined GUC must assign a tag either GUC_GPDB_NEED_SYNC
 * or GUC_GPDB_NO_SYNC. We deprecated direct define in guc.c, instead,
 * add into sync_guc_names_array or unsync_guc_names_array.
 */
static const char *sync_guc_names_array[] =
{
	#include "utils/sync_guc_name.h"
};

static const char *unsync_guc_names_array[] =
{
	#include "utils/unsync_guc_name.h"
};

int sync_guc_num = 0;
int unsync_guc_num = 0;

static int guc_array_compare(const void *a, const void *b)
{
	const char *namea = *(const char **)a;
	const char *nameb = *(const char **)b;

	return guc_name_compare(namea, nameb);
}

void gpdb_assign_sync_flag(struct config_generic **guc_variables, int size, bool predefine)
{
	static bool init = false;
	/* ordering guc_name_array alphabets */
	if (!init) {
		sync_guc_num = sizeof(sync_guc_names_array) / sizeof(char *);
		qsort((void *) sync_guc_names_array, sync_guc_num,
		      sizeof(char *), guc_array_compare);

		unsync_guc_num = sizeof(unsync_guc_names_array) / sizeof(char *);
		qsort((void *) unsync_guc_names_array, unsync_guc_num,
		      sizeof(char *), guc_array_compare);

		init = true;
	}

	for (int i = 0; i < size; i ++)
	{
		struct config_generic *var = guc_variables[i];

		/* if the sync flags is defined in guc variable, skip it */
		if (var->flags & (GUC_GPDB_NEED_SYNC | GUC_GPDB_NO_SYNC))
			continue;

		char *res = (char *) bsearch((void *) &var->name,
		                             (void *) sync_guc_names_array,
		                             sync_guc_num,
		                             sizeof(char *),
		                             guc_array_compare);
		if (!res)
		{
			char *res = (char *) bsearch((void *) &var->name,
			                             (void *) unsync_guc_names_array,
			                             unsync_guc_num,
			                             sizeof(char *),
			                             guc_array_compare);

			/* for predefined guc, we force its name in one array.
			 * for the third-part libraries gucs introduced by customer
			 * we assign unsync flags as default.
			 */
			if (!res && predefine)
			{
				ereport(ERROR,
				        (errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("predefined guc name: %s contain neither "
						 "sync_guc_names_array nor unsync_guc_names_array", var->name)));
			}

			var->flags |= GUC_GPDB_NO_SYNC;
		}
		else
		{
			var->flags |= GUC_GPDB_NEED_SYNC;
		}
	}
}

static bool
check_pljava_classpath_insecure(bool *newval, void **extra, GucSource source)
{
	if ( *newval == true )
	{
		struct config_generic *pljava_cp = find_option("pljava_classpath", false, ERROR);
		if (pljava_cp != NULL)
		{
			pljava_cp->context = PGC_USERSET;
		}
		else
		{
			GUC_check_errdetail("Failed to set insecure PLJAVA classpath");
			return false;
		}
	}
	return true;
}

static void
assign_pljava_classpath_insecure(bool newval, void *extra)
{
	if ( newval == true )
	{
		struct config_generic *pljava_cp = find_option("pljava_classpath", false, ERROR);
		if (pljava_cp != NULL)
		{
			pljava_cp->context = PGC_USERSET;
		}
	}
}

static bool
check_gp_resource_group_bypass(bool *newval, void **extra, GucSource source)
{
	if (!ResGroupIsAssigned())
		return true;

	GUC_check_errmsg("SET gp_resource_group_bypass cannot run inside a transaction block");
	return false;
}

static bool
check_dispatch_log_stats(bool *newval, void **extra, GucSource source)
{
	if (*newval &&
		(log_parser_stats || log_planner_stats || log_executor_stats || log_statement_stats))
	{
		if (source >= PGC_S_INTERACTIVE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot enable \"log_dispatch_stats\" when "
							"\"log_statement_stats\", "
							"\"log_parser_stats\", \"log_planner_stats\", "
							"or \"log_executor_stats\" is true")));
		/* source == PGC_S_OVERRIDE means do it anyway, eg at xact abort */
		else if (source != PGC_S_OVERRIDE)
			return false;
	}
	return true;
}

bool
check_gp_hashagg_default_nbatches(int *newval, void **extra, GucSource source)
{
	/* Must be a power of two */
	if (0 == (*newval & (*newval - 1)))
	{
		return true;
	}
	else
	{
		GUC_check_errmsg("gp_hashagg_default_nbatches must be a power of two");
		return false;
	}
}

/*
 * Set GUC value in GP_REPLICATION_CONFIG_FILENAME.
 *
 * If value is NULL, then this GUC is removed from the configuration.
 *
 * If name exists, its value will be updated.
 * otherwise, the new named GUC will be added.
 */
void
set_gp_replication_config(const char *name, const char *value)
{
	A_Const aconst = {.type = T_A_Const, .val = {.type = T_String, .val.str = pstrdup(value)}};
	List *args = list_make1(&aconst);
	VariableSetStmt setstmt = {.type = T_VariableSetStmt, .kind = VAR_SET_VALUE, .name = pstrdup(name), .args = args};
	AlterSystemStmt alterSystemStmt = {.type = T_AlterSystemStmt, .setstmt = &setstmt};
    
	AlterSystemSetConfigFile(&alterSystemStmt);
}


/*
 * lookup_autostats_mode_by_value
 *
 * Return the string value name for the specified value. This is essentially a
 * specialized version of config_enum_lookup_by_value() for use by autostats.c
 * debugging code.
 */
const char *
lookup_autostats_mode_by_value(GpAutoStatsModeValue val)
{
	const struct config_enum_entry *entry;

	for (entry = gp_autostats_modes; entry && entry->name; entry++)
	{
		if (entry->val == val)
			return entry->name;
	}

	elog(ERROR, "could not find autostats mode %d", val);
	return NULL;				/* silence compiler */
}


static bool
check_gp_workfile_compression(bool *newval, void **extra, GucSource source)
{
#ifndef HAVE_LIBZSTD
	if (*newval)
	{
		GUC_check_errmsg("workfile compresssion is not supported by this build");
		return false;
	}
#endif
	return true;
}

void
DispatchSyncPGVariable(struct config_generic * gconfig)
{
	StringInfoData buffer;

	if (Gp_role != GP_ROLE_DISPATCH || IsBootstrapProcessingMode())
		return;

	initStringInfo( &buffer );

	appendStringInfo(&buffer, "SET ");

	switch (gconfig->vartype)
	{
		case PGC_BOOL:
		{
			struct config_bool *bguc = (struct config_bool *) gconfig;

			appendStringInfo(&buffer, "%s TO %s", gconfig->name, *(bguc->variable) ? "true" : "false");
			break;
		}
		case PGC_INT:
		{
			struct config_int *iguc = (struct config_int *) gconfig;

			appendStringInfo(&buffer, "%s TO %d", gconfig->name, *iguc->variable);
			break;
		}
		case PGC_REAL:
		{
			struct config_real *rguc = (struct config_real *) gconfig;

			appendStringInfo(&buffer, " %s TO %f", gconfig->name, *rguc->variable);
			break;
		}
		case PGC_STRING:
		{
			struct config_string *sguc = (struct config_string *) gconfig;
			const char *str = *sguc->variable;
			int			i;

			appendStringInfo(&buffer, "%s TO ", gconfig->name);

			/*
			 * All whitespace characters must be escaped. See
			 * pg_split_opts() in the backend.
			 */
			for (i = 0; str[i] != '\0'; i++)
				appendStringInfoChar(&buffer, str[i]);

			break;
		}
		case PGC_ENUM:
		{
			struct config_enum *eguc = (struct config_enum *) gconfig;
			int			value = *eguc->variable;
			const char *str = config_enum_lookup_by_value(eguc, value);
			int			i;

			appendStringInfo(&buffer, "%s TO ", gconfig->name);

			/*
			 * All whitespace characters must be escaped. See
			 * pg_split_opts() in the backend. (Not sure if an enum value
			 * can have whitespace, but let's be prepared.)
			 */
			for (i = 0; str[i] != '\0'; i++)
			{
				if (isspace((unsigned char) str[i]))
					appendStringInfoChar(&buffer, '\\');

				appendStringInfoChar(&buffer, str[i]);
			}
			break;
		}
		default:
			Insist(false);

	}

	CdbDispatchSetCommand(buffer.data, false);
}
