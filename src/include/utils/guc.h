/*--------------------------------------------------------------------
 * guc.h
 *
 * External declarations pertaining to backend/utils/misc/guc.c and
 * backend/utils/misc/guc-file.l
 *
 * Portions Copyright (c) 2007-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Copyright (c) 2000-2014, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc.h
 *--------------------------------------------------------------------
 */
#ifndef GUC_H
#define GUC_H

#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/array.h"

#define MAX_AUTHENTICATION_TIMEOUT (600)
#define MAX_PRE_AUTH_DELAY (60)
/*
 * One connection must be reserved for FTS to always able to probe
 * primary. So, this acts as lower limit on reserved superuser connections on
 * primaries.
 */
#define RESERVED_FTS_CONNECTIONS (1)


/*
 * Automatic configuration file name for ALTER SYSTEM.
 * This file will be used to store values of configuration parameters
 * set by ALTER SYSTEM command.
 */
#define PG_AUTOCONF_FILENAME		"postgresql.auto.conf"

/*
 * Certain options can only be set at certain times. The rules are
 * like this:
 *
 * INTERNAL options cannot be set by the user at all, but only through
 * internal processes ("server_version" is an example).  These are GUC
 * variables only so they can be shown by SHOW, etc.
 *
 * POSTMASTER options can only be set when the postmaster starts,
 * either from the configuration file or the command line.
 *
 * SIGHUP options can only be set at postmaster startup or by changing
 * the configuration file and sending the HUP signal to the postmaster
 * or a backend process. (Notice that the signal receipt will not be
 * evaluated immediately. The postmaster and the backend check it at a
 * certain point in their main loop. It's safer to wait than to read a
 * file asynchronously.)
 *
 * BACKEND options can only be set at postmaster startup, from the
 * configuration file, or by client request in the connection startup
 * packet (e.g., from libpq's PGOPTIONS variable).  Furthermore, an
 * already-started backend will ignore changes to such an option in the
 * configuration file.  The idea is that these options are fixed for a
 * given backend once it's started, but they can vary across backends.
 *
 * SUSET options can be set at postmaster startup, with the SIGHUP
 * mechanism, or from SQL if you're a superuser.
 *
 * USERSET options can be set by anyone any time.
 */
typedef enum
{
	PGC_INTERNAL,
	PGC_POSTMASTER,
	PGC_SIGHUP,
	PGC_BACKEND,
	PGC_SUSET,
	PGC_USERSET
} GucContext;

/*
 * The following type records the source of the current setting.  A
 * new setting can only take effect if the previous setting had the
 * same or lower level.  (E.g, changing the config file doesn't
 * override the postmaster command line.)  Tracking the source allows us
 * to process sources in any convenient order without affecting results.
 * Sources <= PGC_S_OVERRIDE will set the default used by RESET, as well
 * as the current value.  Note that source == PGC_S_OVERRIDE should be
 * used when setting a PGC_INTERNAL option.
 *
 * PGC_S_INTERACTIVE isn't actually a source value, but is the
 * dividing line between "interactive" and "non-interactive" sources for
 * error reporting purposes.
 *
 * PGC_S_TEST is used when testing values to be used later ("doit" will always
 * be false, so this never gets stored as the actual source of any value).
 * For example, ALTER DATABASE/ROLE tests proposed per-database or per-user
 * defaults this way, and CREATE FUNCTION tests proposed function SET clauses
 * this way.  This is an interactive case, but it needs its own source value
 * because some assign hooks need to make different validity checks in this
 * case.  In particular, references to nonexistent database objects generally
 * shouldn't throw hard errors in this case, at most NOTICEs, since the
 * objects might exist by the time the setting is used for real.
 *
 * NB: see GucSource_Names in guc.c if you change this.
 */
typedef enum
{
	PGC_S_DEFAULT,				/* hard-wired default ("boot_val") */
	PGC_S_DYNAMIC_DEFAULT,		/* default computed during initialization */
	PGC_S_ENV_VAR,				/* postmaster environment variable */
	PGC_S_FILE,					/* postgresql.conf */
	PGC_S_ARGV,					/* postmaster command line */
	PGC_S_GLOBAL,				/* global in-database setting */
	PGC_S_DATABASE,				/* per-database setting */
	PGC_S_USER,					/* per-user setting */
	PGC_S_DATABASE_USER,		/* per-user-and-database setting */
	PGC_S_CLIENT,				/* from client connection request */
	PGC_S_RESGROUP,				/* per-resgroup setting */
	PGC_S_OVERRIDE,				/* special case to forcibly set default */
	PGC_S_INTERACTIVE,			/* dividing line for error reporting */
	PGC_S_TEST,					/* test per-database or per-user setting */
	PGC_S_SESSION				/* SET command */
} GucSource;

/*
 * Parsing the configuration file(s) will return a list of name-value pairs
 * with source location info.
 *
 * If "ignore" is true, don't attempt to apply the item (it might be an item
 * we determined to be duplicate, for instance).
 */
typedef struct ConfigVariable
{
	char	   *name;
	char	   *value;
	char	   *filename;
	int			sourceline;
	struct ConfigVariable *next;
	bool		ignore;
} ConfigVariable;

extern bool ParseConfigFile(const char *config_file, const char *calling_file,
				bool strict, int depth, int elevel,
				ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigFp(FILE *fp, const char *config_file,
			  int depth, int elevel,
			  ConfigVariable **head_p, ConfigVariable **tail_p);
extern bool ParseConfigDirectory(const char *includedir,
					 const char *calling_file,
					 int depth, int elevel,
					 ConfigVariable **head_p,
					 ConfigVariable **tail_p);
extern void FreeConfigVariables(ConfigVariable *list);

/*
 * The possible values of an enum variable are specified by an array of
 * name-value pairs.  The "hidden" flag means the value is accepted but
 * won't be displayed when guc.c is asked for a list of acceptable values.
 */
struct config_enum_entry
{
	const char *name;
	int			val;
	bool		hidden;
};

/*
 * Signatures for per-variable check/assign/show hook functions
 */
typedef bool (*GucBoolCheckHook) (bool *newval, void **extra, GucSource source);
typedef bool (*GucIntCheckHook) (int *newval, void **extra, GucSource source);
typedef bool (*GucRealCheckHook) (double *newval, void **extra, GucSource source);
typedef bool (*GucStringCheckHook) (char **newval, void **extra, GucSource source);
typedef bool (*GucEnumCheckHook) (int *newval, void **extra, GucSource source);

typedef void (*GucBoolAssignHook) (bool newval, void *extra);
typedef void (*GucIntAssignHook) (int newval, void *extra);
typedef void (*GucRealAssignHook) (double newval, void *extra);
typedef void (*GucStringAssignHook) (const char *newval, void *extra);
typedef void (*GucEnumAssignHook) (int newval, void *extra);

typedef const char *(*GucShowHook) (void);

/*
 * Miscellaneous
 */
typedef enum
{
	/* Types of set_config_option actions */
	GUC_ACTION_SET,				/* regular SET command */
	GUC_ACTION_LOCAL,			/* SET LOCAL command */
	GUC_ACTION_SAVE				/* function SET option, or temp assignment */
} GucAction;

#define GUC_QUALIFIER_SEPARATOR '.'

/*
 * bit values in "flags" of a GUC variable
 */
#define GUC_LIST_INPUT			0x0001	/* input can be list format */
#define GUC_LIST_QUOTE			0x0002	/* double-quote list elements */
#define GUC_NO_SHOW_ALL			0x0004	/* exclude from SHOW ALL */
#define GUC_NO_RESET_ALL		0x0008	/* exclude from RESET ALL */
#define GUC_REPORT				0x0010	/* auto-report changes to client */
#define GUC_NOT_IN_SAMPLE		0x0020	/* not in postgresql.conf.sample */
#define GUC_DISALLOW_IN_FILE	0x0040	/* can't set in postgresql.conf */
#define GUC_CUSTOM_PLACEHOLDER	0x0080	/* placeholder for custom variable */
#define GUC_SUPERUSER_ONLY		0x0100	/* show only to superusers */
#define GUC_IS_NAME				0x0200	/* limit string to NAMEDATALEN-1 */

#define GUC_UNIT_KB				0x0400	/* value is in kilobytes */
#define GUC_UNIT_BLOCKS			0x0800	/* value is in blocks */
#define GUC_UNIT_XBLOCKS		0x0C00	/* value is in xlog blocks */
#define GUC_UNIT_MEMORY			0x0C00	/* mask for KB, BLOCKS, XBLOCKS */

#define GUC_UNIT_MS				0x1000	/* value is in milliseconds */
#define GUC_UNIT_S				0x2000	/* value is in seconds */
#define GUC_UNIT_MIN			0x4000	/* value is in minutes */
#define GUC_UNIT_TIME			0x7000	/* mask for MS, S, MIN */

#define GUC_NOT_WHILE_SEC_REST	0x8000	/* can't set if security restricted */
#define GUC_DISALLOW_IN_AUTO_FILE	0x00010000	/* can't set in PG_AUTOCONF_FILENAME */

/* GPDB speific */
#define GUC_GPDB_NEED_SYNC     0x00020000  /* guc value is synced between master and primary */
#define GUC_GPDB_NO_SYNC       0x00040000  /* guc value is not synced between master and primary */
#define GUC_DISALLOW_USER_SET  0x00080000 /* Do not allow this GUC to be set by the user */

#define GUC_UNIT_MB			   0x00100000 /* value is in metabytes */
/* to avoid ABI change, add a new macro to include GUC_UNIT_MB*/
#define GUC_UNIT_MEMORY_NEW	   (GUC_UNIT_MEMORY | GUC_UNIT_MB)

/* GUC lists for gp_guc_list_show().  (List of struct config_generic) */
extern List    *gp_guc_list_for_explain;
extern List    *gp_guc_list_for_no_plan;

/* Changed GUC which need to be pass to QE from QD */
extern List *gp_guc_restore_list;
extern bool gp_guc_need_restore;

/* GUC vars that are actually declared in guc.c, rather than elsewhere */
extern bool log_duration;
extern bool Debug_print_plan;
extern bool Debug_print_parse;
extern bool Debug_print_rewritten;
extern bool Debug_pretty_print;

extern bool dev_opt_unsafe_truncate_in_subtransaction;
extern bool	Debug_print_full_dtm;
extern bool	Debug_print_snapshot_dtm;
extern bool Debug_disable_distributed_snapshot;
extern bool Debug_abort_after_distributed_prepared;
extern bool Debug_bitmap_print_insert;
extern bool enable_checksum_on_tables;
extern int  gp_max_local_distributed_cache;
extern bool gp_local_distributed_cache_stats;
extern bool gp_heap_require_relhasoids_match;
extern bool	debug_xlog_record_read;
extern bool Debug_cancel_print;
extern bool	Debug_database_command_print;
extern bool Debug_resource_group;
extern bool gp_create_table_random_default_distribution;
extern bool gp_allow_non_uniform_partitioning_ddl;
extern bool gp_enable_exchange_default_partition;
extern int  dtx_phase2_retry_count;

/* WAL replication debug gucs */
extern bool debug_walrepl_snd;
extern bool debug_walrepl_syncrep;
extern bool debug_walrepl_rcv;
extern bool debug_basebackup;

extern int rep_lag_avoidance_threshold;

/* Latch mechanism debug GUCs */
extern bool debug_latch;

extern bool gp_maintenance_mode;
extern bool gp_maintenance_conn;
extern bool allow_segment_DML;
extern bool gp_allow_rename_relation_without_lock;

extern bool gp_ignore_window_exclude;

extern bool gp_ignore_error_table;

extern bool	Debug_dtm_action_primary;

extern bool gp_log_optimization_time;
extern bool log_parser_stats;
extern bool log_planner_stats;
extern bool log_executor_stats;
extern bool log_statement_stats;
extern bool log_dispatch_stats;
extern bool log_btree_build_stats;

extern PGDLLIMPORT bool check_function_bodies;
extern bool default_with_oids;
extern bool SQL_inheritance;

extern int	log_min_error_statement;
extern PGDLLIMPORT int log_min_messages;
extern PGDLLIMPORT int client_min_messages;
extern int	log_min_duration_statement;
extern int	log_temp_files;

extern int	temp_file_limit;

extern int	num_temp_buffers;

extern bool vmem_process_interrupt;
extern bool execute_pruned_plan;

extern bool gp_partitioning_dynamic_selection_log;
extern int gp_max_partition_level;

extern bool gp_perfmon_print_packet_info;

extern bool gp_enable_relsize_collection;

/* Debug DTM Action */
typedef enum
{
	DEBUG_DTM_ACTION_NONE = 0,
	DEBUG_DTM_ACTION_DELAY = 1,
	DEBUG_DTM_ACTION_FAIL_BEGIN_COMMAND = 2,
	DEBUG_DTM_ACTION_FAIL_END_COMMAND = 3,
	DEBUG_DTM_ACTION_PANIC_BEGIN_COMMAND = 4,

	DEBUG_DTM_ACTION_LAST = 4
}	DebugDtmAction;

/* Debug DTM Action */
typedef enum
{
	DEBUG_DTM_ACTION_TARGET_NONE = 0,
	DEBUG_DTM_ACTION_TARGET_PROTOCOL = 1,
	DEBUG_DTM_ACTION_TARGET_SQL = 2,

	DEBUG_DTM_ACTION_TARGET_LAST = 2
}	DebugDtmActionTarget;

extern int Debug_dtm_action;
extern int Debug_dtm_action_target;
extern int Debug_dtm_action_protocol;
extern int Debug_dtm_action_segment;
extern int Debug_dtm_action_nestinglevel;

extern PGDLLIMPORT char *ConfigFileName;
extern char *HbaFileName;
extern char *IdentFileName;
extern char *external_pid_file;

extern char *application_name;

extern char *Debug_dtm_action_sql_command_tag;
extern char *Debug_dtm_action_str;
extern char *Debug_dtm_action_target_str;

/* Enable check for compatibility of encoding and locale in createdb */
extern bool gp_encoding_check_locale_compatibility;

extern int	tcp_keepalives_idle;
extern int	tcp_keepalives_interval;
extern int	tcp_keepalives_count;

extern int	gp_connection_send_timeout;

extern bool create_restartpoint_on_ckpt_record_replay;

extern char  *data_directory;

/* GUCs for slice table*/
extern int	gp_max_slices;

/* Time based authentication GUC */
extern char  *gp_auth_time_override_str;

/* copy GUC */
extern bool gp_enable_segment_copy_checking;

extern int writable_external_table_bufsize;

/* Enable passing of query constraints to external table providers */
extern bool gp_external_enable_filter_pushdown;

/* Enable the Global Deadlock Detector */
extern bool gp_enable_global_deadlock_detector;

typedef enum
{
	INDEX_CHECK_NONE,
	INDEX_CHECK_SYSTEM,
	INDEX_CHECK_ALL
} IndexCheckType;

extern IndexCheckType gp_indexcheck_insert;
extern IndexCheckType gp_indexcheck_vacuum;

/* Storage option names */
#define SOPT_FILLFACTOR    "fillfactor"
/* Max number of chars needed to hold value of a storage option. */
#define MAX_SOPT_VALUE_LEN 15

/*
 * Functions exported by guc.c
 */
extern void SetConfigOption(const char *name, const char *value,
				GucContext context, GucSource source);

extern void DefineCustomBoolVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 bool *valueAddr,
						 bool bootValue,
						 GucContext context,
						 int flags,
						 GucBoolCheckHook check_hook,
						 GucBoolAssignHook assign_hook,
						 GucShowHook show_hook);

extern void DefineCustomIntVariable(
						const char *name,
						const char *short_desc,
						const char *long_desc,
						int *valueAddr,
						int bootValue,
						int minValue,
						int maxValue,
						GucContext context,
						int flags,
						GucIntCheckHook check_hook,
						GucIntAssignHook assign_hook,
						GucShowHook show_hook);

extern void DefineCustomRealVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 double *valueAddr,
						 double bootValue,
						 double minValue,
						 double maxValue,
						 GucContext context,
						 int flags,
						 GucRealCheckHook check_hook,
						 GucRealAssignHook assign_hook,
						 GucShowHook show_hook);

extern void DefineCustomStringVariable(
						   const char *name,
						   const char *short_desc,
						   const char *long_desc,
						   char **valueAddr,
						   const char *bootValue,
						   GucContext context,
						   int flags,
						   GucStringCheckHook check_hook,
						   GucStringAssignHook assign_hook,
						   GucShowHook show_hook);

extern void DefineCustomEnumVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 int *valueAddr,
						 int bootValue,
						 const struct config_enum_entry * options,
						 GucContext context,
						 int flags,
						 GucEnumCheckHook check_hook,
						 GucEnumAssignHook assign_hook,
						 GucShowHook show_hook);

extern void EmitWarningsOnPlaceholders(const char *className);

extern const char *GetConfigOption(const char *name, bool missing_ok,
				bool restrict_superuser);
extern const char *GetConfigOptionResetString(const char *name);
extern int	GetConfigOptionFlags(const char *name, bool missing_ok);
extern void ProcessConfigFile(GucContext context);
extern void InitializeGUCOptions(void);
extern bool SelectConfigFiles(const char *userDoption, const char *progname);
extern void ResetAllOptions(void);
extern void AtStart_GUC(void);
extern int	NewGUCNestLevel(void);
extern void AtEOXact_GUC(bool isCommit, int nestLevel);
extern void BeginReportingGUCOptions(void);
extern void ParseLongOption(const char *string, char **name, char **value);
extern bool parse_int(const char *value, int *result, int flags,
		  const char **hintmsg);
extern bool parse_real(const char *value, double *result);
extern int set_config_option(const char *name, const char *value,
				  GucContext context, GucSource source,
				  GucAction action, bool changeVal, int elevel);
extern void AlterSystemSetConfigFile(AlterSystemStmt *setstmt);
extern char *GetConfigOptionByName(const char *name, const char **varname);
extern void GetConfigOptionByNum(int varnum, const char **values, bool *noshow);
extern int	GetNumConfigOptions(void);

extern void SetPGVariable(const char *name, List *args, bool is_local);
extern void SetPGVariableOptDispatch(const char *name, List *args, bool is_local, bool gp_dispatch);
extern void GetPGVariable(const char *name, DestReceiver *dest);
extern TupleDesc GetPGVariableResultDesc(const char *name);

extern void ExecSetVariableStmt(VariableSetStmt *stmt, bool isTopLevel);
extern char *ExtractSetVariableArgs(VariableSetStmt *stmt);

extern void ProcessGUCArray(ArrayType *array,
				GucContext context, GucSource source, GucAction action);
extern ArrayType *GUCArrayAdd(ArrayType *array, const char *name, const char *value);
extern ArrayType *GUCArrayDelete(ArrayType *array, const char *name);
extern ArrayType *GUCArrayReset(ArrayType *array);

extern void pg_timezone_abbrev_initialize(void);

extern List *gp_guc_list_show(GucSource excluding, List *guclist);

extern struct config_generic *find_option(const char *name,
				bool create_placeholders, int elevel);

extern bool select_gp_replication_config_files(const char *configdir, const char *progname);

extern void set_gp_replication_config(const char *name, const char *value);

extern bool parse_real(const char *value, double *result);

#ifdef EXEC_BACKEND
extern void write_nondefault_variables(GucContext context);
extern void read_nondefault_variables(void);
#endif

/* Support for messages reported from GUC check hooks */

extern PGDLLIMPORT char *GUC_check_errmsg_string;
extern PGDLLIMPORT char *GUC_check_errdetail_string;
extern PGDLLIMPORT char *GUC_check_errhint_string;

extern void GUC_check_errcode(int sqlerrcode);

#define GUC_check_errmsg \
	pre_format_elog_string(errno, TEXTDOMAIN), \
	GUC_check_errmsg_string = format_elog_string

#define GUC_check_errdetail \
	pre_format_elog_string(errno, TEXTDOMAIN), \
	GUC_check_errdetail_string = format_elog_string

#define GUC_check_errhint \
	pre_format_elog_string(errno, TEXTDOMAIN), \
	GUC_check_errhint_string = format_elog_string


/*
 * The following functions are not in guc.c, but are declared here to avoid
 * having to include guc.h in some widely used headers that it really doesn't
 * belong in.
 */

/* in commands/tablespace.c */
extern bool check_default_tablespace(char **newval, void **extra, GucSource source);
extern bool check_temp_tablespaces(char **newval, void **extra, GucSource source);
extern void assign_temp_tablespaces(const char *newval, void *extra);

/* in catalog/namespace.c */
extern bool check_search_path(char **newval, void **extra, GucSource source);
extern void assign_search_path(const char *newval, void *extra);

/* in access/transam/xlog.c */
extern bool check_wal_buffers(int *newval, void **extra, GucSource source);
extern void assign_xlog_sync_method(int new_sync_method, void *extra);

/* in cdb/cdbvars.c */
extern bool check_gp_session_role(char **newval, void **extra, GucSource source);
extern void assign_gp_session_role(const char *newval, void *extra);
extern const char *show_gp_session_role(void);
extern bool check_gp_role(char **newval, void **extra, GucSource source);
extern void assign_gp_role(const char *newval, void *extra);
extern const char *show_gp_role(void);
extern void assign_gp_write_shared_snapshot(bool newval, void *extra);
extern bool gpvars_check_gp_resource_manager_policy(char **newval, void **extra, GucSource source);
extern void gpvars_assign_gp_resource_manager_policy(const char *newval, void *extra);
extern const char *gpvars_show_gp_resource_manager_policy(void);
extern const char *gpvars_assign_gp_resqueue_memory_policy(const char *newval, bool doit, GucSource source);
extern const char *gpvars_show_gp_resqueue_memory_policy(void);
extern bool gpvars_check_statement_mem(int *newval, void **extra, GucSource source);
extern int guc_name_compare(const char *namea, const char *nameb);
extern void DispatchSyncPGVariable(struct config_generic * gconfig);


extern StdRdOptions *defaultStdRdOptions(char relkind);

#endif   /* GUC_H */
