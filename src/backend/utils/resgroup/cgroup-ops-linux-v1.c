/*-------------------------------------------------------------------------
 *
 * cgroup-ops-linux-v1.c
 *	  OS dependent resource group operations - cgroup implementation
 *
 * Copyright (c) 2017 VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/utils/resgroup/cgroup-ops-linux-v1.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/cgroup.h"
#include "utils/resgroup.h"
#include "utils/cgroup-ops-v1.h"
#include "utils/vmem_tracker.h"

#ifndef __linux__
#error  cgroup is only available on linux
#endif

#include <fcntl.h>
#include <unistd.h>
#include <sched.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <stdio.h>
#include <mntent.h>

static CGroupSystemInfo cgroupSystemInfoAlpha = {
		0,
		""
};

/*
 * Interfaces for OS dependent operations.
 *
 * Resource group relies on OS dependent group implementation to manage
 * resources like cpu usage, such as cgroup on Linux system.
 * We call it OS group in below function description.
 *
 * So far these operations are mainly for CPU rate limitation and accounting.
 */


/*
 * cgroup memory permission is only mandatory on 6.x and main;
 * on 5.x we need to make it optional to provide backward compatibilities.
 */
#define CGROUP_MEMORY_IS_OPTIONAL (GP_VERSION_NUM < 60000)
/*
 * cpuset permission is only mandatory on 6.x and main;
 * on 5.x we need to make it optional to provide backward compatibilities.
 */
#define CGROUP_CPUSET_IS_OPTIONAL (GP_VERSION_NUM < 60000)


typedef struct PermItem PermItem;
typedef struct PermList PermList;

struct PermItem
{
	CGroupComponentType comp;
	const char			*prop;
	int					perm;
};

struct PermList
{
	const PermItem	*items;
	bool			optional;
	bool			*presult;
};

#define foreach_perm_list(i, lists) \
	for ((i) = 0; (lists)[(i)].items; (i)++)

#define foreach_perm_item(i, items) \
	for ((i) = 0; (items)[(i)].comp != CGROUP_COMPONENT_UNKNOWN; (i)++)

#define foreach_comp_type(comp) \
	for ((comp) = CGROUP_COMPONENT_FIRST; \
		 (comp) < CGROUP_COMPONENT_COUNT; \
		 (comp)++)


/* The functions current file used */
static void detect_component_dirs_alpha(void);
static void dump_component_dirs_alpha(void);

static bool perm_list_check_alpha(const PermList *permlist, Oid group, bool report);
static bool check_permission_alpha(Oid group, bool report);
static bool check_cpuset_permission_alpha(Oid group, bool report);
static void check_component_hierarchy_alpha();

static void init_cpu_alpha(void);
static void init_cpuset_alpha(void);

static void create_default_cpuset_group_alpha(void);
static int64 get_cfs_period_us_alpha(CGroupComponentType component);

/*
 * currentGroupIdInCGroup & oldCaps are used for reducing redundant
 * file operations
 */
static Oid currentGroupIdInCGroup = InvalidOid;

static int64 system_cfs_quota_us = -1LL;
static int64 parent_cfs_quota_us = -1LL;

/*
 * These checks should keep in sync with gpMgmt/bin/gpcheckresgroupimpl
 */
static const PermItem perm_items_cpu[] =
{
	{ CGROUP_COMPONENT_CPU, "", R_OK | W_OK | X_OK },
	{ CGROUP_COMPONENT_CPU, "cgroup.procs", R_OK | W_OK },
	{ CGROUP_COMPONENT_CPU, "cpu.cfs_period_us", R_OK | W_OK },
	{ CGROUP_COMPONENT_CPU, "cpu.cfs_quota_us", R_OK | W_OK },
	{ CGROUP_COMPONENT_CPU, "cpu.shares", R_OK | W_OK },
	{ CGROUP_COMPONENT_UNKNOWN, NULL, 0 }
};
static const PermItem perm_items_cpu_acct[] =
{
	{ CGROUP_COMPONENT_CPUACCT, "", R_OK | W_OK | X_OK },
	{ CGROUP_COMPONENT_CPUACCT, "cgroup.procs", R_OK | W_OK },
	{ CGROUP_COMPONENT_CPUACCT, "cpuacct.usage", R_OK },
	{ CGROUP_COMPONENT_CPUACCT, "cpuacct.stat", R_OK },
	{ CGROUP_COMPONENT_UNKNOWN, NULL, 0 }
};
static const PermItem perm_items_cpuset[] =
{
	{ CGROUP_COMPONENT_CPUSET, "", R_OK | W_OK | X_OK },
	{ CGROUP_COMPONENT_CPUSET, "cgroup.procs", R_OK | W_OK },
	{ CGROUP_COMPONENT_CPUSET, "cpuset.cpus", R_OK | W_OK },
	{ CGROUP_COMPONENT_CPUSET, "cpuset.mems", R_OK | W_OK },
	{ CGROUP_COMPONENT_UNKNOWN, NULL, 0 }
};
static const PermItem perm_items_memory[] =
{
	{ CGROUP_COMPONENT_MEMORY, "", R_OK | W_OK | X_OK },
	{ CGROUP_COMPONENT_MEMORY, "memory.limit_in_bytes", R_OK | W_OK },
	{ CGROUP_COMPONENT_MEMORY, "memory.usage_in_bytes", R_OK },
	{ CGROUP_COMPONENT_UNKNOWN, NULL, 0 }
};
static const PermItem perm_items_swap[] =
{
	{ CGROUP_COMPONENT_MEMORY, "", R_OK | W_OK | X_OK },
	{ CGROUP_COMPONENT_MEMORY, "memory.memsw.limit_in_bytes", R_OK | W_OK },
	{ CGROUP_COMPONENT_MEMORY, "memory.memsw.usage_in_bytes", R_OK },
	{ CGROUP_COMPONENT_UNKNOWN, NULL, 0 }
};

/*
 * just for cpuset check, same as the cpuset Permlist in permlists
 */
static const PermList cpusetPermList =
{
	perm_items_cpuset,
	CGROUP_CPUSET_IS_OPTIONAL,
	&gp_resource_group_enable_cgroup_cpuset,
};

/*
 * Permission groups.
 */
static const PermList permlists[] =
{
	/*
	 * swap permissions are optional.
	 *
	 * cgroup/memory/memory.memsw.* is only available if
	 * - CONFIG_MEMCG_SWAP_ENABLED=on in kernel config, or
	 * - swapaccount=1 in kernel cmdline.
	 *
	 * Without these interfaces the swap usage can not be limited or accounted
	 * via cgroup.
	 */
	{ perm_items_swap, true, &gp_resource_group_enable_cgroup_swap },

	/*
	 * memory permissions can be mandatory or optional depends on the switch.
	 *
	 * resgroup memory auditor is introduced in 6.0 devel and backport
	 * to 5.x branch since 5.6.1.  To provide backward compatibilities' memory
	 * permissions are optional on 5.x branch.
	 */
	{ perm_items_memory, CGROUP_MEMORY_IS_OPTIONAL,
		&gp_resource_group_enable_cgroup_memory },

	/* cpu/cpuacct permissions are mandatory */
	{ perm_items_cpu, false, NULL },
	{ perm_items_cpu_acct, false, NULL },

	/*
	 * cpuset permissions can be mandatory or optional depends on the switch.
	 *
	 * resgroup cpuset is introduced in 6.0 devel and backport
	 * to 5.x branch since 5.6.1.  To provide backward compatibilities cpuset
	 * permissions are optional on 5.x branch.
	 */
	{ perm_items_cpuset, CGROUP_CPUSET_IS_OPTIONAL,
		&gp_resource_group_enable_cgroup_cpuset},

	{ NULL, false, NULL }
};

static const char *getcgroupname_v1(void);
static bool probecgroup_v1(void);
static void checkcgroup_v1(void);
static void initcgroup_v1(void);
static void adjustgucs_v1(void);
static void createcgroup_v1(Oid group);
static void attachcgroup_v1(Oid group, int pid, bool is_cpuset_enabled);
static void detachcgroup_v1(Oid group, CGroupComponentType component, int fd_dir);
static void destroycgroup_v1(Oid group, bool migrate);
static int lockcgroup_v1(Oid group, CGroupComponentType component, bool block);
static void unlockcgroup_v1(int fd);
static void setcpulimit_v1(Oid group, int cpu_rate_limit);
static void setmemorylimitbychunks_v1(Oid group, int32 memory_limit_chunks);
static void setmemorylimit_v1(Oid group, int memory_limit);
static int64 getcpuusage_v1(Oid group);
static int32 getmemoryusage_v1(Oid group);
static int32 getmemorylimitchunks_v1(Oid group);
static void getcpuset_v1(Oid group, char *cpuset, int len);
static void setcpuset_v1(Oid group, const char *cpuset);
static float convertcpuusage_v1(int64 usage, int64 duration);

/*
 * Detect gpdb cgroup component dirs.
 *
 * Take cpu for example, by default we expect gpdb dir to locate at
 * cgroup/cpu/gpdb.  But we'll also check for the cgroup dirs of init process
 * (pid 1), e.g. cgroup/cpu/custom, then we'll look for gpdb dir at
 * cgroup/cpu/custom/gpdb, if it's found and has good permissions, it can be
 * used instead of the default one.
 *
 * If any of the gpdb cgroup component dir can not be found under init process'
 * cgroup dirs or has bad permissions we'll fallback all the gpdb cgroup
 * component dirs to the default ones.
 *
 * NOTE: This auto detection will look for memory & cpuset gpdb dirs even on
 * 5X.
 */
static void
detect_component_dirs_alpha(void)
{
	CGroupComponentType component;
	FILE	   *f;
	char		buf[MAX_CGROUP_PATHLEN * 2];
	int			maskAll = (1 << CGROUP_COMPONENT_COUNT) - 1;
	int			maskDetected = 0;

	f = fopen("/proc/1/cgroup", "r");
	if (!f)
		goto fallback;

	/*
	 * format: id:comps:path, e.g.:
	 *
	 *     10:cpuset:/
	 *     4:cpu,cpuacct:/
	 *     1:name=systemd:/init.scope
	 *     0::/init.scope
	 */
	while (fscanf(f, "%*d:%s", buf) != EOF)
	{
		CGroupComponentType components[CGROUP_COMPONENT_COUNT];
		int			ncomps = 0;
		char	   *ptr;
		char	   *tmp;
		char		sep = '\0';
		int			i;

		/* buf is stored with "comps:path" */

		if (buf[0] == ':')
			continue; /* ignore empty comp */

		/* split comps */
		for (ptr = buf; sep != ':'; ptr = tmp)
		{
			tmp = strpbrk(ptr, ":,=");

			sep = *tmp;
			*tmp++ = 0;

			/* for name=comp case there is nothing to do with the name */
			if (sep == '=')
				continue;

			component = getComponentType(ptr);

			if (component == CGROUP_COMPONENT_UNKNOWN)
				continue; /* not used by us */

			/*
			 * push the comp to the comps stack, but if the stack is already
			 * full (which is unlikely to happen in real world), simply ignore
			 * it.
			 */
			if (ncomps < CGROUP_COMPONENT_COUNT)
				components[ncomps++] = component;
		}

		/* now ptr point to the path */
		Assert(strlen(ptr) < MAX_CGROUP_PATHLEN);

		/* if the path is "/" then use empty string "" instead of it */
		if (strcmp(ptr, "/") == 0)
			ptr[0] = '\0';

		/* validate and set path for the comps */
		for (i = 0; i < ncomps; i++)
		{
			component = components[i];
			setComponentDir(component, ptr);

			if (!validateComponentDir(component))
				goto fallback; /* dir missing or bad permissions */

			if (maskDetected & (1 << component))
				goto fallback; /* comp are detected more than once */

			maskDetected |= 1 << component;
		}
	}

	if (maskDetected != maskAll)
		goto fallback; /* not all the comps are detected */

	/*
	 * Dump the comp dirs for debugging?  No!
	 * This function is executed before timezone initialization, logs are
	 * forbidden.
	 */

	fclose(f);
	return;

fallback:
	/* set the fallback dirs for all the comps */
	foreach_comp_type(component)
	{
		setComponentDir(component, FALLBACK_COMP_DIR);
	}

	if (f)
		fclose(f);
}


/*
 * Dump comp dirs.
 */
static void
dump_component_dirs_alpha(void)
{
	CGroupComponentType component;
	char		path[MAX_CGROUP_PATHLEN];
	size_t		path_size = sizeof(path);

	foreach_comp_type(component)
	{
		buildPath(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "", path, path_size);

		elog(LOG, "gpdb dir for cgroup component \"%s\": %s",
			 getComponentName(component), path);
	}
}


/*
 * Check a list of permissions on group.
 *
 * - if all the permissions are met then return true;
 * - otherwise:
 *   - raise an error if report is true and permlist is not optional;
 *   - or return false;
 */
static bool
perm_list_check_alpha(const PermList *permlist, Oid group, bool report)
{
	char path[MAX_CGROUP_PATHLEN];
	size_t path_size = sizeof(path);
	int i;

	if (group == CGROUP_ROOT_ID && permlist->presult)
		*permlist->presult = false;

	foreach_perm_item(i, permlist->items)
	{
		CGroupComponentType component = permlist->items[i].comp;
		const char	*prop = permlist->items[i].prop;
		int			perm = permlist->items[i].perm;

		if (!buildPathSafe(group, BASEDIR_GPDB, component, prop, path, path_size))
		{
			/* Buffer is not large enough for the path */

			if (report && !permlist->optional)
			{
				CGROUP_CONFIG_ERROR("invalid %s name '%s': %m",
									prop[0] ? "file" : "directory",
									path);
			}
			return false;
		}

		if (access(path, perm))
		{
			/* No such file or directory / Permission denied */

			if (report && !permlist->optional)
			{
				CGROUP_CONFIG_ERROR("can't access %s '%s': %m",
									prop[0] ? "file" : "directory",
									path);
			}
			return false;
		}
	}

	if (group == CGROUP_ROOT_ID && permlist->presult)
		*permlist->presult = true;

	return true;
}

/*
 * Check permissions on group's cgroup dir & interface files.
 *
 * - if report is true then raise an error if any mandatory permission
 *   is not met;
 */
static bool
check_permission_alpha(Oid group, bool report)
{
	int i;

	foreach_perm_list(i, permlists)
	{
		const PermList *permList = &permlists[i];

		if (!perm_list_check_alpha(permList, group, report) && !permList->optional)
			return false;
	}

	return true;
}

/*
 * Same as check_permission, just check cpuset dir & interface files.
 */
static bool
check_cpuset_permission_alpha(Oid group, bool report)
{
	if (!gp_resource_group_enable_cgroup_cpuset)
		return true;

	if (!perm_list_check_alpha(&cpusetPermList, group, report) &&
		!cpusetPermList.optional)
		return false;

	return true;
}

/*
 * Check the mount hierarchy of cpu and cpuset subsystem.
 *
 * Raise an error if cpu and cpuset are mounted on the same hierarchy.
 */
static void
check_component_hierarchy_alpha()
{
	CGroupComponentType component;
	FILE       *f;
	char        buf[MAX_CGROUP_PATHLEN * 2];
	
	f = fopen("/proc/1/cgroup", "r");
	if (!f)
	{
		CGROUP_CONFIG_ERROR("can't check component mount hierarchy \
					file '/proc/1/cgroup' doesn't exist");
		return;
	}

	/*
	 * format: id:comps:path, e.g.:
	 *
	 * 10:cpuset:/
	 * 4:cpu,cpuacct:/
	 * 1:name=systemd:/init.scope
	 * 0::/init.scope
	 */
	while (fscanf(f, "%*d:%s", buf) != EOF)
	{
		char       *ptr;
		char       *tmp;
		char        sep = '\0';
		/* mark if the line has already contained cpu or cpuset component */
		int        markComp = CGROUP_COMPONENT_UNKNOWN;

		/* buf is stored with "comps:path" */
		if (buf[0] == ':')
			continue; /* ignore empty comp */

		/* split comps */
		for (ptr = buf; sep != ':'; ptr = tmp)
		{
			tmp = strpbrk(ptr, ":,=");
			
			sep = *tmp;
			*tmp++ = 0;

			/* for name=comp case there is nothing to do with the name */
			if (sep == '=')
				continue;

			component = getComponentType(ptr);

			if (component == CGROUP_COMPONENT_UNKNOWN)
				continue; /* not used by us */
			
			if (component == CGROUP_COMPONENT_CPU || component == CGROUP_COMPONENT_CPUSET)
			{
				if (markComp == CGROUP_COMPONENT_UNKNOWN)
					markComp = component;
				else
				{
					Assert(markComp != component);
					fclose(f);
					CGROUP_CONFIG_ERROR("can't mount 'cpu' and 'cpuset' on the same hierarchy");
					return;
				}
			}
		}
	}

	fclose(f);
}

/*
 * Init gpdb cpu settings.
 */
static void
init_cpu_alpha(void)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPU;
	int64		cfs_quota_us;
	int64		shares;

	/*
	 * CGroup promises that cfs_quota_us will never be 0, however on centos6
	 * we ever noticed that it has the value 0.
	 */
	if (parent_cfs_quota_us <= 0LL)
	{
		/*
		 * parent cgroup is unlimited, calculate gpdb's limitation based on
		 * system hardware configuration.
		 *
		 * cfs_quota_us := parent.cfs_period_us * ncores * gp_resource_group_cpu_limit
		 */
		cfs_quota_us = system_cfs_quota_us * gp_resource_group_cpu_limit;
	}
	else
	{
		/*
		 * parent cgroup is also limited, then calculate gpdb's limitation
		 * based on it.
		 *
		 * cfs_quota_us := parent.cfs_quota_us * gp_resource_group_cpu_limit
		 */
		cfs_quota_us = parent_cfs_quota_us * gp_resource_group_cpu_limit;
	}

	writeInt64(CGROUP_ROOT_ID, BASEDIR_GPDB,
			   component, "cpu.cfs_quota_us", cfs_quota_us);

	/*
	 * shares := parent.shares * gp_resource_group_cpu_priority
	 *
	 * We used to set a large shares (like 1024 * 50, the maximum possible
	 * value), it has very bad effect on overall system performance,
	 * especially on 1-core or 2-core low-end systems.
	 */
	shares = readInt64(CGROUP_ROOT_ID, BASEDIR_PARENT, component, "cpu.shares");
	shares = shares * gp_resource_group_cpu_priority;

	writeInt64(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpu.shares", shares);
}

/*
 * Init gpdb cpuset settings.
 */
static void
init_cpuset_alpha(void)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPUSET;
	char		buffer[MaxCpuSetLength];

	if (!gp_resource_group_enable_cgroup_cpuset)
		return;

	/*
	 * Get cpuset.mems and cpuset.cpus values from cgroup cpuset root path,
	 * and set them to cpuset/gpdb/cpuset.mems and cpuset/gpdb/cpuset.cpus
	 * to make sure that gpdb directory configuration is same as its
	 * parent directory
	 */

	readStr(CGROUP_ROOT_ID, BASEDIR_PARENT, component, "cpuset.mems",
			buffer, sizeof(buffer));
	writeStr(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpuset.mems", buffer);

	readStr(CGROUP_ROOT_ID, BASEDIR_PARENT, component, "cpuset.cpus",
			buffer, sizeof(buffer));
	writeStr(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpuset.cpus", buffer);

	create_default_cpuset_group_alpha();
}

static int64
get_cfs_period_us_alpha(CGroupComponentType component)
{
	int64		cfs_period_us;

	/*
	 * calculate cpu rate limit of system.
	 *
	 * Ideally the cpu quota is calculated from parent information:
	 *
	 * system_cfs_quota_us := parent.cfs_period_us * ncores.
	 *
	 * However, on centos6 we found parent.cfs_period_us can be 0 and is not
	 * writable.  In the other side, gpdb.cfs_period_us should be equal to
	 * parent.cfs_period_us because sub dirs inherit parent properties by
	 * default, so we read it instead.
	 */
	cfs_period_us = readInt64(CGROUP_ROOT_ID, BASEDIR_GPDB,
							  component, "cpu.cfs_period_us");

	if (cfs_period_us == 0LL)
	{
		/*
		 * if gpdb.cfs_period_us is also 0 try to correct it by setting the
		 * default value 100000 (100ms).
		 */
		writeInt64(CGROUP_ROOT_ID, BASEDIR_GPDB,
				   component, "cpu.cfs_period_us", DEFAULT_CPU_PERIOD_US);

		/* read again to verify the effect */
		cfs_period_us = readInt64(CGROUP_ROOT_ID, BASEDIR_GPDB,
								  component, "cpu.cfs_period_us");

		if (cfs_period_us <= 0LL)
			CGROUP_CONFIG_ERROR("invalid cpu.cfs_period_us value: "
								INT64_FORMAT,
								cfs_period_us);
	}

	return cfs_period_us;
}

/* Return the name for the OS group implementation */
static const char *
getcgroupname_v1(void)
{
	return "cgroup";
}

/*
 * Probe the configuration for the OS group implementation.
 *
 * Return true if everything is OK, or false is some requirements are not
 * satisfied.
 */
static bool
probecgroup_v1(void)
{
	/*
	 * Ignore the error even if cgroup mount point can not be successfully
	 * probed, the error will be reported in checkcgroup() later.
	 */
	if (!getCgroupMountDir())
		return false;

	detect_component_dirs_alpha();

	if (!check_permission_alpha(CGROUP_ROOT_ID, false))
		return false;

	return true;
}

/* Check whether the OS group implementation is available and usable */
static void
checkcgroup_v1(void)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPU;
	int64		cfs_period_us;

	/*
	 * We only have to do these checks and initialization once on each host,
	 * so only let postmaster do the job.
	 */
	Assert(!IsUnderPostmaster);

	/*
	 * We should have already detected for cgroup mount point in probecgroup(),
	 * it was not an error if the detection failed at that step.  But once
	 * we call checkcgroup() we know we want to make use of cgroup then we must
	 * know the mount point, otherwise it's a critical error.
	 */
	if (!cgroupSystemInfoAlpha.cgroup_dir[0])
		CGROUP_CONFIG_ERROR("can not find cgroup mount point");

	/*
	 * Check again, this time we will fail on unmet requirements.
	 */
	check_permission_alpha(CGROUP_ROOT_ID, true);

	/*
 	 * Check if cpu and cpuset subsystems are mounted on the same hierarchy.
 	 * We do not allow they mount on the same hierarchy, because writing pid
 	 * to DEFAULT_CPUSET_GROUP_ID in attachcgroup will cause the
 	 * removal of the pid in group BASEDIR_GPDB, which will make cpu usage
 	 * out of control.
	 */
	if (!CGROUP_CPUSET_IS_OPTIONAL)
		check_component_hierarchy_alpha();

	/*
	 * Dump the cgroup comp dirs to logs.
	 * Check detect_component_dirs() to know why this is not done in that function.
	 */
	dump_component_dirs_alpha();

	/*
	 * Get some necessary system information.
	 * We can not do them in probecgroup() as failure is not allowed in that one.
	 */

	/* get system cpu cores */
	cgroupSystemInfoAlpha.ncores = getCPUCores();

	cfs_period_us = get_cfs_period_us_alpha(component);
	system_cfs_quota_us = cfs_period_us * cgroupSystemInfoAlpha.ncores;

	/* read cpu rate limit of parent cgroup */
	parent_cfs_quota_us = readInt64(CGROUP_ROOT_ID, BASEDIR_PARENT,
									component, "cpu.cfs_quota_us");
}

/* Initialize the OS group */
static void
initcgroup_v1(void)
{
	init_cpu_alpha();
	init_cpuset_alpha();

	/* 
	 * After basic controller inited, we need to create the SYSTEM CGROUP
	 * which will control the postmaster and auxiliary process, such as
	 * BgWriter, SysLogger.
	 *
	 * We need to add it to the system cgroup before the postmaster fork
	 * the child process to limit the resource usage of the parent process
	 * and all child processes.
	 */
	createcgroup_v1(GPDB_SYSTEM_CGROUP);
	attachcgroup_v1(GPDB_SYSTEM_CGROUP, PostmasterPid, false);
}

/* Adjust GUCs for this OS group implementation */
static void
adjustgucs_v1(void)
{
	/*
	 * cgroup cpu limitation works best when all processes have equal
	 * priorities, so we force all the segments and postmaster to
	 * work with nice=0.
	 *
	 * this function should be called before GUCs are dispatched to segments.
	 */
	gp_segworker_relative_priority = 0;
}

/*
 * Create the OS group for group.
 */
static void
createcgroup_v1(Oid group)
{
	int retry = 0;

	if (!createDir(group, CGROUP_COMPONENT_CPU) ||
		!createDir(group, CGROUP_COMPONENT_CPUACCT) ||
		(gp_resource_group_enable_cgroup_memory &&
		!createDir(group, CGROUP_COMPONENT_MEMORY)) ||
		(gp_resource_group_enable_cgroup_cpuset &&
		 !createDir(group, CGROUP_COMPONENT_CPUSET)))
	{
		CGROUP_ERROR("can't create cgroup for resource group '%d': %m", group);
	}

	/*
	 * although the group dir is created the interface files may not be
	 * created yet, so we check them repeatedly until everything is ready.
	 */
	while (++retry <= MAX_RETRY && !check_permission_alpha(group, false))
		pg_usleep(1000);

	if (retry > MAX_RETRY)
	{
		/*
		 * still not ready after MAX_RETRY retries, might be a real error,
		 * raise the error.
		 */
		check_permission_alpha(group, true);
	}

	if (gp_resource_group_enable_cgroup_cpuset)
	{
		/*
		 * Initialize cpuset.mems and cpuset.cpus values as its parent directory
		 */
		CGroupComponentType component = CGROUP_COMPONENT_CPUSET;
		char buffer[MaxCpuSetLength];

		readStr(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpuset.mems",
				buffer, sizeof(buffer));
		writeStr(group, BASEDIR_GPDB, component, "cpuset.mems", buffer);

		readStr(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpuset.cpus",
				buffer, sizeof(buffer));
		writeStr(group, BASEDIR_GPDB, component, "cpuset.cpus", buffer);
	}
}

/*
 * Create the OS group for default cpuset group.
 * default cpuset group is a special group, only take effect in cpuset
 */
static void
create_default_cpuset_group_alpha(void)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPUSET;
	int retry = 0;

	if (!createDir(DEFAULT_CPUSET_GROUP_ID, component))
	{
		CGROUP_ERROR("can't create cpuset cgroup for resgroup '%d': %m",
					 DEFAULT_CPUSET_GROUP_ID);
	}

	/*
	 * although the group dir is created the interface files may not be
	 * created yet, so we check them repeatedly until everything is ready.
	 */
	while (++retry <= MAX_RETRY &&
		   !check_cpuset_permission_alpha(DEFAULT_CPUSET_GROUP_ID, false))
		pg_usleep(1000);

	if (retry > MAX_RETRY)
	{
		/*
		 * still not ready after MAX_RETRY retries, might be a real error,
		 * raise the error.
		 */
		check_cpuset_permission_alpha(DEFAULT_CPUSET_GROUP_ID, true);
	}

	/*
	 * Initialize cpuset.mems and cpuset.cpus in default group as its
	 * parent directory
	 */
	char buffer[MaxCpuSetLength];

	readStr(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpuset.mems",
			buffer, sizeof(buffer));
	writeStr(DEFAULT_CPUSET_GROUP_ID, BASEDIR_GPDB, component, "cpuset.mems", buffer);

	readStr(CGROUP_ROOT_ID, BASEDIR_GPDB, component, "cpuset.cpus",
			buffer, sizeof(buffer));
	writeStr(DEFAULT_CPUSET_GROUP_ID, BASEDIR_GPDB, component, "cpuset.cpus", buffer);
}


/*
 * Assign a process to the OS group. A process can only be assigned to one
 * OS group, if it's already running under other OS group then it'll be moved
 * out that OS group.
 *
 * pid is the process id.
 */
static void
attachcgroup_v1(Oid group, int pid, bool is_cpuset_enabled)
{
	/*
	 * needn't write to file if the pid has already been written in.
	 * Unless it has not been written or the group has changed or
	 * cpu control mechanism has changed.
	 */
	if (IsUnderPostmaster && group == currentGroupIdInCGroup)
		return;

	writeInt64(group, BASEDIR_GPDB, CGROUP_COMPONENT_CPU,
			   "cgroup.procs", pid);
	writeInt64(group, BASEDIR_GPDB, CGROUP_COMPONENT_CPUACCT,
			   "cgroup.procs", pid);

	if (gp_resource_group_enable_cgroup_cpuset)
	{
		if (is_cpuset_enabled)
		{
			writeInt64(group, BASEDIR_GPDB,
					   CGROUP_COMPONENT_CPUSET, "cgroup.procs", pid);
		}
		else
		{
			/* add pid to default group */
			writeInt64(DEFAULT_CPUSET_GROUP_ID, BASEDIR_GPDB,
					   CGROUP_COMPONENT_CPUSET, "cgroup.procs", pid);
		}
	}

	/*
	 * Do not assign the process to cgroup/memory for now.
	 */

	currentGroupIdInCGroup = group;
}


/*
 * un-assign all the processes from a cgroup.
 *
 * These processes will be moved to the gpdb default cgroup.
 *
 * This function must be called with the gpdb toplevel dir locked,
 * fd_dir is the fd for this lock, on any failure fd_dir will be closed
 * (and unlocked implicitly) then an error is raised.
 */
static void
detachcgroup_v1(Oid group, CGroupComponentType component, int fd_dir)
{
	char 	path[MAX_CGROUP_PATHLEN];
	size_t 	path_size = sizeof(path);

	char 	*buf;
	size_t 	buf_size;
	size_t 	buf_len = -1;

	int fdr = -1;
	int fdw = -1;

	const size_t buf_delta_size = 512;

	/*
	 * Check an operation result on path.
	 *
	 * Operation can be open(), close(), read(), write(), etc., which must
	 * set the errno on error.
	 *
	 * - condition describes the expected result of the operation;
	 * - action is the cleanup action on failure, such as closing the fd,
	 *   multiple actions can be specified by putting them in brackets,
	 *   such as (op1, op2);
	 * - message describes what's failed;
	 */
#define __CHECK(condition, action, message) do { \
	if (!(condition)) \
	{ \
		/* save errno in case it's changed in actions */ \
		int err = errno; \
		action; \
		CGROUP_ERROR(message ": %s: %s", path, strerror(err)); \
	} \
} while (0)

	buildPath(group, BASEDIR_GPDB, component, "cgroup.procs", path, path_size);

	fdr = open(path, O_RDONLY);

	__CHECK(fdr >= 0, ( close(fd_dir) ), "can't open file for read");

	buf_len = 0;
	buf_size = buf_delta_size;
	buf = palloc(buf_size);

	while (1)
	{
		int n = read(fdr, buf + buf_len, buf_delta_size);
		__CHECK(n >= 0, ( close(fdr), close(fd_dir) ), "can't read from file");

		buf_len += n;

		if (n < buf_delta_size)
			break;

		buf_size += buf_delta_size;
		buf = repalloc(buf, buf_size);
	}

	close(fdr);
	if (buf_len == 0)
		return;

	buildPath(GPDB_DEFAULT_CGROUP, BASEDIR_GPDB, component, "cgroup.procs",
			  path, path_size);

	fdw = open(path, O_WRONLY);
	__CHECK(fdw >= 0, ( close(fd_dir) ), "can't open file for write");

	char *ptr = buf;
	char *end = NULL;
	long pid;

	/*
	 * as required by cgroup, only one pid can be migrated in each single
	 * write() call, so we have to parse the pids from the buffer first,
	 * then write them one by one.
	 */
	while (1)
	{
		pid = strtol(ptr, &end, 10);
		__CHECK(pid != LONG_MIN && pid != LONG_MAX,
				( close(fdw), close(fd_dir) ),
				"can't parse pid");

		if (ptr == end)
			break;

		char str[22];
		sprintf(str, "%ld", pid);
		int n = write(fdw, str, strlen(str));
		if (n < 0)
		{
			elog(LOG, "failed to migrate pid to gpdb root cgroup: pid=%ld: %m",
				 pid);
		}
		else
		{
			__CHECK(n == strlen(str),
					( close(fdw), close(fd_dir) ),
					"can't write to file");
		}

		ptr = end;
	}

	close(fdw);

#undef __CHECK
}


/*
 * Destroy the OS cgroup.
 *
 * One OS group can not be dropped if there are processes running under it,
 * if migrate is true these processes will be moved out automatically.
 */
static void
destroycgroup_v1(Oid group, bool migrate)
{
	if (!deleteDir(group, CGROUP_COMPONENT_CPU, "cpu.shares", migrate, detachcgroup_v1) ||
		!deleteDir(group, CGROUP_COMPONENT_CPUACCT, NULL, migrate, detachcgroup_v1) ||
		(gp_resource_group_enable_cgroup_cpuset &&
		 !deleteDir(group, CGROUP_COMPONENT_CPUSET, NULL, migrate, detachcgroup_v1)) ||
		(gp_resource_group_enable_cgroup_memory &&
		 !deleteDir(group, CGROUP_COMPONENT_MEMORY, "memory.limit_in_bytes", migrate, detachcgroup_v1)))
	{
		CGROUP_ERROR("can't remove cgroup for resource group '%d': %m", group);
	}
}


/*
 * Lock the OS group. While the group is locked it won't be removed by other
 * processes.
 *
 * This function would block if block is true, otherwise it returns with -1
 * immediately.
 *
 * On success, it returns a fd to the OS group, pass it to unlockcgroup_v1()
 * to unlock it.
 */
static int
lockcgroup_v1(Oid group, CGroupComponentType component, bool block)
{
	char path[MAX_CGROUP_PATHLEN];
	size_t path_size = sizeof(path);

	buildPath(group, BASEDIR_GPDB, component, "", path, path_size);

	return lockDir(path, block);
}

/*
 * Unblock an OS group.
 *
 * fd is the value returned by lockcgroup_v1().
 */
static void
unlockcgroup_v1(int fd)
{
	if (fd >= 0)
		close(fd);
}

/*
 * Set the cpu rate limit for the OS group.
 *
 * cpu_rate_limit should be within [0, 100].
 */
static void
setcpulimit_v1(Oid group, int cpu_rate_limit)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPU;

	/* group.shares := gpdb.shares * cpu_rate_limit */

	int64 shares = readInt64(CGROUP_ROOT_ID, BASEDIR_GPDB, component,
							 "cpu.shares");
	writeInt64(group, BASEDIR_GPDB, component,
			   "cpu.shares", shares * cpu_rate_limit / 100);

	/* set cpu.cfs_quota_us if hard CPU enforcement is enabled */
	if (gp_resource_group_cpu_ceiling_enforcement)
	{
		int64 periods = get_cfs_period_us_alpha(component);
		writeInt64(group, BASEDIR_GPDB, component, "cpu.cfs_quota_us",
				   periods * cgroupSystemInfoAlpha.ncores * cpu_rate_limit / 100);
	}
	else
	{
		writeInt64(group, BASEDIR_GPDB, component, "cpu.cfs_quota_us", -1);
	}
}


/*
 * Set the memory limit for the OS group by value.
 *
 * memory_limit is the limit value in chunks
 *
 * If cgroup supports memory swap, we will write the same limit to
 * memory.memsw.limit and memory.limit.
 */
static void
setmemorylimitbychunks_v1(Oid group, int32 memory_limit_chunks)
{
	CGroupComponentType component = CGROUP_COMPONENT_MEMORY;
	int64 memory_limit_in_bytes;

	if (!gp_resource_group_enable_cgroup_memory)
		return;

	memory_limit_in_bytes = VmemTracker_ConvertVmemChunksToBytes(memory_limit_chunks);

	/* Is swap interfaces enabled? */
	if (!gp_resource_group_enable_cgroup_swap)
	{
		/* No, then we only need to setup the memory limit */
		writeInt64(group, BASEDIR_GPDB, component, "memory.limit_in_bytes",
				   memory_limit_in_bytes);
	}
	else
	{
		/* Yes, then we have to setup both the memory and mem+swap limits */

		int64 memory_limit_in_bytes_old;

		/*
		 * Memory limit should always <= mem+swap limit, then the limits
		 * must be set in a proper order depending on the relation between
		 * new and old limits.
		 */
		memory_limit_in_bytes_old = readInt64(group, BASEDIR_GPDB, component,
											  "memory.limit_in_bytes");

		if (memory_limit_in_bytes > memory_limit_in_bytes_old)
		{
			/* When new value > old memory limit, write mem+swap limit first */
			writeInt64(group, BASEDIR_GPDB, component,
					   "memory.memsw.limit_in_bytes", memory_limit_in_bytes);
			writeInt64(group, BASEDIR_GPDB, component,
					   "memory.limit_in_bytes", memory_limit_in_bytes);
		}
		else if (memory_limit_in_bytes < memory_limit_in_bytes_old)
		{
			/* When new value < old memory limit,  write memory limit first */
			writeInt64(group, BASEDIR_GPDB, component,
			"memory.limit_in_bytes", memory_limit_in_bytes);
			writeInt64(group, BASEDIR_GPDB, component,
					   "memory.memsw.limit_in_bytes", memory_limit_in_bytes);
		}
	}
}

/*
 * Set the memory limit for the OS group by rate.
 *
 * memory_limit should be within [0, 100].
 */
static void
setmemorylimit_v1(Oid group, int memory_limit)
{
	CGroupComponentType component = CGROUP_COMPONENT_MEMORY;
	int fd;
	int32 memory_limit_in_chunks;

	memory_limit_in_chunks = ResGroupGetVmemLimitChunks() * memory_limit / 100;
	memory_limit_in_chunks *= ResGroupGetHostPrimaryCount();

	fd = lockcgroup_v1(group, component, true);
	setmemorylimitbychunks_v1(group, memory_limit_in_chunks);
	unlockcgroup_v1(fd);
}


/*
 * Get the cpu usage of the OS group, that is the total cpu time obtained
 * by this OS group, in nano seconds.
 */
static int64
getcpuusage_v1(Oid group)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPUACCT;

	return readInt64(group, BASEDIR_GPDB, component, "cpuacct.usage");
}

/* get cgroup ram and swap (in Byte) */
static void
get_cgroup_memory_info(uint64 *cgram, uint64 *cgmemsw)
{
	CGroupComponentType component = CGROUP_COMPONENT_MEMORY;

	*cgram = readInt64(CGROUP_ROOT_ID, BASEDIR_PARENT,
					   component, "memory.limit_in_bytes");

	if (gp_resource_group_enable_cgroup_swap)
	{
		*cgmemsw = readInt64(CGROUP_ROOT_ID, BASEDIR_PARENT,
							 component, "memory.memsw.limit_in_bytes");
	}
	else
	{
		elog(DEBUG1, "swap memory is unlimited");
		*cgmemsw = (uint64) -1LL;
	}
}

/* get total ram and total swap (in Byte) from sysinfo */
static void
get_memory_info(unsigned long *ram, unsigned long *swap)
{
	struct sysinfo info;
	if (sysinfo(&info) < 0)
		elog(ERROR, "can't get memory information: %m");
	*ram = info.totalram;
	*swap = info.totalswap;
}

/* get vm.overcommit_ratio */
static int
getOvercommitRatio(void)
{
	int ratio;
	char data[MAX_INT_STRING_LEN];
	size_t datasize = sizeof(data);
	const char *path = "/proc/sys/vm/overcommit_ratio";

	readData(path, data, datasize);

	if (sscanf(data, "%d", &ratio) != 1)
		elog(ERROR, "invalid number '%s' in '%s'", data, path);

	return ratio;
}

static int
gettotalmemory_v1(void)
{
	unsigned long ram, swap, total;
	int overcommitRatio;
	uint64 cgram, cgmemsw;
	uint64 memsw;
	uint64 outTotal;

	overcommitRatio = getOvercommitRatio();
	get_memory_info(&ram, &swap);
	/* Get sysinfo total ram and swap size. */
	memsw = ram + swap;
	outTotal = swap + ram * overcommitRatio / 100;
	get_cgroup_memory_info(&cgram, &cgmemsw);
	ram = Min(ram, cgram);
	/*
	 * In the case that total ram and swap read from sysinfo is larger than
	 * from cgroup, ram and swap must both be limited, otherwise swap must
	 * not be limited(we can safely use the value from sysinfo as swap size).
	 */
	if (cgmemsw < memsw)
		swap = cgmemsw - ram;
	/*
	 * If it is in container, the total memory is limited by both the total
	 * memoery outside and the memsw of the container.
	 */
	total = Min(outTotal, swap + ram);
	return total >> BITS_IN_MB;
}

/*
 * Get the memory usage of the OS group
 *
 * memory usage is returned in chunks
 */
static int32
getmemoryusage_v1(Oid group)
{
	CGroupComponentType component = CGROUP_COMPONENT_MEMORY;
	int64 	memory_usage_in_bytes;
	char 	*filename;

	/* Report 0 if cgroup memory is not enabled */
	if (!gp_resource_group_enable_cgroup_memory)
		return 0;

	filename = gp_resource_group_enable_cgroup_swap
		? "memory.memsw.usage_in_bytes"
		: "memory.usage_in_bytes";

	memory_usage_in_bytes = readInt64(group, BASEDIR_GPDB, component, filename);

	return VmemTracker_ConvertVmemBytesToChunks(memory_usage_in_bytes);
}

/*
 * Get the memory limit of the OS group
 *
 * memory limit is returned in chunks
 */
static int32
getmemorylimitchunks_v1(Oid group)
{
	CGroupComponentType component = CGROUP_COMPONENT_MEMORY;
	int64 memory_limit_in_bytes;

	/* Report unlimited (max int32) if cgroup memory is not enabled */
	if (!gp_resource_group_enable_cgroup_memory)
		return (int32) ((1U << 31) - 1);

	memory_limit_in_bytes = readInt64(group, BASEDIR_GPDB,
									  component, "memory.limit_in_bytes");

	return VmemTracker_ConvertVmemBytesToChunks(memory_limit_in_bytes);
}


/*
 * Get the cpuset of the OS group.
 * @param group: the destination group
 * @param cpuset: the str to be set
 * @param len: the upper limit of the str
 */
static void
getcpuset_v1(Oid group, char *cpuset, int len)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPUSET;

	if (!gp_resource_group_enable_cgroup_cpuset)
		return ;

	readStr(group, BASEDIR_GPDB, component, "cpuset.cpus", cpuset, len);
}


/*
 * Set the cpuset for the OS group.
 * @param group: the destination group
 * @param cpuset: the value to be set
 * The syntax of CPUSET is a combination of the tuples, each tuple represents
 * one core number or the core numbers interval, separated by comma.
 * E.g. 0,1,2-3.
 */
static void
setcpuset_v1(Oid group, const char *cpuset)
{
	CGroupComponentType component = CGROUP_COMPONENT_CPUSET;

	if (!gp_resource_group_enable_cgroup_cpuset)
		return ;

	writeStr(group, BASEDIR_GPDB, component, "cpuset.cpus", cpuset);
}


/*
 * Convert the cpu usage to percentage within the duration.
 *
 * usage is the delta of getcpuusage() of a duration,
 * duration is in micro seconds.
 *
 * When fully consuming one cpu core the return value will be 100.0 .
 */
static float
convertcpuusage_v1(int64 usage, int64 duration)
{
	float		percent;

	Assert(usage >= 0LL);
	Assert(duration > 0LL);

	/* There should always be at least one core on the system */
	Assert(cgroupSystemInfoAlpha.ncores > 0);

	/*
	 * Usage is the cpu time (nano seconds) obtained by this group in the time
	 * duration (micro seconds), so cpu time on one core can be calculated as:
	 *
	 *     usage / 1000 / duration / ncores
	 *
	 * To convert it to percentage we should multiple 100%:
	 *
	 *     usage / 1000 / duration / ncores * 100%
	 *   = usage / 10 / duration / ncores
	 */
	percent = usage / 10.0 / duration / cgroupSystemInfoAlpha.ncores;

	/*
	 * Now we have the system level percentage, however when running in a
	 * container with limited cpu quota we need to further scale it with
	 * parent.  Suppose parent has 50% cpu quota and gpdb is consuming all of
	 * it, then we want gpdb to report the cpu usage as 100% instead of 50%.
	 */

	if (parent_cfs_quota_us > 0LL)
	{
		/*
		 * Parent cgroup is also limited, scale the percentage to the one in
		 * parent cgroup.  Do not change the expression to `percent *= ...`,
		 * that will lose the precision.
		 */
		percent = percent * system_cfs_quota_us / parent_cfs_quota_us;
	}

	return percent;
}

static CGroupOpsRoutine cGroupOpsRoutineAlpha = {
		.getcgroupname = getcgroupname_v1,
		.probecgroup = probecgroup_v1,
		.checkcgroup = checkcgroup_v1,
		.initcgroup = initcgroup_v1,
		.adjustgucs = adjustgucs_v1,
		.createcgroup = createcgroup_v1,
		.destroycgroup = destroycgroup_v1,

		.attachcgroup = attachcgroup_v1,
		.detachcgroup = detachcgroup_v1,

		.lockcgroup = lockcgroup_v1,
		.unlockcgroup = unlockcgroup_v1,

		.setcpulimit = setcpulimit_v1,
		.getcpuusage = getcpuusage_v1,
		.getcpuset = getcpuset_v1,
		.setcpuset = setcpuset_v1,

		.gettotalmemory = gettotalmemory_v1,
		.getmemoryusage = getmemoryusage_v1,
		.setmemorylimit = setmemorylimit_v1,
		.getmemorylimitchunks = getmemorylimitchunks_v1,
		.setmemorylimitbychunks = setmemorylimitbychunks_v1,

		.convertcpuusage = convertcpuusage_v1,
};

CGroupOpsRoutine *get_group_routine_alpha(void)
{
	return &cGroupOpsRoutineAlpha;
}

CGroupSystemInfo *get_cgroup_sysinfo_alpha(void)
{
	return &cgroupSystemInfoAlpha;
}
