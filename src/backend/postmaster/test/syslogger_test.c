#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"

#include "../syslogger.c"

static void 
test__logfile_getname(void **state)
{
    char *alert_file_name;

    alert_file_pattern = "alert_log";

	log_timezone = pg_tzset("GMT");

	alert_file_name = logfile_getname((pg_time_t) 12345, NULL, "logdir", "alert_log-%F");
	assert_true(strcmp(alert_file_name, "logdir/alert_log-1970-01-01") == 0);
}

int
main(int argc, char* argv[]) {
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = {    		unit_test(test__logfile_getname)
    };

	MemoryContextInit();

    return run_tests(tests);
}
