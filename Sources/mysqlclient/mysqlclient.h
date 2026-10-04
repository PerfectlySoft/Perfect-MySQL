#include <mysql.h>

#if defined(LIBMARIADB) || defined(MARIADB_BASE_VERSION)
// MariaDB Connector/C (also what Debian ships as "mysqlclient"). It has no MYSQL_OPT_SSL_MODE,
// so give that name a value libmariadb doesn't recognise: setting it then fails at runtime
// (MySQL.setOption returns false, error 2054) instead of breaking the build. Callers that need
// TLS against MariaDB should check that return value; MariaDB's own control is
// MYSQL_OPT_SSL_ENFORCE. MariaDB already defines my_bool.
static const enum mysql_option MYSQL_OPT_SSL_MODE = (enum mysql_option)0x7FFF;
#elif defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 80000
// MySQL 8.0 removed the my_bool typedef (MySQL 5.7 still defines it). `#ifndef my_bool`
// can't detect a typedef, so key off the version instead.
typedef signed char my_bool;
#endif
