#include <mysql.h>
// MySQL 8.0+ removed the my_bool typedef; provide a compat shim so existing
// code that uses my_bool as a byte-sized nullable flag continues to work.
#ifndef my_bool
typedef signed char my_bool;
#endif
