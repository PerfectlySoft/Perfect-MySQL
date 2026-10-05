#include <mysql.h>

#if defined(LIBMARIADB) || defined(MARIADB_BASE_VERSION)
// MariaDB Connector/C (also what Debian ships as "mysqlclient"). MariaDB already defines my_bool.
#define PERFECT_MYSQL_LIBMARIADB 1

// libmariadb has no MYSQL_OPT_SSL_MODE. Give the name a value libmariadb doesn't recognise, so
// passing it straight to mysql_options fails instead of breaking the build; MySQL.setOption
// routes it through perfect_mysql_set_ssl_mode below instead.
static const enum mysql_option MYSQL_OPT_SSL_MODE = (enum mysql_option)0x7FFF;

// MySQL's enum mysql_ssl_mode, so code written against libmysqlclient builds here too.
enum mysql_ssl_mode {
  SSL_MODE_DISABLED = 1,
  SSL_MODE_PREFERRED,
  SSL_MODE_REQUIRED,
  SSL_MODE_VERIFY_CA,
  SSL_MODE_VERIFY_IDENTITY
};

// MYSQL_OPT_SSL_MODE in terms of libmariadb's options. Returns 0 on success, like mysql_options.
// - DISABLED: no TLS. (libmariadb still uses TLS if MYSQL_OPT_SSL_CA, _CERT, _KEY, _CAPATH or
//   _CIPHER is set.)
// - PREFERRED, REQUIRED: TLS without verifying the certificate. libmariadb quietly falls back to
//   plaintext when the server has no TLS, so MySQL.connect checks REQUIRED itself (only after
//   authenticating; the VERIFY modes fail before that).
// - VERIFY_CA, VERIFY_IDENTITY: TLS with the certificate verified. libmariadb has one switch for
//   both and also checks the host name (except on local connections, from 3.4), so VERIFY_CA is
//   stricter than libmysqlclient's.
static inline int perfect_mysql_set_ssl_mode(MYSQL *mysql, unsigned int mode) {
  my_bool enforce, verify;
  switch (mode) {
  case SSL_MODE_DISABLED:
    enforce = 0; verify = 0;
    break;
  case SSL_MODE_PREFERRED:
  case SSL_MODE_REQUIRED:
    enforce = 1; verify = 0;
    break;
  case SSL_MODE_VERIFY_CA:
  case SSL_MODE_VERIFY_IDENTITY:
    enforce = 1; verify = 1;
    break;
  default:
    return 1;
  }
  if (mysql_options(mysql, MYSQL_OPT_SSL_ENFORCE, &enforce))
    return 1;
  return mysql_options(mysql, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &verify);
}
#else
#define PERFECT_MYSQL_LIBMARIADB 0

#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 80000
// MySQL 8.0 removed the my_bool typedef (MySQL 5.7 still defines it). `#ifndef my_bool`
// can't detect a typedef, so key off the version instead.
typedef signed char my_bool;
#endif

// libmysqlclient doesn't reject an unknown mode itself.
static inline int perfect_mysql_set_ssl_mode(MYSQL *mysql, unsigned int mode) {
  if (mode < SSL_MODE_DISABLED || mode > SSL_MODE_VERIFY_IDENTITY)
    return 1;
  return mysql_options(mysql, MYSQL_OPT_SSL_MODE, &mode);
}
#endif
