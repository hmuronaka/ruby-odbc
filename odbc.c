/*
 * ODBC-Ruby binding
 * Copyright (C) 2001 Christian Werner
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * $Id: odbc.c,v 1.10 2001/06/19 19:37:55 chw Exp chw $
 */

#if defined(_WIN32) || defined(__CYGWIN32__) || defined(__MINGW32__)
#include <windows.h>
#endif
#include "ruby.h"
#ifdef HAVE_SQL_H
#include <sql.h>
#else
#error Missing include: sql.h
#endif
#ifdef HAVE_SQLEXT_H
#include <sqlext.h>
#else
#error Missing include: sqlext.h
#endif
#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#else
#error Missing include: odbcinst.h
#endif

typedef struct {
    VALUE dbcs;
    SQLHENV henv;
} ENV;

typedef struct {
    VALUE self;
    VALUE env;
    VALUE stmts;
    SQLHDBC hdbc;
} DBC;

typedef struct {
    SQLSMALLINT type;
    SQLUINTEGER coldef;
    SQLSMALLINT scale;
    SQLINTEGER rlen;
    SQLSMALLINT nullable;
    char buffer[32];
} PINFO;

typedef struct {
    int type;
    int size;
} COLTYPE;

typedef struct stmt {
    VALUE self;
    VALUE dbc;
    SQLHSTMT hstmt;
    int nump;
    PINFO *pinfo;
    int ncols;
    COLTYPE *coltypes;
    char **colnames;
    char **dbufs;
} STMT;

static VALUE Modbc;
static VALUE Cobj;
static VALUE Cenv;
static VALUE Cdbc;
static VALUE Cstmt;
static VALUE Ccolumn;
static VALUE Cerror;
static VALUE Cdsn;
static VALUE Cdrv;
static VALUE Cdate;
static VALUE Ctime;
static VALUE Ctimestamp;
static VALUE Cproc;
static VALUE rb_cDate;

/*
 * Modes for dbc_info
 */

#define INFO_TABLES   0
#define INFO_COLUMNS  1
#define INFO_PRIMKEYS 2
#define INFO_INDEXES  3
#define INFO_TYPES    4
#define INFO_FORKEYS  5
#define INFO_TPRIV    6
#define INFO_PROCS    7
#define INFO_PROCCOLS 8

/*
 * Modes for make_result
 */

#define MAKERES_BLOCK   1
#define MAKERES_NOCLOSE 2
#define MAKERES_PREPARE 4

/*
 * Modes for do_fetch
 */

#define DOFETCH_ARY   0
#define DOFETCH_HASH  1
#define DOFETCH_HASH2 2
#define DOFETCH_MODES 3
#define DOFETCH_BANG  8

/*
 * Size of segment when SQL_NO_TOTAL
 */

#define SEGSIZE 65536

/*
 * Forward declarations.
 */

static VALUE stmt_exec(int argc, VALUE *argv, VALUE self);
static VALUE stmt_each(VALUE self);
static VALUE stmt_each_hash(int argc, VALUE *argv, VALUE self);
static VALUE stmt_close(VALUE self);
static VALUE stmt_drop(VALUE self);


/*
 *----------------------------------------------------------------------
 *
 *      Constructor for ODBCDSN
 *
 *----------------------------------------------------------------------
 */

static VALUE
dsn_new(VALUE self)
{
    VALUE obj = rb_obj_alloc(Cdsn);

    rb_obj_call_init(obj, 0, NULL);
    return obj;
}

static VALUE
dsn_init(VALUE self)
{
    rb_iv_set(self, "@name", Qnil);
    rb_iv_set(self, "@descr", Qnil);
    return self;
}

/*
 *----------------------------------------------------------------------
 *
 *      Constructor for ODBCDriver
 *
 *----------------------------------------------------------------------
 */

static VALUE
drv_new(VALUE self)
{
    VALUE obj = rb_obj_alloc(Cdrv);

    rb_obj_call_init(obj, 0, NULL);
    return obj;
}

static VALUE
drv_init(VALUE self)
{
    rb_iv_set(self, "@name", Qnil);
    rb_iv_set(self, "@attrs", rb_hash_new());
    return self;
}

/*
 *----------------------------------------------------------------------
 *
 *      GC free callbacks.
 *
 *----------------------------------------------------------------------
 */

static void
free_env(ENV *e)
{
    if (e->henv != SQL_NULL_HENV) {
	SQLFreeEnv(e->henv);
	e->henv = SQL_NULL_HENV;
    }
    e->dbcs = Qnil;
    free(e);
}

static void
free_dbc(DBC *p)
{
    if (p->hdbc != SQL_NULL_HDBC) {
	SQLDisconnect(p->hdbc);
	SQLFreeConnect(p->hdbc);
	p->hdbc = SQL_NULL_HDBC;
    }
    p->self = p->env = p->stmts = Qnil;
    free(p);
}

static void
free_stmt_sub(STMT *q)
{
    q->nump = 0;
    q->ncols = 0;
    if (q->pinfo) {
	xfree(q->pinfo);
	q->pinfo = NULL;
    }
    if (q->coltypes) {
	xfree(q->coltypes);
	q->coltypes = NULL;
    }
    if (q->colnames) {
	xfree(q->colnames);
	q->colnames = NULL;
    }
    if (q->dbufs) {
	xfree(q->dbufs);
	q->dbufs = NULL;
    }
    if (q->self != Qnil) {
	VALUE v;

	v = rb_iv_get(q->self, "@_a");
	if (v != Qnil) {
	    rb_ary_clear(v);
	}
	v = rb_iv_get(q->self, "@_h");
	if (v != Qnil) {
	    rb_iv_set(q->self, "@_h", rb_hash_new());
	}
    }
}

static void
free_stmt(STMT *q)
{
    free_stmt_sub(q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	SQLFreeStmt(q->hstmt, SQL_DROP);
	q->hstmt = SQL_NULL_HSTMT;
    }
    q->self = Qnil;
    q->dbc = Qnil;
    free(q);
}

/*
 *----------------------------------------------------------------------
 *
 *      GC mark callbacks.
 *
 *----------------------------------------------------------------------
 */

static void
mark_env(ENV *e)
{
    if (e->dbcs != Qnil) {
	rb_gc_mark(e->dbcs);
    }
}

static void
mark_dbc(DBC *p)
{
    if (p->env != Qnil) {
	rb_gc_mark(p->env);
    }
    if (p->stmts != Qnil) {
	rb_gc_mark(p->stmts);
    }
}

static void
mark_stmt(STMT *q)
{
    if (q->dbc != Qnil) {
	rb_gc_mark(q->dbc);
    }
}

/*
 *----------------------------------------------------------------------
 *
 *      Set internal error message.
 *
 *----------------------------------------------------------------------
 */

static char *
set_err(char *msg)
{
    VALUE v = rb_str_new2("INTERNAL: ");

    v = rb_str_cat2(v, msg);
    rb_cvar_set(Cobj, rb_intern("@@error"), v);
    return STR2CSTR(v);
}

/*
 *----------------------------------------------------------------------
 *
 *      Retrieve last SQL error.
 *
 *----------------------------------------------------------------------
 */

static char *
get_err(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
    char msg[SQL_MAX_MESSAGE_LENGTH + 1], st[6 + 1], buf[32], *p;
    SQLRETURN err;
    SQLINTEGER nerr;
    SQLSMALLINT len;
    VALUE v;

    err = SQLError(henv, hdbc, hstmt, st, &nerr, msg, sizeof (msg) - 1, &len);
    st[6] = '\0';
    msg[SQL_MAX_MESSAGE_LENGTH] = '\0';
    /* get rid of dangerous % chars */
    while ((p = strchr(msg, '%')) != NULL) {
	*p = '?';
    }
    while ((p = strchr(st, '%')) != NULL) {
	*p = '?';
    }
    switch (err) {
    case SQL_SUCCESS:
	v = rb_str_new2(st);
	sprintf(buf, " (%d) ", (int) nerr);
	v = rb_str_cat2(v, buf);
	v = rb_str_cat(v, msg, len);
	break;
    case SQL_NO_DATA_FOUND:
	v = Qnil;
	break;
    case SQL_INVALID_HANDLE:
	v = rb_str_new2("invalid handle");
	break;
    default:
	sprintf(msg, "unknown error %d", err);
	v = rb_str_new2(msg);
	break;
    }
    rb_cvar_set(Cobj, rb_intern("@@error"), v);
    return v == Qnil ? NULL : STR2CSTR(v);
}

/*
 *----------------------------------------------------------------------
 *
 *      Return ENV from VALUE.
 *
 *----------------------------------------------------------------------
 */

static VALUE
env_of(VALUE self)
{
    if (CLASS_OF(self) == Cstmt) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	self = q->dbc;
    }
    if (CLASS_OF(self) == Cdbc) {
	DBC *p;

	Data_Get_Struct(self, DBC, p);
	self = p->env;
    }
    return self;
}

static ENV *
get_env(VALUE self)
{
    ENV *e;

    Data_Get_Struct(env_of(self), ENV, e);
    return e;
}

/*
 *----------------------------------------------------------------------
 *
 *      Return DBC from VALUE.
 *
 *----------------------------------------------------------------------
 */

static DBC *
get_dbc(VALUE self)
{
    DBC *p;

    if (CLASS_OF(self) == Cstmt) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	self = q->dbc;
    }
    Data_Get_Struct(self, DBC, p);
    return p;
}

/*
 *----------------------------------------------------------------------
 *
 *      Raise ODBC error from Ruby.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_raise(VALUE self, VALUE msg)
{
    VALUE v;
    int i;
    char buf[256 + 1], *p;

    if (TYPE(msg) == T_STRING) {
	v = msg;
    } else {
	v = rb_any_to_s(msg);
    }
    p = rb_str2cstr(v, &i);
    strncpy(buf, p, 256);
    buf[256] = '\0';
    while ((p = strchr(buf, '%')) != NULL) {
	*p = '?';
    }
    rb_cvar_set(Cobj, rb_intern("@@error"), msg);
    rb_raise(Cerror, buf);
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain an ENV.
 *
 *----------------------------------------------------------------------
 */

static VALUE
env_new(self)
{
    ENV *e;
    SQLHENV henv = SQL_NULL_HENV;
    VALUE obj;

    if (TYPE(self) == T_MODULE) {
	self = Cobj;
    }
    if (self == Cobj) {
	self = Cenv;
    }
    if (SQLAllocEnv(&henv) != SQL_SUCCESS || henv == SQL_NULL_HENV) {
	rb_raise(Cerror, set_err("cannot allocate SQLHENV"));
    }
    obj = Data_Make_Struct(self, ENV, mark_env, free_env, e);
    e->henv = henv;
    e->dbcs = rb_hash_new();
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain array of known DSNs.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_dsns(VALUE self)
{
    SQLRETURN result;
    char dsn[SQL_MAX_DSN_LENGTH], descr[256];
    SQLSMALLINT dsnLen, descrLen;
    int first = 1;
    VALUE env, aret;
    ENV *e;

    env = env_new(Cenv);
    Data_Get_Struct(env, ENV, e);
    aret = rb_ary_new();
    while ((result = SQLDataSources(e->henv, (SQLUSMALLINT) (first ?
				    SQL_FETCH_FIRST : SQL_FETCH_NEXT),
				    (SQLCHAR *) dsn,
				    (SQLSMALLINT) sizeof (dsn), &dsnLen,
				    (SQLCHAR *) descr,
				    (SQLSMALLINT) sizeof (descr), &descrLen))
	   == SQL_SUCCESS) {
	VALUE odsn = rb_obj_alloc(Cdsn);

	rb_iv_set(odsn, "@name", rb_str_new(dsn, dsnLen));
	rb_iv_set(odsn, "@descr", rb_str_new(descr, descrLen));
	rb_ary_push(aret, odsn);
	first = 0;
    }
    return aret;
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain array of known drivers.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_drivers(VALUE self)
{
    SQLRETURN result;
    char driver[256], attrs[1024], *attr;
    SQLSMALLINT driverLen, attrsLen;
    int first = 1;
    VALUE env, aret;
    ENV *e;

    env = env_new(Cenv);
    Data_Get_Struct(env, ENV, e);
    aret = rb_ary_new();
    while ((result = SQLDrivers(e->henv, (SQLUSMALLINT) (first ?
				SQL_FETCH_FIRST : SQL_FETCH_NEXT),
				(SQLCHAR *) driver,
				(SQLSMALLINT) sizeof (driver), &driverLen,
				(SQLCHAR *) attrs,
				(SQLSMALLINT) sizeof (attrs), &attrsLen))
	   == SQL_SUCCESS) {
	VALUE odrv = rb_obj_alloc(Cdrv);
	VALUE h = rb_hash_new();
	int count = 0;

	rb_iv_set(odrv, "@name", rb_str_new(driver, driverLen));
	for (attr = attrs; *attr; attr += strlen(attr) + 1) {
	    char *p = strchr(attr, '=');

	    if (p != NULL && p != attr) {
		rb_hash_aset(h, rb_str_new(attr, p - attr),
			     rb_str_new2(p + 1));
		count++;
	    }
	}
	if (count) {
	    rb_iv_set(odrv, "@attrs", h);
	}
	rb_ary_push(aret, odrv);
	first = 0;
    }
    return aret;
}

/*
 *----------------------------------------------------------------------
 *
 *      Management methods.
 *
 *----------------------------------------------------------------------
 */

static VALUE
conf_dsn(int argc, VALUE *argv, VALUE self, int op)
{
    VALUE drv, attr, issys, astr;

    rb_scan_args(argc, argv, "12", &drv, &attr, &issys);
    if (rb_obj_is_kind_of(drv, Cdrv) == Qtrue) {
	VALUE a, x;

	if (argc > 2) {
	    rb_raise(rb_eArgError, "wrong # of arguments");
	}
	x = rb_iv_get(drv, "@name");
	a = rb_iv_get(drv, "@attrs");
	issys = attr;
	drv = x;
	attr = a;
    }
    Check_Type(drv, T_STRING);
    if (RTEST(issys)) {
	switch (op) {
	case ODBC_ADD_DSN:	op = ODBC_ADD_SYS_DSN; break;
	case ODBC_CONFIG_DSN:	op = ODBC_CONFIG_SYS_DSN; break;
	case ODBC_REMOVE_DSN:	op = ODBC_REMOVE_SYS_DSN; break;
	}
    }
    astr = rb_str_new2("");
    if (rb_obj_is_kind_of(attr, rb_cHash) == Qtrue) {
	VALUE a, x;

	a = rb_funcall(attr, rb_intern("keys"), 0, NULL);
	while ((x = rb_ary_shift(a)) != Qnil) {
	    VALUE v = rb_hash_aref(attr, x);

	    astr = rb_str_concat(astr, x);
	    astr = rb_str_cat2(astr, "=");
	    astr = rb_str_concat(astr, v);
	    astr = rb_str_cat(astr, "", 1);
	}
    }
    astr = rb_str_cat(astr, "", 1);
    if (SQLConfigDataSource(NULL, (WORD) op, STR2CSTR(drv), STR2CSTR(astr))) {
	return Qnil;
    }
    rb_raise(Cerror, set_err("DSN configuration error"));
    return Qnil;
}

static VALUE
dbc_adddsn(int argc, VALUE *argv, VALUE self)
{
    return conf_dsn(argc, argv, self, ODBC_ADD_DSN);
}

static VALUE
dbc_confdsn(int argc, VALUE *argv, VALUE self)
{
    return conf_dsn(argc, argv, self, ODBC_CONFIG_DSN);
}

static VALUE
dbc_deldsn(int argc, VALUE *argv, VALUE self)
{
    return conf_dsn(argc, argv, self, ODBC_REMOVE_DSN);
}

/*
 *----------------------------------------------------------------------
 *
 *      Return last ODBC error.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_error(VALUE self)
{
    return rb_cvar_get(Cobj, rb_intern("@@error"));
}

/*
 *----------------------------------------------------------------------
 *
 *      Connection instance initializer.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_new(int argc, VALUE *argv, VALUE self)
{
    DBC *p;
    VALUE obj, env = Qnil;

    if (TYPE(self) == T_MODULE) {
	self = Cobj;
    }
    if (self == Cobj) {
	self = Cdbc;
    }
    if (rb_obj_is_kind_of(self, Cenv) == Qtrue) {
	env = env_of(self);
	self = Cdbc;
    }
    obj = Data_Make_Struct(self, DBC, mark_dbc, free_dbc, p);
    p->self = obj;
    p->env = env;
    if (env != Qnil) {
	ENV *e;

	Data_Get_Struct(env, ENV, e);
	rb_hash_aset(e->dbcs, p->self, p->self);
    }
    p->hdbc = SQL_NULL_HDBC;
    p->stmts = rb_hash_new();
    if (argc > 0) {
	rb_obj_call_init(obj, argc, argv);
    }
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Connect to data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_connect(int argc, VALUE *argv, VALUE self)
{
    ENV *e;
    DBC *p;
    VALUE dsn, user, passwd;
    char *sdsn, *suser = "", *spasswd = "";
    SQLHDBC dbc;

    rb_scan_args(argc, argv, "12", &dsn, &user, &passwd);
    if (rb_obj_is_kind_of(dsn, Cdsn) == Qtrue) {
	dsn = rb_iv_get(dsn, "@name");
    }
    Check_Type(dsn, T_STRING);
    if (user != Qnil) {
	Check_Type(user, T_STRING);
	suser = STR2CSTR(user);
    }
    if (passwd != Qnil) {
	Check_Type(passwd, T_STRING);
	spasswd = STR2CSTR(passwd);
    }
    sdsn = STR2CSTR(dsn);
    p = get_dbc(self);
    if (p->hdbc != SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("already connected"));
    }
    if (p->env == Qnil) {
	p->env = env_new(Cenv);
	rb_hash_aset(get_env(p->env)->dbcs, p->self, p->self);
    }
    e = get_env(p->env);
    if (SQLAllocConnect(e->henv, &dbc) != SQL_SUCCESS) {
	rb_raise(Cerror, get_err(e->henv, NULL, NULL));
    }
    if (SQLConnect(dbc, (SQLCHAR *) sdsn, SQL_NTS, (SQLCHAR *) suser, SQL_NTS,
		   (SQLCHAR *) spasswd, SQL_NTS) != SQL_SUCCESS) {
	char *msg = get_err(e->henv, dbc, NULL);

	SQLFreeConnect(dbc);
	rb_raise(Cerror, msg);
    }
    p->hdbc = dbc;
    return self;
}

static VALUE
dbc_drvconnect(VALUE self, VALUE drv)
{
    ENV *e;
    DBC *p;
    char *sdrv;
    SQLHDBC dbc;

    if (rb_obj_is_kind_of(drv, Cdrv) == Qtrue) {
	VALUE d, a, x;

	d = rb_str_new2("");
	a = rb_funcall(rb_iv_get(drv, "@attrs"), rb_intern("keys"), 0, NULL);
	while ((x = rb_ary_shift(a)) != Qnil) {
	    VALUE v = rb_hash_aref(rb_iv_get(drv, "@attrs"), x);

	    d = rb_str_concat(d, x);
	    d = rb_str_cat2(d, "=");
	    d = rb_str_concat(d, v);
	    d = rb_str_cat2(d, ";");
	}
	drv = d;
    }
    Check_Type(drv, T_STRING);
    sdrv = STR2CSTR(drv);
    p = get_dbc(self);
    if (p->hdbc != SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("already connected"));
    }
    if (p->env == Qnil) {
	p->env = env_new(Cenv);
	rb_hash_aset(get_env(p->env)->dbcs, p->self, p->self);
    }
    e = get_env(p->env);
    if (SQLAllocConnect(e->henv, &dbc) != SQL_SUCCESS) {
	rb_raise(Cerror, get_err(e->henv, NULL, NULL));
    }
    if (SQLDriverConnect(dbc, NULL, (SQLCHAR *) sdrv, SQL_NTS,
			 NULL, 0, NULL, SQL_DRIVER_NOPROMPT) != SQL_SUCCESS) {
	char *msg = get_err(e->henv, dbc, NULL);

	SQLFreeConnect(dbc);
	rb_raise(Cerror, msg);
    }
    p->hdbc = dbc;
    return self;
}

static VALUE
dbc_connected(VALUE self)
{
    DBC *p = get_dbc(self);

    return p->hdbc == SQL_NULL_HDBC ? Qfalse : Qtrue;
}

/*
 *----------------------------------------------------------------------
 *
 *      Drop all active statements from data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_dropall(VALUE self)
{
    DBC *p = get_dbc(self);
    VALUE a, s;
    int n = 0;

    a = rb_funcall(p->stmts, rb_intern("keys"), 0, NULL);
    while ((s = rb_ary_shift(a)) != Qnil) {
	stmt_drop(s);
	n++;
    }
    if (n) {
	rb_funcall(rb_mGC, rb_intern("start"), 0, NULL);
    }
    return self;
}

/*
 *----------------------------------------------------------------------
 *
 *      Disconnect from data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_disconnect(VALUE self)
{
    DBC *p = get_dbc(self);

    if (p->hdbc != SQL_NULL_HDBC &&
	rb_funcall(p->stmts, rb_intern("empty?"), 0, NULL) == Qtrue) {
	SQLDisconnect(p->hdbc);
	if (SQLFreeConnect(p->hdbc) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, p->hdbc, NULL));
	}
	p->hdbc = SQL_NULL_HDBC;
	if (p->env != Qnil) {
	    ENV *e;

	    Data_Get_Struct(p->env, ENV, e);
	    rb_funcall(e->dbcs, rb_intern("delete"), 1, p->self);
	    p->env = Qnil;
	}
	return Qtrue;
    }
    return Qfalse;
}

/*
 *----------------------------------------------------------------------
 *
 *      Fill column type array for statement.
 *
 *----------------------------------------------------------------------
 */

static COLTYPE *
make_coltypes(SQLHSTMT hstmt, int ncols)
{
    int i;
    COLTYPE *ret = NULL;
    SQLINTEGER type, size;
    
    for (i = 0; i < ncols; i++) {
	if (SQLColAttributes(hstmt, (SQLUSMALLINT) (i + 1), SQL_COLUMN_TYPE,
			     NULL, 0, NULL, &type) != SQL_SUCCESS) {
	    return ret;
	}
	if (SQLColAttributes(hstmt, (SQLUSMALLINT) (i + 1),
			     SQL_COLUMN_DISPLAY_SIZE,
			     NULL, 0, NULL, &size) != SQL_SUCCESS) {
	    return ret;
	}
    }
    ret = ALLOC_N(COLTYPE, ncols);
    if (ret == NULL) {
	return NULL;
    }
    for (i = 0; i < ncols; i++) {
	SQLColAttributes(hstmt, (SQLUSMALLINT) (i + 1), SQL_COLUMN_TYPE,
			 NULL, 0, NULL, &type);
	SQLColAttributes(hstmt, (SQLUSMALLINT) (i + 1),
			 SQL_COLUMN_DISPLAY_SIZE,
			 NULL, 0, NULL, &size);
	switch (type) {
#ifdef SQL_BIT
	case SQL_BIT:
#endif
#ifdef SQL_TINYINT
	case SQL_TINYINT:
#endif
	case SQL_SMALLINT:
	case SQL_INTEGER:
	    type = SQL_C_LONG;
	    size = sizeof (long);
	    break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
	case SQL_REAL:
	    type = SQL_C_DOUBLE;
	    size = sizeof (double);
	    break;
	case SQL_DATE:
	case SQL_TYPE_DATE:
	    type = SQL_C_DATE;
	    size = sizeof (DATE_STRUCT);
	    break;
	case SQL_TIME:
	case SQL_TYPE_TIME:
	    type = SQL_C_TIME;
	    size = sizeof (TIME_STRUCT);
	    break;
	case SQL_TIMESTAMP:
	case SQL_TYPE_TIMESTAMP:
	    type = SQL_C_TIMESTAMP;
	    size = sizeof (TIMESTAMP_STRUCT);
	    break;
	case SQL_LONGVARBINARY:
	case SQL_LONGVARCHAR:
	    type = SQL_C_CHAR;
	    size = SQL_NO_TOTAL;
	    break;
	default:
	    type = SQL_C_CHAR;
	    if (size != SQL_NO_TOTAL) {
		size += 1;
	    }
	    break;
	}
	ret[i].type = type;
	ret[i].size = size;
    }
    return ret;
}

/*
 *----------------------------------------------------------------------
 *
 *      Fill parameter info array for statement.
 *
 *----------------------------------------------------------------------
 */

static PINFO *
make_pinfo(SQLHSTMT hstmt, int nump)
{
    int i;
    PINFO *pinfo = NULL;

    pinfo = ALLOC_N(PINFO, nump);
    if (pinfo == NULL) {
	return NULL;
    }
    for (i = 0; i < nump; i++) {
	if (SQLDescribeParam(hstmt, (SQLUSMALLINT) (i + 1),
				 &pinfo[i].type, &pinfo[i].coldef,
				 &pinfo[i].scale, &pinfo[i].nullable)
	    != SQL_SUCCESS) {
	    pinfo[i].type = SQL_VARCHAR;
	    pinfo[i].coldef = 0;
	    pinfo[i].scale = 0;
	    pinfo[i].nullable = SQL_NULLABLE_UNKNOWN;
	}
    }
    return pinfo;
}

/*
 *----------------------------------------------------------------------
 *
 *      Create statement with result.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_result(VALUE dbc, SQLHSTMT hstmt, VALUE result, int mode)
{
    DBC *p;
    STMT *q;
    SQLSMALLINT cols = 0, nump;
    COLTYPE *coltypes = NULL;
    PINFO *pinfo = NULL;
    char *msg;

    Data_Get_Struct(dbc, DBC, p);
    if (SQLNumParams(hstmt, &nump) != SQL_SUCCESS) {
	nump = 0;
    }
    if (nump > 0) {
	pinfo = make_pinfo(hstmt, nump);
	if (pinfo == NULL) {
	    goto error;
	}
    }
    if ((mode & MAKERES_PREPARE) ||
	SQLNumResultCols(hstmt, &cols) != SQL_SUCCESS) {
	cols = 0;
    }
    if (cols > 0) {
	coltypes = make_coltypes(hstmt, cols);
	if (coltypes == NULL) {
	    goto error;
	}
    }
    if (result == Qnil) {
	result = Data_Make_Struct(Cstmt, STMT, mark_stmt, free_stmt, q);
	q->self = result;
	q->pinfo = NULL;
	q->coltypes = NULL;
	q->colnames = q->dbufs = NULL;
	rb_hash_aset(p->stmts, q->self, q->self);
	rb_iv_set(q->self, "@_a", rb_ary_new());
	rb_iv_set(q->self, "@_h", rb_hash_new());
    } else {
	Data_Get_Struct(result, STMT, q);
	free_stmt_sub(q);
    }
    q->dbc = dbc;
    q->hstmt = hstmt;
    q->nump = nump;
    q->pinfo = pinfo;
    q->ncols = cols;
    q->coltypes = coltypes;
    if ((mode & MAKERES_BLOCK) && rb_block_given_p()) {
	if (mode & MAKERES_NOCLOSE) {
	    return rb_yield(result);
	}
	return rb_ensure(rb_yield, result, stmt_close, result);
    }
    return result;
error:
    msg = get_err(NULL, NULL, hstmt); 
    SQLFreeStmt(hstmt, SQL_DROP);
    if (result != Qnil) {
	Data_Get_Struct(result, STMT, q);
	if (q->hstmt == hstmt) {
	    q->hstmt = SQL_NULL_HSTMT;
	}
    }
    if (pinfo) {
	xfree(pinfo);
    }
    if (coltypes) {
	xfree(coltypes);
    }
    rb_raise(Cerror, msg);
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Constructor: make column from statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_col(SQLHSTMT hstmt, int i)
{
    VALUE obj, v;
    SQLUSMALLINT ic = i + 1;
    SQLINTEGER iv;
    char name[256];

    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_LABEL, name,
			 (SQLSMALLINT) sizeof (name), NULL, NULL)
	!= SQL_SUCCESS) {
	rb_raise(Cerror, get_err(NULL, NULL, hstmt));
    }
    obj = rb_obj_alloc(Ccolumn);
    rb_iv_set(obj, "@name", rb_str_new2(name));
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_TABLE_NAME, name,
			 (SQLSMALLINT) sizeof (name), NULL, NULL)
	== SQL_SUCCESS) {
	v = rb_str_new2(name);
    }
    rb_iv_set(obj, "@table", v);
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_TYPE, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = INT2NUM(iv);
    } else {
	v = INT2NUM(SQL_UNKNOWN_TYPE);
    }
    rb_iv_set(obj, "@type", v);
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_DESC_LENGTH, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = INT2NUM(iv);
    } else if (SQLColAttributes(hstmt, ic, SQL_COLUMN_DISPLAY_SIZE, NULL,
				0, NULL, &iv) == SQL_SUCCESS) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@length", v);
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_NULLABLE, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = iv == SQL_NO_NULLS ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@nullable", v);
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_SCALE, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@scale", v);
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_PRECISION, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@precision", v);
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_SEARCHABLE, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = iv == SQL_NO_NULLS ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@searchable", v);
    v = Qnil;
    if (SQLColAttributes(hstmt, ic, SQL_COLUMN_UNSIGNED, NULL,
			 0, NULL, &iv) == SQL_SUCCESS) {
	v = iv == SQL_NO_NULLS ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@unsigned", v);
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Query tables/columns/keys/indexes/types of data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_info(int argc, VALUE *argv, VALUE self, int mode)
{
    DBC *p = get_dbc(self);
    VALUE which = Qnil, which2 = Qnil;
    char *swhich = NULL, *swhich2 = NULL, *msg, *argspec = NULL;
    SQLHSTMT hstmt;
    int needstr = 1, itype = SQL_ALL_TYPES;

    if (p->hdbc == SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("no connection"));
    }
    switch (mode) {
    case INFO_TYPES:
	needstr = 0;
	/* FALL THRU */
    case INFO_TABLES:
    case INFO_COLUMNS:
    case INFO_PRIMKEYS:
    case INFO_TPRIV:
    case INFO_PROCS:
	argspec = "01";
	break;
    case INFO_INDEXES:
	argspec = "1";
	break;
    case INFO_FORKEYS:
    case INFO_PROCCOLS:
	argspec = "11";
	break;
    default:
	rb_raise(Cerror, set_err("invalid info mode"));
	break;
    }
    rb_scan_args(argc, argv, argspec, &which, &which2);
    if (which != Qnil) {
	if (needstr) {
	    Check_Type(which, T_STRING);
	    swhich = STR2CSTR(which);
	} else {
	    itype = NUM2INT(which);
	}
    }
    if (which2 != Qnil) {
	Check_Type(which2, T_STRING);
	swhich2 = STR2CSTR(which2);
    }
    if (SQLAllocStmt(p->hdbc, &hstmt) != SQL_SUCCESS) {
	rb_raise(Cerror, get_err(NULL, p->hdbc, NULL));
    }
    switch (mode) {
    case INFO_TABLES:
	if (SQLTables(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS, NULL, 0)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_COLUMNS:
	if (SQLColumns(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS, NULL, 0)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_PRIMKEYS:
	if (SQLPrimaryKeys(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_INDEXES:
	if (SQLStatistics(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS,
			  SQL_INDEX_ALL, SQL_ENSURE) != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_TYPES:
	if (SQLGetTypeInfo(hstmt, (SQLSMALLINT) itype) != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_FORKEYS:
	if (SQLForeignKeys(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS,
			   NULL, 0, NULL, 0, swhich2, SQL_NTS)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_TPRIV:
	if (SQLTablePrivileges(hstmt, NULL, 0, NULL, 0,
			       swhich == NULL ? NULL : swhich, SQL_NTS)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_PROCS:
	if (SQLProcedures(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    case INFO_PROCCOLS:
	if (SQLProcedureColumns(hstmt, NULL, 0, NULL, 0, swhich, SQL_NTS,
				swhich2, SQL_NTS)
	    != SQL_SUCCESS) {
	    goto error;
	}
	break;
    }
    return make_result(self, hstmt, Qnil, MAKERES_BLOCK);
error:
    msg = get_err(NULL, NULL, hstmt);
    SQLFreeStmt(hstmt, SQL_DROP);
    rb_raise(Cerror, msg);
    return Qnil;
}

static VALUE
dbc_tables(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_TABLES);
}

static VALUE
dbc_columns(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_COLUMNS);
}

static VALUE
dbc_primkeys(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_PRIMKEYS);
}

static VALUE
dbc_indexes(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_INDEXES);
}

static VALUE
dbc_types(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_TYPES);
}

static VALUE
dbc_forkeys(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_FORKEYS);
}

static VALUE
dbc_tpriv(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_TPRIV);
}

static VALUE
dbc_procs(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_PROCS);
}

static VALUE
dbc_proccols(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_PROCCOLS);
}

/*
 *----------------------------------------------------------------------
 *
 *      Transaction stuff.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_trans(VALUE self, int what)
{
    ENV *e;
    SQLHDBC dbc = SQL_NULL_HDBC;

    e = get_env(self);
    if (CLASS_OF(self) != Cenv) {
	DBC *d;

	d = get_dbc(self);
	dbc = d->hdbc;
    }
    if (SQLTransact(e->henv, dbc, (SQLUSMALLINT) what) != SQL_SUCCESS) {
	rb_raise(Cerror, get_err(e->henv, dbc, NULL));
    }
    return Qnil;
}

static VALUE
dbc_commit(VALUE self)
{
    return dbc_trans(self, SQL_COMMIT);
}

static VALUE
dbc_rollback(VALUE self)
{
    return dbc_trans(self, SQL_ROLLBACK);
}

static VALUE
dbc_nop(VALUE self)
{
    return Qnil;
}

static VALUE
dbc_transbody(VALUE self)
{
    return rb_yield(rb_ary_entry(self, 0));
}

static VALUE
dbc_transfail(VALUE self, VALUE err)
{
    rb_ary_store(self, 1, err);
    dbc_rollback(rb_ary_entry(self, 0));
    return Qundef;
}

static VALUE
dbc_transaction(VALUE self)
{
    VALUE a, ret;

    if (!rb_block_given_p()) {
	rb_raise(Cerror, set_err("missing block for transaction"));
    }
    rb_ensure(dbc_commit, self, dbc_nop, self);
    a = rb_ary_new2(2);
    rb_ary_store(a, 0, self);
    rb_ary_store(a, 1, Qnil);
    if ((ret = rb_rescue2(dbc_transbody, a, dbc_transfail, a,
			  rb_eException)) != Qundef) {
	dbc_commit(self);
	return ret;
    }
    ret = rb_ary_entry(a, 1);
    rb_exc_raise(rb_exc_new3(CLASS_OF(ret), ret));
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Environment attribute handling.
 *
 *----------------------------------------------------------------------
 */

static VALUE
do_attr(int argc, VALUE *argv, VALUE self, int op)
{
    SQLHENV henv = NULL;
    VALUE val;
    SQLINTEGER v, l;

    if (self != Modbc) {
	henv = get_env(self)->henv;
    }
    rb_scan_args(argc, argv, "01", &val);
    if (val == Qnil) {
	if (SQLGetEnvAttr(henv, (SQLINTEGER) op, (SQLPOINTER) &v,
			  sizeof (v), &l) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(henv, NULL, NULL));
	}
	return rb_int2inum(v);
    }
    v = NUM2INT(val);
    if (SQLSetEnvAttr(henv, (SQLINTEGER) op, (SQLPOINTER) v, SQL_IS_INTEGER)
	!= SQL_SUCCESS) {
	rb_raise(Cerror, get_err(henv, NULL, NULL));
    }
    return Qnil;
}

static VALUE
env_cpooling(int argc, VALUE *argv, VALUE self)
{
    return do_attr(argc, argv, self, SQL_ATTR_CONNECTION_POOLING);
}

static VALUE
env_cpmatch(int argc, VALUE *argv, VALUE self)
{
    return do_attr(argc, argv, self, SQL_ATTR_CP_MATCH);
}

static VALUE
env_odbcver(int argc, VALUE *argv, VALUE self)
{
    return do_attr(argc, argv, self, SQL_ATTR_ODBC_VERSION);
}

/*
 *----------------------------------------------------------------------
 *
 *      Connection option handling.
 *
 *----------------------------------------------------------------------
 */

static VALUE
do_option(int argc, VALUE *argv, VALUE self, int op)
{
    DBC *p;
    VALUE val;
    SQLINTEGER v;

    rb_scan_args(argc, argv, "01", &val);
    p = get_dbc(self);
    if (p->hdbc == SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("no connection"));
    }
    if (val == Qnil) {
	if (SQLGetConnectOption(p->hdbc, (SQLUSMALLINT) op, (SQLPOINTER) &v)
	    != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, p->hdbc, NULL));
	}
    }
    switch (op) {
    case SQL_AUTOCOMMIT:
	if (val == Qnil) {
	    return v ? Qtrue : Qfalse;
	}
	v = RTEST(val) ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
	break;

    case SQL_NOSCAN:
	if (val == Qnil) {
	    return v ? Qtrue : Qfalse;
	}
	v = RTEST(val) ? SQL_NOSCAN_ON : SQL_NOSCAN_OFF;
	break;

    case SQL_CONCURRENCY:
    case SQL_QUERY_TIMEOUT:
    case SQL_MAX_ROWS:
    case SQL_MAX_LENGTH:
    case SQL_ROWSET_SIZE:
    case SQL_CURSOR_TYPE:
	if (val == Qnil) {
	    return rb_int2inum(v);
	}
	Check_Type(val, T_FIXNUM);
	v = FIX2INT(val);
	break;

    default:
	return Qnil;
    }
    if (SQLSetConnectOption(p->hdbc, (SQLUSMALLINT) op, (SQLUINTEGER) v)
	!= SQL_SUCCESS) {
	rb_raise(Cerror, get_err(NULL, p->hdbc, NULL));
    }
    return Qnil;
}

static VALUE
dbc_autocommit(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_AUTOCOMMIT);
}

static VALUE
dbc_concurrency(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_CONCURRENCY);
}

static VALUE
dbc_maxrows(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_MAX_ROWS);
}

static VALUE
dbc_timeout(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_QUERY_TIMEOUT);
}

static VALUE
dbc_maxlength(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_MAX_LENGTH);
}

static VALUE
dbc_rowsetsize(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_ROWSET_SIZE);
}

static VALUE
dbc_cursortype(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_CURSOR_TYPE);
}

static VALUE
dbc_noscan(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_NOSCAN);
}

/*
 *----------------------------------------------------------------------
 *
 *      Date methods.
 *
 *----------------------------------------------------------------------
 */

static VALUE
date_new(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE obj = Data_Make_Struct(self, DATE_STRUCT, 0, free, date);
    
    rb_obj_call_init(obj, argc, argv);
    return obj;
}

static VALUE
date_load(VALUE self, VALUE str)
{
    int i, y, m, d;

    if (sscanf(rb_str2cstr(str, &i), "%d-%d-%d", &y, &m, &d) == 3) {
	DATE_STRUCT *date;
	VALUE obj = Data_Make_Struct(self, DATE_STRUCT, 0, free, date);

	date->year = y;
	date->month = m;
	date->day = d;
	return obj;
    }
    rb_raise(rb_eTypeError, "marshaled ODBC::Date format error");
    return Qnil;
}

static VALUE
date_init(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE d, m, y;

    rb_scan_args(argc, argv, "03", &y, &m, &d);
    if (rb_obj_is_kind_of(y, Cdate) == Qtrue) {
	DATE_STRUCT *date2;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, DATE_STRUCT, date);
	Data_Get_Struct(y, DATE_STRUCT, date2);
	*date = *date2;
	return self;
    }
    if (rb_obj_is_kind_of(y, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, DATE_STRUCT, date);
	Data_Get_Struct(y, TIMESTAMP_STRUCT, ts);
	date->year  = ts->year;
	date->month = ts->month;
	date->day   = ts->day;
	return self;
    }
    if (rb_obj_is_kind_of(y, rb_cTime) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	d = rb_funcall(y, rb_intern("day"), 0, NULL);
	m = rb_funcall(y, rb_intern("month"), 0, NULL);
	y = rb_funcall(y, rb_intern("year"), 0, NULL);
    } else if (rb_obj_is_kind_of(y, rb_cDate) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	d = rb_funcall(y, rb_intern("mday"), 0, NULL);
	m = rb_funcall(y, rb_intern("month"), 0, NULL);
	y = rb_funcall(y, rb_intern("year"), 0, NULL);
    }
    Data_Get_Struct(self, DATE_STRUCT, date);
    date->year  = y == Qnil ? 0 : NUM2INT(y);
    date->month = m == Qnil ? 0 : NUM2INT(m);
    date->day   = d == Qnil ? 0 : NUM2INT(d);
    return self;
}

static VALUE
date_to_s(VALUE self)
{
    DATE_STRUCT *date;
    char buf[128];

    Data_Get_Struct(self, DATE_STRUCT, date);
    sprintf(buf, "%04d-%02d-%02d", date->year, date->month, date->day);
    return rb_str_new2(buf);
}

static VALUE
date_dump(VALUE self, VALUE depth)
{
    return date_to_s(self);
}

static VALUE
date_inspect(VALUE self)
{
    VALUE s = rb_str_new2("#<ODBC::Date: ");

    s = rb_str_append(s, date_to_s(self));
    return rb_str_append(s, rb_str_new2(">"));
}

static VALUE
date_year(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, DATE_STRUCT, date);
    if (v == Qnil) {
	return INT2NUM(date->year);
    }
    date->year = NUM2INT(v);
    return self;
}

static VALUE
date_month(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, DATE_STRUCT, date);
    if (v == Qnil) {
	return INT2NUM(date->month);
    }
    date->month = NUM2INT(v);
    return self;
}

static VALUE
date_day(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, DATE_STRUCT, date);
    if (v == Qnil) {
	return INT2NUM(date->day);
    }
    date->day = NUM2INT(v);
    return self;
}

static VALUE
date_cmp(VALUE self, VALUE date)
{
    DATE_STRUCT *date1, *date2;

    if (rb_obj_is_kind_of(date, Cdate) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::Date as argument");
    }
    Data_Get_Struct(self, DATE_STRUCT, date1);
    Data_Get_Struct(date, DATE_STRUCT, date2);
    if (date1->year < date2->year) {
	return INT2FIX(-1);
    }
    if (date1->year == date2->year) {
	if (date1->month < date2->month) {
	    return INT2FIX(-1);
	}
	if (date1->month == date2->month) {
	    if (date1->day < date2->day) {
		return INT2FIX(-1);
	    }
	    if (date1->day == date2->day) {
		return INT2FIX(0);
	    }
	}
    }
    return INT2FIX(1);
}

/*
 *----------------------------------------------------------------------
 *
 *      Time methods.
 *
 *----------------------------------------------------------------------
 */

static VALUE
time_new(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE obj = Data_Make_Struct(self, TIME_STRUCT, 0, free, time);
    
    rb_obj_call_init(obj, argc, argv);
    return obj;
}

static VALUE
time_load(VALUE self, VALUE str)
{
    int i, h, m, s;

    if (sscanf(rb_str2cstr(str, &i), "%d:%d:%d", &h, &m, &s) == 3) {
	TIME_STRUCT *time;
	VALUE obj = Data_Make_Struct(self, TIME_STRUCT, 0, free, time);

	time->hour = h;
	time->minute = m;
	time->second = s;
	return obj;
    }
    rb_raise(rb_eTypeError, "marshaled ODBC::Time format error");
    return Qnil;
}

static VALUE
time_init(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE h, m, s;

    rb_scan_args(argc, argv, "03", &h, &m, &s);
    if (rb_obj_is_kind_of(h, Ctime) == Qtrue) {
	TIME_STRUCT *time2;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, TIME_STRUCT, time);
	Data_Get_Struct(h, TIME_STRUCT, time2);
	*time = *time2;
	return self;
    }
    if (rb_obj_is_kind_of(h, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, TIME_STRUCT, time);
	Data_Get_Struct(h, TIMESTAMP_STRUCT, ts);
	time->hour   = ts->hour;
	time->minute = ts->minute;
	time->second = ts->second;
	return self;
    }
    if (rb_obj_is_kind_of(h, rb_cTime) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	s = rb_funcall(h, rb_intern("sec"), 0, NULL);
	m = rb_funcall(h, rb_intern("min"), 0, NULL);
	h = rb_funcall(h, rb_intern("hour"), 0, NULL);
    }
    Data_Get_Struct(self, TIME_STRUCT, time);
    time->hour   = h == Qnil ? 0 : NUM2INT(h);
    time->minute = m == Qnil ? 0 : NUM2INT(m);
    time->second = s == Qnil ? 0 : NUM2INT(s);
    return self;
}

static VALUE
time_to_s(VALUE self)
{
    TIME_STRUCT *time;
    char buf[128];

    Data_Get_Struct(self, TIME_STRUCT, time);
    sprintf(buf, "%02d:%02d:%02d", time->hour, time->minute, time->second);
    return rb_str_new2(buf);
}

static VALUE
time_dump(VALUE self, VALUE depth)
{
    return time_to_s(self);
}

static VALUE
time_inspect(VALUE self)
{
    VALUE s = rb_str_new2("#<ODBC::Time: ");

    s = rb_str_append(s, time_to_s(self));
    return rb_str_append(s, rb_str_new2(">"));
}

static VALUE
time_hour(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIME_STRUCT, time);
    if (v == Qnil) {
	return INT2NUM(time->hour);
    }
    time->hour = NUM2INT(v);
    return self;
}

static VALUE
time_minute(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIME_STRUCT, time);
    if (v == Qnil) {
	return INT2NUM(time->minute);
    }
    time->minute = NUM2INT(v);
    return self;
}

static VALUE
time_second(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIME_STRUCT, time);
    if (v == Qnil) {
	return INT2NUM(time->second);
    }
    time->second = NUM2INT(v);
    return self;
}

static VALUE
time_cmp(VALUE self, VALUE time)
{
    TIME_STRUCT *time1, *time2;

    if (rb_obj_is_kind_of(time, Ctime) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::Time as argument");
    }
    Data_Get_Struct(self, TIME_STRUCT, time1);
    Data_Get_Struct(time, TIME_STRUCT, time2);
    if (time1->hour < time2->hour) {
	return INT2FIX(-1);
    }
    if (time1->hour == time2->hour) {
	if (time1->minute < time2->minute) {
	    return INT2FIX(-1);
	}
	if (time1->minute == time2->minute) {
	    if (time1->second < time2->second) {
		return INT2FIX(-1);
	    }
	    if (time1->second == time2->second) {
		return INT2FIX(0);
	    }
	}
    }
    return INT2FIX(1);
}

/*
 *----------------------------------------------------------------------
 *
 *      TimeStamp methods.
 *
 *----------------------------------------------------------------------
 */

static VALUE
timestamp_new(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, free, ts);

    rb_obj_call_init(obj, argc, argv);
    return obj;
}

static VALUE
timestamp_load(VALUE self, VALUE str)
{
    int i, y, m, d, hh, mm, ss, frac;

    if (sscanf(rb_str2cstr(str, &i), "%d-%d-%d %d:%d:%d %d",
	       &y, &m, &d, &hh, &mm, &ss, &frac) == 7) {
	TIMESTAMP_STRUCT *ts;
	VALUE obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, free, ts);

	ts->year = y;
	ts->month = m;
	ts->day = d;
	ts->hour = hh;
	ts->minute = mm;
	ts->second = ss;
	ts->fraction = frac;
	return obj;
    }
    rb_raise(rb_eTypeError, "marshaled ODBC::TimeStamp format error");
    return Qnil;
}

static VALUE
timestamp_init(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE d, m, y, hh, mm, ss, f;

    rb_scan_args(argc, argv, "07", &y, &m, &d, &hh, &mm, &ss, &f);
    if (rb_obj_is_kind_of(y, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts2;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
	Data_Get_Struct(y, TIMESTAMP_STRUCT, ts2);
	*ts = *ts2;
	return self;
    }
    if (rb_obj_is_kind_of(y, Cdate) == Qtrue) {
	DATE_STRUCT *date;

	if (argc > 1) {
	    if (argc > 2) {
		rb_raise(rb_eArgError, "wrong # arguments");
	    }
	    if (rb_obj_is_kind_of(m, Ctime) == Qtrue) {
		TIME_STRUCT *time;

		Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
		Data_Get_Struct(m, TIME_STRUCT, time);
		ts->hour   = time->hour;
		ts->minute = time->minute;
		ts->second = time->second;
	    } else {
		rb_raise(rb_eArgError, "need ODBC::Time argument");
	    }
	}
	Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
	Data_Get_Struct(y, DATE_STRUCT, date);
	ts->year = date->year;
	ts->year = date->year;
	ts->year = date->year;
	ts->fraction = 0;
	return self;
    }
    if (rb_obj_is_kind_of(y, rb_cTime) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	f  = rb_funcall(y, rb_intern("usec"), 0, NULL);
	ss = rb_funcall(y, rb_intern("sec"), 0, NULL);
	mm = rb_funcall(y, rb_intern("min"), 0, NULL);
	hh = rb_funcall(y, rb_intern("hour"), 0, NULL);
	d  = rb_funcall(y, rb_intern("day"), 0, NULL);
	m  = rb_funcall(y, rb_intern("month"), 0, NULL);
	y  = rb_funcall(y, rb_intern("year"), 0, NULL);
	f = INT2NUM(NUM2INT(f) * 1000);
    } else if (rb_obj_is_kind_of(y, rb_cDate) == Qtrue) {
	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	f  = INT2FIX(0);
	ss = INT2FIX(0);
	mm = INT2FIX(0);
	hh = INT2FIX(0);
	d  = rb_funcall(y, rb_intern("mday"), 0, NULL);
	m  = rb_funcall(y, rb_intern("month"), 0, NULL);
	y  = rb_funcall(y, rb_intern("year"), 0, NULL);
    }
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    ts->year     = y  == Qnil ? 0 : NUM2INT(y);
    ts->month    = m  == Qnil ? 0 : NUM2INT(m);
    ts->day      = d  == Qnil ? 0 : NUM2INT(d);
    ts->hour     = hh == Qnil ? 0 : NUM2INT(hh);
    ts->minute   = mm == Qnil ? 0 : NUM2INT(mm);
    ts->second   = ss == Qnil ? 0 : NUM2INT(ss);
    ts->fraction = f  == Qnil ? 0 : NUM2INT(f);
    return self;
}

static VALUE
timestamp_to_s(VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    char buf[256];

    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d %ld",
	    ts->year, ts->month, ts->day,
	    ts->hour, ts->minute, ts->second,
	    ts->fraction);
    return rb_str_new2(buf);
}

static VALUE
timestamp_dump(VALUE self, VALUE depth)
{
    return timestamp_to_s(self);
}

static VALUE
timestamp_inspect(VALUE self)
{
    VALUE s = rb_str_new2("#<ODBC::TimeStamp: \"");

    s = rb_str_append(s, timestamp_to_s(self));
    return rb_str_append(s, rb_str_new2("\">"));
}

static VALUE
timestamp_year(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->year);
    }
    ts->year = NUM2INT(v);
    return self;
}

static VALUE
timestamp_month(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->month);
    }
    ts->month = NUM2INT(v);
    return self;
}

static VALUE
timestamp_day(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->day);
    }
    ts->day = NUM2INT(v);
    return self;
}

static VALUE
timestamp_hour(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->hour);
    }
    ts->hour = NUM2INT(v);
    return self;
}

static VALUE
timestamp_minute(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->minute);
    }
    ts->minute = NUM2INT(v);
    return self;
}

static VALUE
timestamp_second(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->second);
    }
    ts->second = NUM2INT(v);
    return self;
}

static VALUE
timestamp_fraction(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE v;

    rb_scan_args(argc, argv, "01", &v);
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    if (v == Qnil) {
	return INT2NUM(ts->fraction);
    }
    ts->fraction = NUM2INT(v);
    return self;
}

static VALUE
timestamp_cmp(VALUE self, VALUE timestamp)
{
    TIMESTAMP_STRUCT *ts1, *ts2;

    if (rb_obj_is_kind_of(timestamp, Ctimestamp) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::TimeStamp as argument");
    }
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts1);
    Data_Get_Struct(timestamp, TIMESTAMP_STRUCT, ts2);
    if (ts1->year < ts2->year) {
	return INT2FIX(-1);
    }
    if (ts1->year == ts2->year) {
	if (ts1->month < ts2->month) {
	    return INT2FIX(-1);
	}
	if (ts1->month == ts2->month) {
	    if (ts1->day < ts2->day) {
		return INT2FIX(-1);
	    }
	    if (ts1->day == ts2->day) {
		if (ts1->hour < ts2->hour) {
		    return INT2FIX(-1);
		}
		if (ts1->hour == ts2->hour) {
		    if (ts1->minute < ts2->minute) {
			return INT2FIX(-1);
		    }
		    if (ts1->minute == ts2->minute) {
			if (ts1->second < ts2->second) {
			    return INT2FIX(-1);
			}
			if (ts1->second == ts2->second) {
			    if (ts1->fraction < ts2->fraction) {
				return INT2FIX(-1);
			    }
			    if (ts1->fraction == ts2->fraction) {
				return INT2FIX(0);
			    }
			}
		    }
		}
	    }
	}
    }
    return INT2FIX(1);
}

/*
 *----------------------------------------------------------------------
 *
 *      Statement methods.
 *
 *----------------------------------------------------------------------
 */

static VALUE
stmt_drop(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	DBC *p;

	SQLFreeStmt(q->hstmt, SQL_DROP);
	q->hstmt = SQL_NULL_HSTMT;
	Data_Get_Struct(q->dbc, DBC, p);
	rb_funcall(p->stmts, rb_intern("delete"), 1, q->self);
	q->dbc = Qnil;
    }
    free_stmt_sub(q);
    return self;
}

static VALUE
stmt_close(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	SQLFreeStmt(q->hstmt, SQL_CLOSE);
    }
    free_stmt_sub(q);
    return self;
}

static VALUE
stmt_cancel(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	if (SQLCancel(q->hstmt) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
	}
    }
    return self;
}

static VALUE
stmt_ncols(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    return INT2FIX(q->ncols);
}

static VALUE
stmt_nrows(VALUE self)
{
    STMT *q;
    SQLINTEGER rows;

    Data_Get_Struct(self, STMT, q);
    if (SQLRowCount(q->hstmt, &rows) != SQL_SUCCESS) {
	rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
    }
    return INT2NUM(rows);
}

static VALUE
stmt_column(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    VALUE col;

    rb_scan_args(argc, argv, "1", &col);
    Check_Type(col, T_FIXNUM);
    Data_Get_Struct(self, STMT, q);
    return make_col(q->hstmt, FIX2INT(col));
}

static VALUE
stmt_columns(VALUE self)
{
    STMT *q;
    int i;
    VALUE res;

    Data_Get_Struct(self, STMT, q);
    res = rb_hash_new();
    for (i = 0; i < q->ncols; i++) {
	VALUE obj, name;

	obj = make_col(q->hstmt, i);
	name = rb_iv_get(obj, "@name");
	rb_hash_aset(res, name, obj);
    }
    return res;
}

static VALUE
do_fetch(STMT *q, int mode)
{
    int i;
    char **bufs;
    VALUE res;

    if (q->ncols <= 0) {
	rb_raise(Cerror, set_err("no columns in result set"));
    }
    bufs = q->dbufs;
    if (bufs == NULL) {
	int need = sizeof (char *) * q->ncols;
	char *p;

	for (i = 0; i < q->ncols; i++) {
	    if (q->coltypes[i].size != SQL_NO_TOTAL) {
		need += q->coltypes[i].size;
	    }
	}
	p = ALLOC_N(char, need);
	if (p == NULL) {
	    rb_raise(Cerror, set_err("out of memory"));
	}
	q->dbufs = bufs = (char **) p;
	p += sizeof (char *) * q->ncols;
	for (i = 0; i < q->ncols; i++) {
	    int len = q->coltypes[i].size;

	    if (len == SQL_NO_TOTAL) {
		bufs[i] = NULL;
	    } else {
		bufs[i] = p;
		p += len;
	    }
	}
    }
    switch (mode & DOFETCH_MODES) {
    case DOFETCH_HASH:
    case DOFETCH_HASH2:
	if (q->colnames == NULL) {
	    int need = sizeof (char *) * 2 * q->ncols;
	    char **na, *p, name[256];

	    for (i = 0; i < q->ncols; i++) {
		if (SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
				     SQL_COLUMN_TABLE_NAME, name,
				     sizeof (name), NULL, NULL)
		    != SQL_SUCCESS) {
		    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
		}
		need += strlen(name) + 1;
		if (SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
				     SQL_COLUMN_LABEL, name,
				     sizeof (name), NULL, NULL)
		    != SQL_SUCCESS) {
		    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
		}
		need += strlen(name) + 1;
	    }
	    p = ALLOC_N(char, need);
	    if (p == NULL) {
		rb_raise(Cerror, set_err("out of memory"));
	    }
	    na = (char **) p;
	    p += sizeof (char *) * 2 * q->ncols;
	    for (i = 0; i < q->ncols; i++) {
		SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
				 SQL_COLUMN_TABLE_NAME, name,
				 sizeof (name), NULL, NULL);
		na[i + q->ncols] = p;
		strcpy(p, name);
		strcat(p, ".");
		p += strlen(p);
		SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
				 SQL_COLUMN_LABEL, name,
				 sizeof (name), NULL, NULL);
		strcpy(p, name);
		na[i] = p;
		p += strlen(p) + 1;
	    }
	    q->colnames = na;
	}
	if (mode & DOFETCH_BANG) {
	    res = rb_iv_get(q->self, "@_h");
	    if (res == Qnil) {
		res = rb_hash_new();
		rb_iv_set(q->self, "@_h", res);
	    }
	} else {
	    res = rb_hash_new();
	}
	break;
    default:
	if (mode & DOFETCH_BANG) {
	    res = rb_iv_get(q->self, "@_a");
	    if (res == Qnil) {
		res = rb_ary_new2(q->ncols);
		rb_iv_set(q->self, "@_a", res);
	    } else {
		rb_ary_clear(res);
	    }
	} else {
	    res = rb_ary_new2(q->ncols);
	}
    }
    for (i = 0; i < q->ncols; i++) {
	SQLINTEGER curlen;
	SQLSMALLINT type = q->coltypes[i].type;
	VALUE v;
	char *valp, *freep = NULL;
	SQLINTEGER totlen;

	curlen = q->coltypes[i].size;
	if (curlen == SQL_NO_TOTAL) {

	    totlen = 0;
	    valp = ALLOC_N(char, SEGSIZE + 1);
	    freep = valp;
	    while (curlen == SQL_NO_TOTAL || curlen > SEGSIZE) {
		int ret;

		ret = SQLGetData(q->hstmt, (SQLUSMALLINT) (i + 1), type,
				 (SQLPOINTER) (valp + totlen),
				 type == SQL_C_CHAR ? SEGSIZE + 1 : SEGSIZE,
				 &curlen);
		if (ret == SQL_ERROR) {
		    xfree(valp);
		    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
		}
		if (curlen == SQL_NULL_DATA) {
		    break;
		}
		if (curlen == SQL_NO_TOTAL) {
		    totlen += SEGSIZE;
		} else if (curlen > SEGSIZE) {
		    totlen += SEGSIZE;
		} else {
		    totlen += curlen;
		    break;
		}
		REALLOC_N(valp, char, totlen + SEGSIZE);
		if (valp == NULL) {
		    if (freep) {
			xfree(freep);
		    }
		    rb_raise(Cerror, set_err("out of memory"));
		}
		freep = valp;
	    }
	    if (totlen > 0) {
		curlen = totlen;
	    }
	} else {
	    totlen = curlen;
	    valp = bufs[i];
	    if (SQLGetData(q->hstmt, (SQLUSMALLINT) (i + 1), type,
			   (SQLPOINTER) valp, totlen, &curlen) == SQL_ERROR) {
		rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
	    }
	}
	if (curlen == SQL_NULL_DATA) {
	    v = Qnil;
	} else {
	    switch (type) {
	    case SQL_C_LONG:
		v = INT2NUM(*((long *) valp));
		break;
	    case SQL_C_DOUBLE:
		v = rb_float_new(*((double *) valp));
		break;
	    case SQL_C_DATE:
		{
		    DATE_STRUCT *date;

		    v = Data_Make_Struct(Cdate, DATE_STRUCT, 0, free, date);
		    *date = *(DATE_STRUCT *) valp;
		}
		break;
	    case SQL_C_TIME:
		{
		    TIME_STRUCT *time;

		    v = Data_Make_Struct(Ctime, TIME_STRUCT, 0, free, time);
		    *time = *(TIME_STRUCT *) valp;
		}
		break;
	    case SQL_C_TIMESTAMP:
		{
		    TIMESTAMP_STRUCT *ts;

		    v = Data_Make_Struct(Ctimestamp, TIMESTAMP_STRUCT,
					 0, free, ts);
		    *ts = *(TIMESTAMP_STRUCT *) valp;
		}
		break;
	    default:
		v = rb_str_new(valp, curlen);
		break;
	    }
	}
	if (freep) {
	    xfree(freep);
	}
	switch (mode & DOFETCH_MODES) {
	case DOFETCH_HASH:
	    rb_hash_aset(res, rb_str_new2(q->colnames[i]), v);
	    break;
	case DOFETCH_HASH2:
	    rb_hash_aset(res, rb_str_new2(q->colnames[i + q->ncols]), v);
	    break;
	default:
	    rb_ary_push(res, v);
	}
    }
    return res;
}

static VALUE
stmt_fetch1(VALUE self, int bang)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    switch (SQLFetch(q->hstmt)) {
    case SQL_NO_DATA:
	return Qnil;
    case SQL_SUCCESS:
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
    return Qnil;
}

static VALUE
stmt_fetch(VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each(self);
    }
    return stmt_fetch1(self, 0);
}

static VALUE
stmt_fetch_bang(VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each(self);
    }
    return stmt_fetch1(self, 1);
}

static VALUE
stmt_fetch_first1(VALUE self, int bang)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    switch (SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0)) {
    case SQL_NO_DATA:
	return Qnil;
    case SQL_SUCCESS:
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
    return Qnil;
}

static VALUE
stmt_fetch_first(VALUE self)
{
    return stmt_fetch_first1(self, 0);
}

static VALUE
stmt_fetch_first_bang(VALUE self)
{
    return stmt_fetch_first1(self, 1);
}

static VALUE
stmt_fetch_scroll1(int argc, VALUE *argv, VALUE self, int bang)
{
    STMT *q;
    VALUE dir, offs;
    int idir, ioffs = 1;

    rb_scan_args(argc, argv, "11", &dir, &offs);
    idir = NUM2INT(dir);
    if (offs != Qnil) {
	ioffs = NUM2INT(offs);
    }
    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    switch (SQLFetchScroll(q->hstmt, (SQLSMALLINT) idir, (SQLINTEGER) ioffs)) {
    case SQL_NO_DATA:
	return Qnil;
    case SQL_SUCCESS:
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
    return Qnil;
}

static VALUE
stmt_fetch_scroll(int argc, VALUE *argv, VALUE self)
{
    return stmt_fetch_scroll1(argc, argv, self, 0);
}

static VALUE
stmt_fetch_scroll_bang(int argc, VALUE *argv, VALUE self)
{
    return stmt_fetch_scroll1(argc, argv, self, 1);
}

static VALUE
stmt_fetch_many(VALUE self, VALUE arg)
{
    int i, max, all = arg == Qnil;
    VALUE res;

    if (!all) {
	max = NUM2INT(arg);
    }
    res = rb_ary_new();
    for (i = 0; all || i < max; i++) {
	VALUE v = stmt_fetch1(self, 0);

	if (v == Qnil) {
	    break;
	}
	rb_ary_push(res, v);
    }
    return i == 0 ? Qnil : res;
}

static VALUE
stmt_fetch_all(VALUE self)
{
    return stmt_fetch_many(self, Qnil);
}

static int
stmt_hash_mode(int argc, VALUE *argv, VALUE self)
{
    VALUE withtab = Qnil;

    rb_scan_args(argc, argv, "01", &withtab);
    return RTEST(withtab) ? DOFETCH_HASH2 : DOFETCH_HASH;
}

static VALUE
stmt_fetch_hash1(int argc, VALUE *argv, VALUE self, int bang)
{
    STMT *q;
    int mode = stmt_hash_mode(argc, argv, self);

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    switch (SQLFetch(q->hstmt)) {
    case SQL_NO_DATA:
	return Qnil;
    case SQL_SUCCESS:
	return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
    return Qnil;
}

static VALUE
stmt_fetch_hash(int argc, VALUE *argv, VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each_hash(argc, argv, self);
    }
    return stmt_fetch_hash1(argc, argv, self, 0);
}

static VALUE
stmt_fetch_hash_bang(int argc, VALUE *argv, VALUE self)
{
    if (rb_block_given_p()) {
	return stmt_each_hash(argc, argv, self);
    }
    return stmt_fetch_hash1(argc, argv, self, 1);
}

static VALUE
stmt_fetch_first_hash1(int argc, VALUE *argv, VALUE self, int bang)
{
    STMT *q;
    int mode = stmt_hash_mode(argc, argv, self);

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    switch (SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0)) {
    case SQL_NO_DATA:
	return Qnil;
    case SQL_SUCCESS:
	return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
    return Qnil;
}

static VALUE
stmt_fetch_first_hash(int argc, VALUE *argv, VALUE self)
{
    return stmt_fetch_first_hash1(argc, argv, self, 0);
}

static VALUE
stmt_each(VALUE self)
{
    VALUE row;
    int first;
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    switch (SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0)) {
    case SQL_NO_DATA:
    case SQL_SUCCESS:
	first = 1;
	break;
    default:
	first = 0;
    }
    while ((row = first ? stmt_fetch_first1(self, 0) :
			  stmt_fetch1(self, 0)) != Qnil) {
	first = 0;
	rb_yield(row);
    }
    return self;
}

static VALUE
stmt_each_hash(int argc, VALUE *argv, VALUE self)
{
    VALUE row, withtab;
    int first;
    STMT *q;
    int mode = stmt_hash_mode(argc, argv, self);

    withtab = mode == DOFETCH_HASH2 ? Qtrue : Qfalse;
    Data_Get_Struct(self, STMT, q);
    switch (SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0)) {
    case SQL_NO_DATA:
    case SQL_SUCCESS:
	first = 1;
	break;
    default:
	first = 0;
    }
    while ((row = first ? stmt_fetch_first_hash1(1, &withtab, self, 0) :
			  stmt_fetch_hash1(1, &withtab, self, 0)) != Qnil) {
	first = 0;
	rb_yield(row);
    }
    return self;
}

static VALUE
stmt_prep_int(int argc, VALUE *argv, VALUE self, int mode)
{
    DBC *p = get_dbc(self);
    STMT *q = NULL;
    VALUE sql, dbc, stmt;
    SQLHSTMT hstmt;
    char *ssql = NULL;

    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	Data_Get_Struct(self, STMT, q);
	if (q->hstmt == SQL_NULL_HSTMT) {
	    if (SQLAllocStmt(p->hdbc, &q->hstmt) != SQL_SUCCESS) {
		rb_raise(Cerror, get_err(NULL, p->hdbc, NULL));
	    }
	} else if (SQLFreeStmt(q->hstmt, SQL_CLOSE) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, NULL, q->hstmt));
	}
	hstmt = q->hstmt;
	stmt = self;
	dbc = q->dbc;
    } else {
	if (SQLAllocStmt(p->hdbc, &hstmt) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, p->hdbc, NULL));
	}
	stmt = Qnil;
	dbc = self;
    }
    rb_scan_args(argc, argv, "1", &sql);
    Check_Type(sql, T_STRING);
    ssql = STR2CSTR(sql);
    if (SQLPrepare(hstmt, ssql, SQL_NTS) != SQL_SUCCESS) {
	char *msg = get_err(NULL, NULL, hstmt);

	SQLFreeStmt(hstmt, SQL_DROP);
	if (q) {
	    q->hstmt = SQL_NULL_HSTMT;
	}
	rb_raise(Cerror, msg);
    }
    return make_result(dbc, hstmt, stmt, mode | MAKERES_PREPARE);
}

static VALUE
stmt_prep(int argc, VALUE *argv, VALUE self)
{
    return stmt_prep_int(argc, argv, self, MAKERES_BLOCK);
}

static VALUE
stmt_exec_int(int argc, VALUE *argv, VALUE self, int mode)
{
    STMT *q;
    int i;
    char *msg;

    Data_Get_Struct(self, STMT, q);
    if (argc > q->nump) {
	rb_raise(Cerror, set_err("too much parameters"));
    }
    if (q->hstmt == SQL_NULL_HSTMT) {
	rb_raise(Cerror, set_err("stale ODBC::Statement"));
    }
    if (SQLFreeStmt(q->hstmt, SQL_CLOSE) != SQL_SUCCESS) {
	goto error;
    }
    SQLFreeStmt(q->hstmt, SQL_RESET_PARAMS);
    for (i = 0; i < argc; i++) {
	SQLPOINTER valp = (SQLPOINTER) &q->pinfo[i].buffer;
	SQLSMALLINT ctype, stype;
	SQLINTEGER vlen, rlen;
	SQLUINTEGER coldef;

	switch (TYPE(argv[i])) {
	case T_STRING:
	    ctype = SQL_C_CHAR;
	    valp = (SQLPOINTER) STR2CSTR(argv[i]);
	    rlen = strlen((char *) valp);
	    vlen = rlen + 1;
	    break;
	case T_FIXNUM:
	    ctype = SQL_C_LONG;
	    *(long *) valp = FIX2INT(argv[i]);
	    rlen = 1;
	    vlen = sizeof (long);
	    break;
	case T_FLOAT:
	    ctype = SQL_C_DOUBLE;
	    *(double *) valp = NUM2DBL(argv[i]);
	    rlen = 1;
	    vlen = sizeof (double);
	    break;
	case T_NIL:
	    ctype = SQL_C_CHAR;
	    valp = NULL;
	    rlen = SQL_NULL_DATA;
	    vlen = 0;
	    break;
	default:
	    if (rb_obj_is_kind_of(argv[i], Cdate) == Qtrue) {
		DATE_STRUCT *date;

		ctype = SQL_C_DATE;
		Data_Get_Struct(argv[i], DATE_STRUCT, date);
		valp = (SQLPOINTER) date;
		rlen = 1;
		vlen = sizeof (DATE_STRUCT);
		break;
	    }
	    if (rb_obj_is_kind_of(argv[i], Ctime) == Qtrue) {
		TIME_STRUCT *time;

		ctype = SQL_C_TIME;
		Data_Get_Struct(argv[i], TIME_STRUCT, time);
		valp = (SQLPOINTER) time;
		rlen = 1;
		vlen = sizeof (TIME_STRUCT);
		break;
	    }
	    if (rb_obj_is_kind_of(argv[i], Ctimestamp) == Qtrue) {
		TIMESTAMP_STRUCT *ts;

		ctype = SQL_C_TIMESTAMP;
		Data_Get_Struct(argv[i], TIMESTAMP_STRUCT, ts);
		valp = (SQLPOINTER) ts;
		rlen = 1;
		vlen = sizeof (TIMESTAMP_STRUCT);
		break;
	    }
	    ctype = SQL_C_CHAR;
	    valp = (void *) STR2CSTR(rb_any_to_s(argv[i]));
	    rlen = strlen((char *) valp);
	    vlen = rlen + 1;
	    break;
	}
	stype = q->pinfo[i].type;
	coldef = q->pinfo[i].coldef;
	q->pinfo[i].rlen = rlen;
	if (coldef == 0) {
	    switch (ctype) {
	    case SQL_C_LONG:
		coldef = 10;
		break;
	    case SQL_C_DOUBLE:
		coldef = 15;
		if (stype == SQL_VARCHAR) {
		    stype = SQL_DOUBLE;
		}
		break;
	    case SQL_C_DATE:
		coldef = 10;
		break;
	    case SQL_C_TIME:
		coldef = 8;
		break;
	    case SQL_C_TIMESTAMP:
		coldef = 19;
		break;
	    default:
		coldef = vlen;
		break;
	    }
	}
	if (SQLBindParameter(q->hstmt, (SQLUSMALLINT) (i + 1), SQL_PARAM_INPUT,
			     ctype, stype, coldef, q->pinfo[i].scale,
			     valp, vlen, &q->pinfo[i].rlen) == SQL_ERROR) {
	    goto error;
	}
    }
    for (; i < q->nump; i++) {
	q->pinfo[i].rlen = SQL_NULL_DATA;
	if (SQLBindParameter(q->hstmt, (SQLUSMALLINT) (i + 1), SQL_PARAM_INPUT,
			     SQL_C_CHAR, q->pinfo[i].type,
			     q->pinfo[i].coldef, q->pinfo[i].scale,
			     NULL, 0, &q->pinfo[i].rlen) == SQL_ERROR) {
	    goto error;
	}
    }
    if (SQLExecute(q->hstmt) != SQL_SUCCESS) {
error:
	msg = get_err(NULL, NULL, q->hstmt);
	SQLFreeStmt(q->hstmt, SQL_DROP);
	q->hstmt = SQL_NULL_HSTMT;
	rb_raise(Cerror, msg);
    }
    SQLFreeStmt(q->hstmt, SQL_RESET_PARAMS);
    return make_result(q->dbc, q->hstmt, self, mode);
}

static VALUE
stmt_exec(int argc, VALUE *argv, VALUE self)
{
    return stmt_exec_int(argc, argv, self, MAKERES_BLOCK);
}

static VALUE
stmt_run(int argc, VALUE *argv, VALUE self)
{
    if (argc < 1) {
	rb_raise(rb_eArgError, "wrong # of arguments");
    }
    return stmt_exec(argc - 1, argv + 1, stmt_prep_int(1, argv, self, 0));
}

static VALUE
stmt_do(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt;
    
    if (argc < 1) {
	rb_raise(rb_eArgError, "wrong # of arguments");
    }
    stmt = stmt_prep_int(1, argv, self, 0);
    stmt_exec_int(argc - 1, argv + 1, stmt, MAKERES_BLOCK | MAKERES_NOCLOSE);
    return rb_ensure(stmt_nrows, stmt, stmt_drop, stmt);
}

/*
 *----------------------------------------------------------------------
 *
 *      Procedures with statements.
 *
 *----------------------------------------------------------------------
 */

static VALUE
stmt_proc_init(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt = argc > 0 ? argv[0] : Qnil;

    if (rb_obj_is_kind_of(stmt, Cstmt) == Qtrue) {
	rb_iv_set(self, "@statement", stmt);
	return self;
    }
    rb_raise(rb_eTypeError, "need ODBC::Statement as argument");
    return Qnil;
}

static VALUE
stmt_proc_call(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt[1];

    stmt[0] = rb_iv_get(self, "@statement");
    stmt_exec_int(argc, argv, stmt[0], 0);
    return rb_call_super(1, stmt);
}

static VALUE
stmt_proc(VALUE self, VALUE sql)
{
    if (rb_block_given_p()) {
	VALUE stmt = stmt_prep_int(1, &sql, self, 0);

	return rb_funcall(Cproc, rb_intern("new"), 1, stmt);
    }
    rb_raise(Cerror, set_err("missing block for proc"));
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Module functions.
 *
 *----------------------------------------------------------------------
 */

static VALUE
mod_connect(int argc, VALUE *argv, VALUE self)
{
    VALUE dbc = dbc_new(argc, argv, self);

    if (rb_block_given_p()) {
	return rb_ensure(rb_yield, dbc, dbc_disconnect, dbc);
    }
    return dbc;
}

static VALUE
mod_2time(int argc, VALUE *argv, VALUE self)
{
    VALUE a1, a2;
    VALUE y, m, d, hh, mm, ss, us;

    rb_scan_args(argc, argv, "11", &a1, &a2);
    if (rb_obj_is_kind_of(a1, Ctimestamp) == Qtrue) {
	TIMESTAMP_STRUCT *ts;

	if (argc > 1) {
	    rb_raise(rb_eArgError, "wrong # arguments(2 for 1)");
	}
	Data_Get_Struct(a1, TIMESTAMP_STRUCT, ts);
	y = INT2NUM(ts->year);
	m = INT2NUM(ts->month);
	d = INT2NUM(ts->day);
	hh = INT2NUM(ts->hour);
	mm = INT2NUM(ts->minute);
	ss = INT2NUM(ts->second);
	us = INT2NUM(ts->fraction / 1000);
	goto mktime;
    }
    if (rb_obj_is_kind_of(a1, Cdate) == Qtrue) {
	DATE_STRUCT *date;

	if (a2 != Qnil) {
	    if (rb_obj_is_kind_of(a2, Ctime) == Qtrue) {
		TIME_STRUCT *time;

		Data_Get_Struct(a2, TIME_STRUCT, time);
		hh = INT2NUM(time->hour);
		mm = INT2NUM(time->minute);
		ss = INT2NUM(time->second);
	    } else {
		rb_raise(rb_eTypeError, "expecting ODBC::Time");
	    }
	} else {
	    hh = INT2FIX(0);
	    mm = INT2FIX(0);
	    ss = INT2FIX(0);
	}
	Data_Get_Struct(a1, DATE_STRUCT, date);
	y = INT2NUM(date->year);
	m = INT2NUM(date->month);
	d = INT2NUM(date->day);
	us = INT2FIX(0);
    }
    if (rb_obj_is_kind_of(a1, Ctime) == Qtrue) {
	TIME_STRUCT *time;

	if (a2 != Qnil) {
	    if (rb_obj_is_kind_of(a2, Cdate) == Qtrue) {
		DATE_STRUCT *date;

		Data_Get_Struct(a2, DATE_STRUCT, date);
		y = INT2NUM(date->year);
		m = INT2NUM(date->month);
		d = INT2NUM(date->day);
	    } else {
		rb_raise(rb_eTypeError, "expecting ODBC::Date");
	    }
	} else {
	    VALUE now = rb_funcall(rb_cTime, rb_intern("now"), 0, NULL);

	    y = rb_funcall(rb_cTime, rb_intern("year"), 1, now);
	    m = rb_funcall(rb_cTime, rb_intern("month"), 1, now);
	    d = rb_funcall(rb_cTime, rb_intern("day"), 1, now);
	}
	Data_Get_Struct(a1, TIME_STRUCT, time);
	hh = INT2NUM(time->hour);
	mm = INT2NUM(time->minute);
	ss = INT2NUM(time->second);
	us = INT2FIX(0);
mktime:
	return rb_funcall(rb_cTime, rb_intern("local"), 7,
			  y, m, d, hh, mm, ss, us);
    }
    rb_raise(rb_eTypeError, "expecting ODBC::TimeStamp or ODBC::Date/Time");
    return Qnil;
}

static VALUE
mod_2date(VALUE self, VALUE arg)
{
    VALUE y, m, d;

    if (rb_obj_is_kind_of(arg, Cdate) == Qtrue) {
	DATE_STRUCT *date;

	Data_Get_Struct(arg, DATE_STRUCT, date);
	y = INT2NUM(date->year);
	m = INT2NUM(date->month);
	d = INT2NUM(date->day);
	goto mkdate;
    }
    if (rb_obj_is_kind_of(arg, Ctimestamp) == Qtrue){
	TIMESTAMP_STRUCT *ts;

	Data_Get_Struct(arg, TIMESTAMP_STRUCT, ts);
	y = INT2NUM(ts->year);
	m = INT2NUM(ts->month);
	d = INT2NUM(ts->day);
mkdate:
	return rb_funcall(rb_cDate, rb_intern("new"), 3, y, m, d);
    }
    rb_raise(rb_eTypeError, "expecting ODBC::Date or ODBC::Timestamp");
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Table of constants.
 *
 *----------------------------------------------------------------------
 */

#define O_CONST(x)    { #x, x }
#define O_CONSTU(x)   { #x, SQL_UNKOWN_TYPE }
#define O_CONST2(x,y) { #x, y }

static struct {
    const char *name;
    int value;
} o_const[] = {
    O_CONST(SQL_CURSOR_FORWARD_ONLY),
    O_CONST(SQL_CURSOR_KEYSET_DRIVEN),
    O_CONST(SQL_CURSOR_DYNAMIC),
    O_CONST(SQL_CURSOR_STATIC),
    O_CONST(SQL_CONCUR_READ_ONLY),
    O_CONST(SQL_CONCUR_LOCK),
    O_CONST(SQL_CONCUR_ROWVER),
    O_CONST(SQL_CONCUR_VALUES),
    O_CONST(SQL_FETCH_NEXT),
    O_CONST(SQL_FETCH_FIRST),
    O_CONST(SQL_FETCH_LAST),
    O_CONST(SQL_FETCH_PRIOR),
    O_CONST(SQL_FETCH_ABSOLUTE),
    O_CONST(SQL_FETCH_RELATIVE),
    O_CONST(SQL_UNKNOWN_TYPE),
    O_CONST(SQL_CHAR),
    O_CONST(SQL_NUMERIC),
    O_CONST(SQL_DECIMAL),
    O_CONST(SQL_INTEGER),
    O_CONST(SQL_SMALLINT),
    O_CONST(SQL_FLOAT),
    O_CONST(SQL_REAL),
    O_CONST(SQL_DOUBLE),
    O_CONST(SQL_VARCHAR),
#ifdef SQL_DATETIME
    O_CONST(SQL_DATETIME),
#else
    O_CONSTU(SQL_DATETIME),
#endif
#ifdef SQL_DATE
    O_CONST(SQL_DATE),
#else
    O_CONSTU(SQL_DATE),
#endif
#ifdef SQL_TYPE_DATE
    O_CONST(SQL_TYPE_DATE),
#else
    O_CONSTU(SQL_TYPE_DATE),
#endif
#ifdef SQL_TIME
    O_CONST(SQL_TIME),
#else
    O_CONSTU(SQL_TIME),
#endif
#ifdef SQL_TYPE_TIME
    O_CONST(SQL_TYPE_TIME),
#else
    O_CONSTU(SQL_TYPE_TIME),
#endif
#ifdef SQL_TIMESTAMP
    O_CONST(SQL_TIMESTAMP),
#else
    O_CONSTU(SQL_TIMESTAMP),
#endif
#ifdef SQL_TYPE_TIMESTAMP
    O_CONST(SQL_TYPE_TIMESTAMP),
#else
    O_CONSTU(SQL_TYPE_TIMESTAMP),
#endif
#ifdef SQL_LONGVARCHAR
    O_CONST(SQL_LONGVARCHAR),
#else
    O_CONSTU(SQL_LONGVARCHAR),
#endif
#ifdef SQL_BINARY
    O_CONST(SQL_BINARY),
#else
    O_CONSTU(SQL_BINARY),
#endif
#ifdef SQL_VARBINARY
    O_CONST(SQL_VARBINARY),
#else
    O_CONSTU(SQL_VARBINARY),
#endif
#ifdef SQL_LONGVARBINARY
    O_CONST(SQL_LONGVARBINARY),
#else
    O_CONSTU(SQL_LONGVARBINARY),
#endif
#ifdef SQL_BIGINT
    O_CONST(SQL_BIGINT),
#else
    O_CONSTU(SQL_BIGINT),
#endif
#ifdef SQL_TINYINT
    O_CONST(SQL_TINYINT),
#else
    O_CONSTU(SQL_TINYINT),
#endif
#ifdef SQL_BIT
    O_CONST(SQL_BIT),
#else
    O_CONSTU(SQL_BIT),
#endif
#ifdef SQL_GUID
    O_CONST(SQL_GUID),
#else
    O_CONSTU(SQL_GUID),
#endif
#ifdef SQL_ATTR_ODBC_VERSION
    O_CONST(SQL_OV_ODBC2),
    O_CONST(SQL_OV_ODBC3),
#else
    O_CONST2(SQL_OV_ODBC2, 2),
    O_CONST2(SQL_OV_ODBC3, 3),
#endif
#ifdef SQL_ATTR_CONNECTION_POOLING
    O_CONST(SQL_CP_OFF),
    O_CONST(SQL_CP_ONE_PER_DRIVER),
    O_CONST(SQL_CP_ONE_PER_HENV),
    O_CONST(SQL_CP_DEFAULT),
#else
    O_CONST2(SQL_CP_OFF, 0),
    O_CONST2(SQL_CP_ONE_PER_DRIVER, 0),
    O_CONST2(SQL_CP_ONE_PER_HENV, 0),
    O_CONST2(SQL_CP_DEFAULT, 0),
#endif
#ifdef SQL_ATTR_CP_MATCH
    O_CONST(SQL_CP_STRICT_MATCH),
    O_CONST(SQL_CP_RELAXED_MATCH),
    O_CONST(SQL_CP_MATCH_DEFAULT),
#else
    O_CONST(SQL_CP_STRICT_MATCH, 0),
    O_CONST(SQL_CP_RELAXED_MATCH, 0),
    O_CONST(SQL_CP_MATCH_DEFAULT, 0),
#endif
    { NULL, 0 }
};

/*
 *----------------------------------------------------------------------
 *
 *      Module initializer.
 *
 *----------------------------------------------------------------------
 */

void
Init_odbc()
{
    int i;

    rb_require("date");
    rb_cDate = rb_eval_string("Date");

    Modbc = rb_define_module("ODBC");
    Cobj = rb_define_class_under(Modbc, "Object", rb_cObject);
    rb_define_class_variable(Cobj, "@@error", Qnil);

    Cenv = rb_define_class_under(Modbc, "Environment", Cobj);
    Cdbc = rb_define_class_under(Modbc, "Database", Cenv);
    Cstmt = rb_define_class_under(Modbc, "Statement", Cdbc);
    rb_include_module(Cstmt, rb_mEnumerable);

    Ccolumn = rb_define_class_under(Modbc, "Column", Cobj);
    rb_attr(Ccolumn, rb_intern("name"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("table"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("type"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("length"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("nullable"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("scale"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("precision"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("searchable"), 1, 0, 0);
    rb_attr(Ccolumn, rb_intern("unsigned"), 1, 0, 0);

    Cdsn = rb_define_class_under(Modbc, "DSN", Cobj);
    rb_attr(Cdsn, rb_intern("name"), 1, 1, 0);
    rb_attr(Cdsn, rb_intern("descr"), 1, 1, 0);

    Cdrv = rb_define_class_under(Modbc, "Driver", Cobj);
    rb_attr(Cdrv, rb_intern("name"), 1, 1, 0);
    rb_attr(Cdrv, rb_intern("attrs"), 1, 1, 0);

    Cerror = rb_define_class_under(Modbc, "Error", rb_eStandardError);

    Cproc = rb_define_class("ODBCProc", rb_cProc);

    Cdate = rb_define_class_under(Modbc, "Date", Cobj);
    rb_include_module(Cdate, rb_mComparable);
    Ctime = rb_define_class_under(Modbc, "Time", Cobj);
    rb_include_module(Ctime, rb_mComparable);
    Ctimestamp = rb_define_class_under(Modbc, "TimeStamp", Cobj);
    rb_include_module(Ctimestamp, rb_mComparable);

    /* module functions */
    rb_define_module_function(Modbc, "connect", mod_connect, -1);
    rb_define_module_function(Modbc, "datasources", dbc_dsns, 0);
    rb_define_module_function(Modbc, "drivers", dbc_drivers, 0);
    rb_define_module_function(Modbc, "error", dbc_error, 0);
    rb_define_module_function(Modbc, "newenv", env_new, 0);
    rb_define_module_function(Modbc, "to_time", mod_2time, -1);
    rb_define_module_function(Modbc, "to_date", mod_2date, 1);
    rb_define_module_function(Modbc, "connection_pooling", env_cpooling, -1);

    /* singleton methods and constructors */
    rb_define_singleton_method(Cobj, "error", dbc_error, 0);
    rb_define_singleton_method(Cenv, "new", env_new, 0);
    rb_define_singleton_method(Cenv, "connect", dbc_new, -1);
    rb_define_singleton_method(Cdbc, "new", dbc_new, -1);
    rb_define_singleton_method(Cdsn, "new", dsn_new, 0);
    rb_define_singleton_method(Cdrv, "new", drv_new, 0);
    rb_define_method(Cdsn, "initialize", dsn_init, 0);
    rb_define_method(Cdrv, "initialize", drv_init, 0);

    /* common (Cobj) methods */
    rb_define_method(Cobj, "error", dbc_error, 0);
    rb_define_method(Cobj, "raise", dbc_raise, 1);

    /* common (Cenv) methods */
    rb_define_method(Cenv, "connect", dbc_new, -1);
    rb_define_method(Cenv, "environment", env_of, 0);
    rb_define_method(Cenv, "transaction", dbc_transaction, 0);
    rb_define_method(Cenv, "commit", dbc_commit, 0);
    rb_define_method(Cenv, "rollback", dbc_rollback, 0);
    rb_define_method(Cenv, "connection_pooling", env_cpooling, -1);
    rb_define_method(Cenv, "cp_match", env_cpmatch, -1);
    rb_define_method(Cenv, "odbc_version", env_odbcver, -1);

    /* management things (odbcinst.h) */
    rb_define_module_function(Modbc, "add_dsn", dbc_adddsn, -1);
    rb_define_module_function(Modbc, "config_dsn", dbc_confdsn, -1);
    rb_define_module_function(Modbc, "del_dsn", dbc_deldsn, -1);

    /* connection (database) methods */
    rb_define_method(Cdbc, "initialize", dbc_connect, -1);
    rb_define_method(Cdbc, "connected?", dbc_connected, 0);
    rb_define_method(Cdbc, "drvconnect", dbc_drvconnect, 1);
    rb_define_method(Cdbc, "drop_all", dbc_dropall, 0);
    rb_define_method(Cdbc, "disconnect", dbc_disconnect, 0);
    rb_define_method(Cdbc, "tables", dbc_tables, -1);
    rb_define_method(Cdbc, "columns", dbc_columns, -1);
    rb_define_method(Cdbc, "primary_keys", dbc_primkeys, -1);
    rb_define_method(Cdbc, "indexes", dbc_indexes, -1);
    rb_define_method(Cdbc, "types", dbc_types, -1);
    rb_define_method(Cdbc, "foreign_keys", dbc_forkeys, -1);
    rb_define_method(Cdbc, "table_privileges", dbc_tpriv, -1);
    rb_define_method(Cdbc, "procedures", dbc_procs, -1);
    rb_define_method(Cdbc, "procedure_columns", dbc_proccols, -1);
    rb_define_method(Cdbc, "prepare", stmt_prep, -1);
    rb_define_method(Cdbc, "run", stmt_run, -1);
    rb_define_method(Cdbc, "do", stmt_do, -1);
    rb_define_method(Cdbc, "proc", stmt_proc, 1);

    /* connection options */
    rb_define_method(Cdbc, "autocommit", dbc_autocommit, -1);
    rb_define_method(Cdbc, "concurrency", dbc_concurrency, -1);
    rb_define_method(Cdbc, "maxrows", dbc_maxrows, -1);
    rb_define_method(Cdbc, "timeout", dbc_timeout, -1);
    rb_define_method(Cdbc, "maxlength", dbc_maxlength, -1);
    rb_define_method(Cdbc, "rowsetsize", dbc_rowsetsize, -1);
    rb_define_method(Cdbc, "cursortype", dbc_cursortype, -1);
    rb_define_method(Cdbc, "noscan", dbc_noscan, -1);

    /* statement methods */
    rb_define_method(Cstmt, "drop", stmt_drop, 0);
    rb_define_method(Cstmt, "close", stmt_close, 0);
    rb_define_method(Cstmt, "cancel", stmt_cancel, 0);
    rb_define_method(Cstmt, "column", stmt_column, -1);
    rb_define_method(Cstmt, "columns", stmt_columns, 0);
    rb_define_method(Cstmt, "ncols", stmt_ncols, 0);
    rb_define_method(Cstmt, "nrows", stmt_nrows, 0);
    rb_define_method(Cstmt, "fetch", stmt_fetch, 0);
    rb_define_method(Cstmt, "fetch!", stmt_fetch_bang, 0);
    rb_define_method(Cstmt, "fetch_first", stmt_fetch_first, 0);
    rb_define_method(Cstmt, "fetch_first!", stmt_fetch_first_bang, 0);
    rb_define_method(Cstmt, "fetch_scroll", stmt_fetch_scroll, -1);
    rb_define_method(Cstmt, "fetch_scroll!", stmt_fetch_scroll_bang, -1);
    rb_define_method(Cstmt, "fetch_hash", stmt_fetch_hash, -1);
    rb_define_method(Cstmt, "fetch_hash!", stmt_fetch_hash_bang, -1);
    rb_define_method(Cstmt, "fetch_many", stmt_fetch_many, 1);
    rb_define_method(Cstmt, "fetch_all", stmt_fetch_all, 0);
    rb_define_method(Cstmt, "each", stmt_each, 0);
    rb_define_method(Cstmt, "each_hash", stmt_each_hash, -1);
    rb_define_method(Cstmt, "execute", stmt_exec, -1);

    /* data type methods */
    rb_define_singleton_method(Cdate, "new", date_new, -1);
    rb_define_singleton_method(Cdate, "_load", date_load, 1);
    rb_define_method(Cdate, "initialize", date_init, -1);
    rb_define_method(Cdate, "to_s", date_to_s, 0);
    rb_define_method(Cdate, "_dump", date_dump, 1);
    rb_define_method(Cdate, "inspect", date_inspect, 0);
    rb_define_method(Cdate, "year", date_year, -1);
    rb_define_method(Cdate, "month", date_month, -1);
    rb_define_method(Cdate, "day", date_day, -1);
    rb_define_method(Cdate, "year=", date_year, -1);
    rb_define_method(Cdate, "month=", date_month, -1);
    rb_define_method(Cdate, "day=", date_day, -1);
    rb_define_method(Cdate, "<=>", date_cmp, 1);

    rb_define_singleton_method(Ctime, "new", time_new, -1);
    rb_define_singleton_method(Ctime, "_load", time_load, 1);
    rb_define_method(Ctime, "initialize", time_init, -1);
    rb_define_method(Ctime, "to_s", time_to_s, 0);
    rb_define_method(Ctime, "_dump", time_dump, 1);
    rb_define_method(Ctime, "inspect", time_inspect, 0);
    rb_define_method(Ctime, "hour", time_hour, -1);
    rb_define_method(Ctime, "minute", time_minute, -1);
    rb_define_method(Ctime, "second", time_second, -1);
    rb_define_method(Ctime, "hour=", time_hour, -1);
    rb_define_method(Ctime, "minute=", time_minute, -1);
    rb_define_method(Ctime, "second=", time_second, -1);
    rb_define_method(Ctime, "<=>", time_cmp, 1);

    rb_define_singleton_method(Ctimestamp, "new", timestamp_new, -1);
    rb_define_singleton_method(Ctimestamp, "_load", timestamp_load, 1);
    rb_define_method(Ctimestamp, "initialize", timestamp_init, -1);
    rb_define_method(Ctimestamp, "to_s", timestamp_to_s, 0);
    rb_define_method(Ctimestamp, "_dump", timestamp_dump, 1);
    rb_define_method(Ctimestamp, "inspect", timestamp_inspect, 0);
    rb_define_method(Ctimestamp, "year", timestamp_year, -1);
    rb_define_method(Ctimestamp, "month", timestamp_month, -1);
    rb_define_method(Ctimestamp, "day", timestamp_day, -1);
    rb_define_method(Ctimestamp, "hour", timestamp_hour, -1);
    rb_define_method(Ctimestamp, "minute", timestamp_minute, -1);
    rb_define_method(Ctimestamp, "second", timestamp_second, -1);
    rb_define_method(Ctimestamp, "fraction", timestamp_fraction, -1);
    rb_define_method(Ctimestamp, "year=", timestamp_year, -1);
    rb_define_method(Ctimestamp, "month=", timestamp_month, -1);
    rb_define_method(Ctimestamp, "day=", timestamp_day, -1);
    rb_define_method(Ctimestamp, "hour=", timestamp_hour, -1);
    rb_define_method(Ctimestamp, "minute=", timestamp_minute, -1);
    rb_define_method(Ctimestamp, "second=", timestamp_second, -1);
    rb_define_method(Ctimestamp, "fraction=", timestamp_fraction, -1);
    rb_define_method(Ctimestamp, "<=>", timestamp_cmp, 1);

    /* procedure methods */
    rb_define_method(Cproc, "initialize", stmt_proc_init, -1);
    rb_define_method(Cproc, "call", stmt_proc_call, -1);
    rb_define_method(Cproc, "[]", stmt_proc_call, -1);
    rb_enable_super(Cproc, "call");
    rb_enable_super(Cproc, "[]");

    /* constants */
    for (i = 0; o_const[i].name != NULL; i++) {
	rb_define_const(Modbc, o_const[i].name, INT2NUM(o_const[i].value));
    }
}
