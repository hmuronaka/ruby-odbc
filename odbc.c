/*
 * ODBC-Ruby binding
 * Copyright (c) 2001-2003 Christian Werner <chw@ch-werner.de>
 *
 * See the file "COPYING" for information on usage
 * and redistribution of this file and for a
 * DISCLAIMER OF ALL WARRANTIES.
 *
 * $Id: odbc.c,v 1.30 2003/12/26 12:47:32 chw Exp chw $
 */

#undef ODBCVER

#if defined(_WIN32) || defined(__CYGWIN32__) || defined(__MINGW32__)
#include <windows.h>
#endif
#include <stdarg.h>
#include <ctype.h>
#include "ruby.h"
#include "version.h"
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
#endif

#ifdef UNICODE
#include <sqlucode.h>
#endif

#ifndef HAVE_RB_DEFINE_ALLOC_FUNC
#define rb_define_alloc_func(cls, func) \
    rb_define_singleton_method(cls, "new", func, -1)
#define rb_undefine_alloc_fund(cls) \
    rb_undef_method(CLASS_OF(cls), "new")
#endif

#ifdef RB_CVAR_SET_4ARGS
#define CVAR_SET(x, y, z) rb_cvar_set(x, y, z, 0)
#else
#define CVAR_SET(x, y, z) rb_cvar_set(x, y, z)
#endif

#ifdef TRACING
static int tracing = 0;
#define tracemsg(t, x) {if (tracing & t) { x }}
static SQLRETURN tracesql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt,
			  SQLRETURN ret, char *m);
#else
#define tracemsg(t, x)
#define tracesql(a, b, c, d, e) d
#endif

#ifndef SQL_SUCCEEDED
#define SQL_SUCCEEDED(x) ((x) == SQL_SUCCESS || (x) == SQL_SUCCESS_WITH_INFO)
#endif

#ifndef SQL_NO_DATA
#define SQL_NO_DATA SQL_NO_DATA_FOUND
#endif

typedef struct link {
    struct link *succ;
    struct link *pred;
    struct link *head;
    int offs;
} LINK;

typedef struct env {
    VALUE self;
    LINK dbcs;
    SQLHENV henv;
} ENV;

typedef struct dbc {
    LINK link;
    VALUE self;
    VALUE env;
    struct env *envp;
    LINK stmts; 
    SQLHDBC hdbc;
    int upc;
} DBC;

typedef struct {
    SQLSMALLINT type;
    SQLUINTEGER coldef;
    SQLSMALLINT scale;
    SQLINTEGER rlen;
    SQLSMALLINT nullable;
    int tofree;
    char buffer[sizeof (double) * 4];
} PINFO;

typedef struct {
    int type;
    int size;
} COLTYPE;

typedef struct stmt {
    LINK link;
    VALUE self;
    VALUE dbc;
    struct dbc *dbcp;
    SQLHSTMT hstmt;
    int nump;
    PINFO *pinfo;
    int ncols;
    COLTYPE *coltypes;
    char **colnames;
    char **dbufs;
    int fetchc;
    int upc;
    int usef;
} STMT;

static VALUE Modbc;
static VALUE Cobj;
static VALUE Cenv;
static VALUE Cdbc;
static VALUE Cstmt;
static VALUE Ccolumn;
static VALUE Cparam;
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
#define INFO_SPECCOLS 9

/*
 * Modes for make_result
 */

#define MAKERES_BLOCK   1
#define MAKERES_NOCLOSE 2
#define MAKERES_PREPARE 4
#define MAKERES_EXECD   8

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

static SQLRETURN callsql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt,
			 SQLRETURN ret, char *m);

static VALUE stmt_exec(int argc, VALUE *argv, VALUE self);
static VALUE stmt_each(VALUE self);
static VALUE stmt_each_hash(int argc, VALUE *argv, VALUE self);
static VALUE stmt_close(VALUE self);
static VALUE stmt_drop(VALUE self);

/*
 * Macro to align buffers.
 */

#define LEN_ALIGN(x) \
    ((x) + sizeof (double) - (((x) + sizeof (double)) % sizeof (double)))


/*
 *----------------------------------------------------------------------
 *
 *      UNICODE converters et.al.
 *
 *----------------------------------------------------------------------
 */

#ifdef UNICODE

static int
uc_strlen(SQLWCHAR *str)
{
    int len = 0;

    if (str) {
	while (*str) {
	    ++len;
	    ++str;
	}
    }
    return len;
}

static SQLWCHAR *
uc_strchr(SQLWCHAR *str, SQLWCHAR c)
{
    if (str) {
	while (*str && *str != c) {
	    ++str;
	}
	str = *str == c ? str : NULL;
    }
    return str;
}

static int
mkutf(char *dest, SQLWCHAR *src, int len)
{      
    int i;
    char *cp = dest;

    for (i = 0; i < len; i++) {
	unsigned long c = src[i];

	if (c < 0xc0) {
	    *cp++ = c;
	} else if (c < 0x800) {
	    *cp++ = 0xc0 | ((c >> 6) & 0x1f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x10000) {
	    *cp++ = 0xe0 | ((c >> 12) & 0x0f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x200000) {
	    *cp++ = 0xf0 | ((c >> 18) & 0x07);
	    *cp++ = 0x80 | ((c >> 12) & 0x3f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x4000000) {
	    *cp++ = 0xf8 | ((c >> 24) & 0x03);
	    *cp++ = 0x80 | ((c >> 18) & 0x3f);
	    *cp++ = 0x80 | ((c >> 12) & 0x3f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	} else if (c < 0x80000000) {
	    *cp++ = 0xfc | ((c >> 31) & 0x01);
	    *cp++ = 0x80 | ((c >> 24) & 0x3f);
	    *cp++ = 0x80 | ((c >> 18) & 0x3f);
	    *cp++ = 0x80 | ((c >> 12) & 0x3f);
	    *cp++ = 0x80 | ((c >> 6) & 0x3f);
	    *cp++ = 0x80 | (c & 0x3f);
	}
    }
    *cp = '\0';
    return cp - dest;
}

static VALUE
uc_tainted_str_new(SQLWCHAR *str, int len)
{
    VALUE v;
    char *cp = xmalloc(len * 6 + 1);
    int ulen = 0;

    if (cp && str) {
	ulen = mkutf(cp, str, len);
    }
    v = rb_tainted_str_new(cp ? cp : "", ulen);
    if (cp) {
	xfree(cp);
    }
    return v;
}

static VALUE
uc_tainted_str_new2(SQLWCHAR *str)
{
    return uc_tainted_str_new(str, uc_strlen(str));
}

static VALUE
uc_str_new(SQLWCHAR *str, int len)
{
    VALUE v;
    char *cp = xmalloc(len * 6 + 1);
    int ulen = 0;

    if (cp && str) {
	ulen = mkutf(cp, str, len);
    }
    v = rb_str_new(cp ? cp : "", ulen);
    if (cp) {
	xfree(cp);
    }
    return v;
}

static VALUE
uc_str_new2(SQLWCHAR *str)
{
    return uc_str_new(str, uc_strlen(str));
}

static VALUE
uc_str_cat(VALUE v, SQLWCHAR *str, int len)
{
    VALUE vv = v;
    char *cp = xmalloc(len * 6 + 1);
    int ulen = 0;

    if (cp && str) {
	ulen = mkutf(cp, str, len);
    }
    if (cp) {
	vv = rb_str_cat(v, cp, ulen);
	xfree(cp);
    }
    return vv;
}

static SQLWCHAR *
uc_from_utf(unsigned char *str)
{
    SQLWCHAR *uc = NULL;

    if (str) {
	int i = 0, len = strlen(str);

	uc = ALLOC_N(SQLWCHAR, len + 1);
	if (uc) {
	    while (*str) {
		unsigned char c = str[0];

		if (c < 0xc0) {
		    uc[i++] = c;
		    ++str;
		} else if (c < 0xe0) {
		    if ((str[1] & 0xc0) == 0x80) {
			unsigned long t = ((c & 0x1f) << 6) | (str[1] & 0x3f);

			uc[i++] = t;
			str += 2;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else if (c < 0xf0) {
		    if ((str[1] & 0xc0) == 0x80 && (str[2] & 0xc0) == 0x80) {
			unsigned long t = ((c & 0x0f) << 12) |
			    ((str[1] << 6) & 0x3f) | (str[2] & 0x3f);

			uc[i++] = t;
			str += 3;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else if (c < 0xf8) {
		    if ((str[1] & 0xc0) == 0x80 && (str[2] & 0xc0) == 0x80 &&
			(str[3] & 0xc0) == 0x80) {
			unsigned long t = ((c & 0x03) << 18) |
			    ((str[1] << 12) & 0x3f) | ((str[2] << 6) & 0x3f) |
			    (str[4] & 0x3f);

			uc[i++] = t;
			str += 4;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else if (c < 0xfc) {
		    if ((str[1] & 0xc0) == 0x80 && (str[2] & 0xc0) == 0x80 &&
			(str[3] & 0xc0) == 0x80 && (str[4] & 0xc0) == 0x80) {
			unsigned long t = ((c & 0x01) << 24) |
			    ((str[1] << 18) & 0x3f) | ((str[2] << 12) & 0x3f) |
			    ((str[4] << 6) & 0x3f) | (str[5] & 0x3f);

			uc[i++] = t;
			str += 5;
		    } else {
			uc[i++] = c;
			++str;
		    }
		} else {
		    /* ignore */
		    ++str;
		}
	    }
	    uc[i] = 0;
	}
    }
    return uc;
}

static void
uc_free(SQLWCHAR *str)
{
    if (str) {
	xfree(str);
    }
}

#endif


/*
 *----------------------------------------------------------------------
 *
 *      Things for ODBC::DSN
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
 *      Things for ODBC::Driver
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
 *      Cleanup routines and GC mark/free callbacks.
 *
 *----------------------------------------------------------------------
 */

static void
list_init(LINK *link, int offs)
{
    link->succ = link->pred = link->head = NULL;
    link->offs = offs;
}

static void
list_add(LINK *link, LINK *head)
{
    if (link->head) {
	rb_fatal("RubyODBC: already in list");
    }
    if (head == NULL) {
	rb_fatal("RubyODBC: invalid list head");
    }
    link->head = head;
    link->pred = NULL;
    link->succ = head->succ;
    head->succ = link;
    if (link->succ) {
	link->succ->pred = link;
    }
}

static void
list_del(LINK *link)
{
    if (link == NULL) {
	rb_fatal("RubyODBC: invalid list item");
    }
    if (link->head == NULL) {
	rb_fatal("RubyODBC: item not in list");
    }
    if (link->succ) {
	link->succ->pred = link->pred;
    }
    if (link->pred) {
	link->pred->succ = link->succ;
    } else {
	link->head->succ = link->succ;
    }
    link->succ = link->pred = link->head = NULL;
}

static void *
list_first(LINK *head)
{
    if (head->succ == NULL) {
	return NULL;
    }
    return (void *) ((char *) head->succ - head->offs);
}

static int
list_empty(LINK *head)
{
    return head->succ == NULL; 
}

static void
free_env(ENV *e)
{
    e->self = Qnil;
    if (!list_empty(&e->dbcs)) {
	return;
    }
    tracemsg(2, fprintf(stderr, "ObjFree: ENV %p\n", e););
    if (e->henv != SQL_NULL_HENV) {
	callsql(SQL_NULL_HENV, e->henv, SQL_NULL_HSTMT,
		SQLFreeEnv(e->henv), "SQLFreeEnv");
	e->henv = SQL_NULL_HENV;
    }
    xfree(e);
}

static void
link_dbc(DBC *p, ENV *e)
{
    p->envp = e;
    list_add(&p->link, &e->dbcs);
}

static void
unlink_dbc(DBC *p)
{
    if (p == NULL) {
	return;
    }
    p->env = Qnil;
    if (p->envp) {
	ENV *e = p->envp;

	list_del(&p->link);
	if (e->self == Qnil) {
	    free_env(e);
	}
	p->envp = NULL;
    }
}

static void
free_dbc(DBC *p)
{
    p->self = p->env = Qnil;
    if (!list_empty(&p->stmts)) {
	return;
    }
    tracemsg(2, fprintf(stderr, "ObjFree: DBC %p\n", p););
    if (p->hdbc != SQL_NULL_HDBC) {
	callsql(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		SQLDisconnect(p->hdbc), "SQLDisconnect");
	callsql(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		SQLFreeConnect(p->hdbc), "SQLFreeConnect");
	p->hdbc = SQL_NULL_HDBC;
    }
    unlink_dbc(p);
    xfree(p);
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
link_stmt(STMT *q, DBC *p)
{
    q->dbcp = p;
    list_add(&q->link, &p->stmts);
}

static void
unlink_stmt(STMT *q)
{
    if (q == NULL) {
	return;
    }
    q->dbc = Qnil;
    if (q->dbcp) {
	DBC *p = q->dbcp;

	list_del(&q->link);
	if (p->self == Qnil) {
	    free_dbc(p);
	}
	q->dbcp = NULL;
    }
}

static void
free_stmt(STMT *q)
{
    q->self = q->dbc = Qnil;
    free_stmt_sub(q);
    tracemsg(2, fprintf(stderr, "ObjFree: STMT %p\n", q););
    if (q->hstmt != SQL_NULL_HSTMT) {
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	q->hstmt = SQL_NULL_HSTMT;
    }
    unlink_stmt(q);
    xfree(q);
}

static void
start_gc()
{
    rb_funcall(rb_mGC, rb_intern("start"), 0, NULL);
}

static void
mark_dbc(DBC *p)
{
    if (p->env != Qnil) {
	rb_gc_mark(p->env);
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
    VALUE a, v = rb_str_new2("INTERN (0) [RubyODBC]");

    v = rb_str_cat2(v, msg);
    a = rb_ary_new2(1);
    rb_ary_push(a, rb_obj_taint(v));
    CVAR_SET(Cobj, rb_intern("@@error"), a);
    return STR2CSTR(v);
}

/*
 *----------------------------------------------------------------------
 *
 *      Functions to retrieve last SQL error or warning.
 *
 *----------------------------------------------------------------------
 */

static char *
get_err_or_info(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, int isinfo)
{
#ifdef UNICODE
    SQLWCHAR msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#else
    char msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#endif
    char buf[32], tmp[SQL_MAX_MESSAGE_LENGTH];
    SQLRETURN err;
    SQLINTEGER nativeerr;
    SQLSMALLINT len;
    VALUE v0 = Qnil, a = Qnil, v;
    int done = 0;

    while (!done) {
	v = Qnil;
	err = tracesql(henv, hdbc, hstmt,
		       SQLError(henv, hdbc, hstmt, state, &nativeerr, msg,
		       SQL_MAX_MESSAGE_LENGTH - 1, &len),
		       "SQLError");
	state[6] = '\0';
	msg[SQL_MAX_MESSAGE_LENGTH - 1] = '\0';
	switch (err) {
	case SQL_SUCCESS:
#ifdef UNICODE
	    v = uc_str_new2(state);
#else
	    v = rb_str_new2(state);
#endif
	    sprintf(buf, " (%d) ", (int) nativeerr);
	    v = rb_str_cat2(v, buf);
#ifdef UNICODE
	    v = uc_str_cat(v, msg, len);
#else
	    v = rb_str_cat(v, msg, len);
#endif
	    break;
	case SQL_NO_DATA:
	    if (v0 == Qnil && !isinfo) {
		v = rb_str_new2("INTERN (0) [RubyODBC]No data found");
	    } else {
		v = Qnil;
	    }
	    done = 1;
	    break;
	case SQL_INVALID_HANDLE:
	    v = rb_str_new2("INTERN (0) [RubyODBC]Invalid handle");
	    done = 1;
	    break;
	case SQL_ERROR:
	    v = rb_str_new2("INTERN (0) [RubyODBC]Error reading error message");
	    done = 1;
	    break;
	default:
	    sprintf(tmp, "INTERN (0) [RubyODBC]Unknown error %d", err);
	    v = rb_str_new2(tmp);
	    done = 1;
	    break;
	}
	if (v != Qnil) {
	    if (v0 == Qnil) {
		v0 = v;
		a = rb_ary_new();
	    }
	    rb_ary_push(a, rb_obj_taint(v));
	    tracemsg(1, fprintf(stderr, "  | %s\n", STR2CSTR(v)););
	}
    }
    CVAR_SET(Cobj, rb_intern(isinfo ? "@@info" : "@@error"), a);
    if (isinfo) {
	return NULL;
    }
    return (v0 == Qnil) ? NULL : STR2CSTR(v0);
}

static char *
get_err(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt)
{
    return get_err_or_info(henv, hdbc, hstmt, 0);
}

#ifdef TRACING
static void
trace_sql_ret(SQLRETURN ret)
{
    char msg[32], *p;

    switch (ret) {
    case SQL_SUCCESS:
	p = "SQL_SUCCESS";
	break;
    case SQL_SUCCESS_WITH_INFO:
	p = "SQL_SUCCESS_WITH_INFO";
	break;
    case SQL_NO_DATA:
	p = "SQL_NO_DATA";
	break;
    case SQL_ERROR:
	p = "SQL_ERROR";
	break;
    case SQL_INVALID_HANDLE:
	p = "SQL_INVALID_HANDLE";
	break;
    default:
	sprintf(msg, "SQL_RETURN=%d", ret);
	p = msg;
	break;
    }
    fprintf(stderr, "  < %s\n", p);
}

static SQLRETURN
tracesql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret, char *m)
{
    if (tracing & 1) {
	fprintf(stderr, "SQLCall: %s", m);
	fprintf(stderr, "\n  > HENV=0x%lx, HDBC=0x%lx, HSTMT=0x%lx\n",
		(long) henv, (long) hdbc, (long) hstmt);
	trace_sql_ret(ret);
    }
    return ret;
}
#endif

static SQLRETURN
callsql(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret, char *m)
{
    SQLRETURN err;

    err = tracesql(henv, hdbc, hstmt, ret, m);
    if (err != SQL_SUCCESS) {
#ifdef UNICODE
	SQLWCHAR msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#else
	char msg[SQL_MAX_MESSAGE_LENGTH], state[6 + 1];
#endif
	SQLINTEGER nativeerr;
	SQLSMALLINT len;
	int done = 0;

	while (!done) {
	    err = tracesql(henv, hdbc, hstmt,
			   SQLError(henv, hdbc, hstmt, state, &nativeerr, msg,
				    SQL_MAX_MESSAGE_LENGTH - 1, &len),
			   "SQLError");
	    switch (err) {
	    case SQL_SUCCESS:
		break;
	    case SQL_NO_DATA:
	    case SQL_INVALID_HANDLE:
	    case SQL_ERROR:
	    default:
		done = 1;
		break;
	    }
	}
    }
    return ret;
}

static int
succeeded(SQLHENV henv, SQLHDBC hdbc, SQLHSTMT hstmt, SQLRETURN ret,
	  char **msgp, char *m, ...)
{
#ifdef TRACING
    va_list args;

    if (tracing & 1) {
	va_start(args, m);
	fprintf(stderr, "SQLCall: ");
	vfprintf(stderr, m, args);
	va_end(args);
	fprintf(stderr, "\n  > HENV=0x%lx, HDBC=0x%lx, HSTMT=0x%lx\n",
		(long) henv, (long) hdbc, (long) hstmt);
	trace_sql_ret(ret);
    }
#endif
    if (!SQL_SUCCEEDED(ret)) {
	char *dummy;

	if (msgp == NULL) {
	    msgp = &dummy;
	}
	*msgp = get_err_or_info(henv, hdbc, hstmt, 0);
	return 0;
    }
    if (ret == SQL_SUCCESS_WITH_INFO) {
	get_err_or_info(henv, hdbc, hstmt, 1);
    } else {
	CVAR_SET(Cobj, rb_intern("@@info"), Qnil);
    }
    return 1;
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
    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	self = q->dbc;
	if (self == Qnil) {
	    rb_raise(Cerror, set_err("Stale ODBC::Statement"));
	}
    }
    if (rb_obj_is_kind_of(self, Cdbc) == Qtrue) {
	DBC *p;

	Data_Get_Struct(self, DBC, p);
	self = p->env;
	if (self == Qnil) {
	    rb_raise(Cerror, set_err("Stale ODBC::Database"));
	}
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

    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	self = q->dbc;
	if (self == Qnil) {
	    rb_raise(Cerror, set_err("Stale ODBC::Statement"));
	}
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
    VALUE a, v;
    char buf[SQL_MAX_MESSAGE_LENGTH + 1], *p;

    if (TYPE(msg) == T_STRING) {
	v = msg;
    } else {
	v = rb_any_to_s(msg);
    }
    strcpy(buf, "INTERN (1) [RubyODBC]");
    p = STR2CSTR(v);
    strncat(buf, p, SQL_MAX_MESSAGE_LENGTH - strlen(buf));
    buf[SQL_MAX_MESSAGE_LENGTH] = '\0';
    v = rb_str_new2(buf);
    a = rb_ary_new2(1);
    rb_ary_push(a, rb_obj_taint(v));
    CVAR_SET(Cobj, rb_intern("@@error"), a);
    rb_raise(Cerror, "%s", buf);
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
env_new(VALUE self)
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
    if (!SQL_SUCCEEDED(SQLAllocEnv(&henv)) || henv == SQL_NULL_HENV) {
	rb_raise(Cerror, set_err("Cannot allocate SQLHENV"));
    }
    obj = Data_Make_Struct(self, ENV, NULL, free_env, e);
    tracemsg(2, fprintf(stderr, "ObjAlloc: ENV %p\n", e););
    e->self = obj;
    e->henv = henv;
    list_init(&e->dbcs, offsetof(DBC, link));
#if defined(UNICODE) && defined(SQL_OV_ODBC3)
    callsql(henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
	    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
			  (SQLPOINTER) SQL_OV_ODBC3, 0),
	    "SQLSetEnvAttr(SQL_OV_ODBC3)");
#endif
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
#ifdef UNICODE
    SQLWCHAR dsn[SQL_MAX_DSN_LENGTH], descr[SQL_MAX_MESSAGE_LENGTH * 2];
#else
    char dsn[SQL_MAX_DSN_LENGTH], descr[SQL_MAX_MESSAGE_LENGTH * 2];
#endif
    SQLSMALLINT dsnLen = 0, descrLen = 0;
    int first = 1;
    VALUE env, aret;
    ENV *e;

    env = env_new(Cenv);
    Data_Get_Struct(env, ENV, e);
    aret = rb_ary_new();
    while (succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		     SQLDataSources(e->henv, (SQLUSMALLINT) (first ?
				    SQL_FETCH_FIRST : SQL_FETCH_NEXT),
				    (SQLTCHAR *) dsn,
				    (SQLSMALLINT) sizeof (dsn), &dsnLen,
				    (SQLTCHAR *) descr,
				    (SQLSMALLINT) sizeof (descr),
				    &descrLen),
		     NULL, "SQLDataSources")) {
	VALUE odsn = rb_obj_alloc(Cdsn);

#ifdef UNICODE
	dsnLen = (dsnLen == 0) ? uc_strlen(dsn) : (dsnLen / sizeof (SQLWCHAR));
	descrLen = (descrLen == 0) ?
	    uc_strlen(descr) : (descrLen / sizeof (SQLWCHAR));
	rb_iv_set(odsn, "@name", uc_tainted_str_new(dsn, dsnLen));
	rb_iv_set(odsn, "@descr", uc_tainted_str_new(descr, descrLen));
#else
	dsnLen = (dsnLen == 0) ? strlen(dsn) : dsnLen;
	descrLen = (descrLen == 0) ? strlen(descr) : descrLen;
	rb_iv_set(odsn, "@name", rb_tainted_str_new(dsn, dsnLen));
	rb_iv_set(odsn, "@descr", rb_tainted_str_new(descr, descrLen));
#endif
	rb_ary_push(aret, odsn);
	first = dsnLen = descrLen = 0;
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
#ifdef UNICODE
    SQLWCHAR driver[SQL_MAX_MESSAGE_LENGTH], attrs[SQL_MAX_MESSAGE_LENGTH * 2];
    SQLWCHAR *attr;
#else
    char driver[SQL_MAX_MESSAGE_LENGTH], attrs[SQL_MAX_MESSAGE_LENGTH * 2];
    char *attr;
#endif
    SQLSMALLINT driverLen = 0, attrsLen = 0;
    int first = 1;
    VALUE env, aret;
    ENV *e;

    env = env_new(Cenv);
    Data_Get_Struct(env, ENV, e);
    aret = rb_ary_new();
    while (succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		     SQLDrivers(e->henv, (SQLUSMALLINT) (first ?
				SQL_FETCH_FIRST : SQL_FETCH_NEXT),
				(SQLTCHAR *) driver,
				(SQLSMALLINT) sizeof (driver), &driverLen,
				(SQLTCHAR *) attrs,
				(SQLSMALLINT) sizeof (attrs), &attrsLen),
		     NULL, "SQLDrivers")) {
	VALUE odrv = rb_obj_alloc(Cdrv);
	VALUE h = rb_hash_new();
	int count = 0;

#ifdef UNICODE
	driverLen = (driverLen == 0) ?
	    uc_strlen(driver) : (driverLen / sizeof (SQLWCHAR));
	rb_iv_set(odrv, "@name", uc_tainted_str_new(driver, driverLen));
	for (attr = attrs; *attr; attr += uc_strlen(attr) + 1) {
	    SQLWCHAR *p = uc_strchr(attr, (SQLWCHAR) '=');

	    if (p != NULL && p != attr) {
		rb_hash_aset(h,
			     uc_tainted_str_new(attr, (p - attr) /
						 sizeof (SQLWCHAR)),
			     uc_tainted_str_new2(p + 1));
		count++;
	    }
	}
#else
	driverLen = (driverLen == 0) ? strlen(driver) : driverLen;
	rb_iv_set(odrv, "@name", rb_tainted_str_new(driver, driverLen));
	for (attr = attrs; *attr; attr += strlen(attr) + 1) {
	    char *p = strchr(attr, '=');

	    if (p != NULL && p != attr) {
		rb_hash_aset(h, rb_tainted_str_new(attr, p - attr),
			     rb_tainted_str_new2(p + 1));
		count++;
	    }
	}
#endif
	if (count) {
	    rb_iv_set(odrv, "@attrs", h);
	}
	rb_ary_push(aret, odrv);
	first = driverLen = attrsLen = 0;
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

#ifdef HAVE_ODBCINST_H
static VALUE
conf_dsn(int argc, VALUE *argv, VALUE self, int op)
{
    VALUE drv, attr, issys, astr;
#if defined(UNICODE) && defined(HAVE_SQLCONFIGDATASOURCEW)
    SQLWCHAR *sdrv, *sastr;
#else
    char *sdrv, *sastr;
#endif

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
#if defined(UNICODE) && defined(HAVE_SQLCONFIGDATASOURCEW)
    sdrv = uc_from_utf(STR2CSTR(drv));
    sastr = uc_from_utf(STR2CSTR(astr));
    if (!sdrv || !sastr) {
	uc_free(sdrv);
	uc_free(sastr);
	rb_raise(Cerror, set_err("Out of memory"));
    }
    if (SQLConfigDataSourceW(NULL, (WORD) op, sdrv, sastr)) {
	uc_free(sdrv);
	uc_free(sastr);
	return Qnil;
    }
    uc_free(sdrv);
    uc_free(sastr);
#else
    sdrv = STR2CSTR(drv);
    sastr = STR2CSTR(astr);
    if (SQLConfigDataSource(NULL, (WORD) op, sdrv, sastr)) {
	return Qnil;
    }
#endif
    rb_raise(Cerror, set_err("DSN configuration error"));
    return Qnil;
}
#endif

static VALUE
dbc_adddsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    return conf_dsn(argc, argv, self, ODBC_ADD_DSN);
#else
    rb_raise(Cerror, set_err("ODBC::add_dsn not supported"));
    return Qnil;
#endif
}

static VALUE
dbc_confdsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    return conf_dsn(argc, argv, self, ODBC_CONFIG_DSN);
#else
    rb_raise(Cerror, set_err("ODBC::config_dsn not supported"));
    return Qnil;
#endif
}

static VALUE
dbc_deldsn(int argc, VALUE *argv, VALUE self)
{
#ifdef HAVE_ODBCINST_H
    return conf_dsn(argc, argv, self, ODBC_REMOVE_DSN);
#else
    rb_raise(Cerror, set_err("ODBC::del_dsn not supported"));
    return Qnil;
#endif
}

/*
 *----------------------------------------------------------------------
 *
 *      Return last ODBC error or warning.
 *
 *----------------------------------------------------------------------
 */

static VALUE
dbc_error(VALUE self)
{
    return rb_cvar_get(Cobj, rb_intern("@@error"));
}

static VALUE
dbc_warn(VALUE self)
{
    return rb_cvar_get(Cobj, rb_intern("@@info"));
}

/*
 *----------------------------------------------------------------------
 *
 *      Connection instance initializer.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
dbc_alloc(VALUE self)
{
    DBC *p;
    VALUE obj = Data_Make_Struct(self, DBC, mark_dbc, free_dbc, p);

    tracemsg(2, fprintf(stderr, "ObjAlloc: DBC %p\n", p););
    list_init(&p->link, offsetof(DBC, link));
    p->self = obj;
    p->env = Qnil;
    p->envp = NULL;
    list_init(&p->stmts, offsetof(STMT, link));
    p->hdbc = SQL_NULL_HDBC;
    return obj;
}
#endif

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
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    obj = rb_obj_alloc(Cdbc);
    Data_Get_Struct(obj, DBC, p);
    p->env = env;
#else
    obj = Data_Make_Struct(self, DBC, mark_dbc, free_dbc, p);
    tracemsg(2, fprintf(stderr, "ObjAlloc: DBC %p\n", p););
    list_init(&p->link, offsetof(DBC, link));
    p->self = obj;
    p->env = env;
    p->envp = NULL;
    list_init(&p->stmts, offsetof(STMT, link));
    p->hdbc = SQL_NULL_HDBC;
    p->upc = 0;
#endif
    if (env != Qnil) {
	ENV *e;

	Data_Get_Struct(env, ENV, e);
	link_dbc(p, e);
    }
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
#ifdef UNICODE
    SQLWCHAR *sdsn = NULL, *suser = NULL, *spasswd = NULL;
#else
    char *sdsn, *suser = "", *spasswd = "";
#endif
    char *msg;
    SQLHDBC dbc;

    rb_scan_args(argc, argv, "12", &dsn, &user, &passwd);
    if (rb_obj_is_kind_of(dsn, Cdsn) == Qtrue) {
	dsn = rb_iv_get(dsn, "@name");
    }
    Check_Type(dsn, T_STRING);
    if (user != Qnil) {
	Check_Type(user, T_STRING);
    }
    if (passwd != Qnil) {
	Check_Type(passwd, T_STRING);
    }
    p = get_dbc(self);
    if (p->hdbc != SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("Already connected"));
    }
    if (p->env == Qnil) {
	p->env = env_new(Cenv);
	e = get_env(p->env);
	link_dbc(p, e);
    } else {
	e = get_env(p->env);
    }
#ifdef UNICODE
    if (user != Qnil) {
	suser = uc_from_utf(STR2CSTR(user));
    } else {
	suser = uc_from_utf("");
    }
    if (passwd != Qnil) {
	spasswd = uc_from_utf(STR2CSTR(passwd));
    } else {
	spasswd = uc_from_utf("");
    }
    sdsn = uc_from_utf(STR2CSTR(dsn));
    if (!suser || !spasswd || !sdsn) {
	uc_free(sdsn);
	uc_free(suser);
	uc_free(spasswd);
	rb_raise(Cerror, set_err("Out of memory"));
    }
#else
    if (user != Qnil) {
	suser = STR2CSTR(user);
    }
    if (passwd != Qnil) {
	spasswd = STR2CSTR(passwd);
    }
    sdsn = STR2CSTR(dsn);
#endif
    if (!succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		   SQLAllocConnect(e->henv, &dbc), &msg, "SQLAllocConnect")) {
#ifdef UNICODE
	uc_free(sdsn);
	uc_free(suser);
	uc_free(spasswd);
#endif
	rb_raise(Cerror, "%s", msg);
    }
    if (!succeeded(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT,
		   SQLConnect(dbc, (SQLTCHAR *) sdsn, SQL_NTS,
			      (SQLTCHAR *) suser, SQL_NTS,
			      (SQLTCHAR *) spasswd, SQL_NTS),
		   &msg,
		   "SQLConnect('%s')", sdsn)) {
#ifdef UNICODE
	uc_free(sdsn);
	uc_free(suser);
	uc_free(spasswd);
#endif
	callsql(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT,
		SQLFreeConnect(dbc), "SQLFreeConnect");
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    uc_free(sdsn);
    uc_free(suser);
    uc_free(spasswd);
#endif
    p->hdbc = dbc;
    return self;
}

static VALUE
dbc_drvconnect(VALUE self, VALUE drv)
{
    ENV *e;
    DBC *p;
#ifdef UNICODE
    SQLWCHAR *sdrv;
#else
    char *sdrv;
#endif
    char *msg;
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
    p = get_dbc(self);
    if (p->hdbc != SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("Already connected"));
    }
    if (p->env == Qnil) {
	p->env = env_new(Cenv);
	e = get_env(p->env);
	link_dbc(p, e);
    } else {
	e = get_env(p->env);
    }
#ifdef UNICODE
    sdrv = uc_from_utf(STR2CSTR(drv));
    if (!sdrv) {
	rb_raise(Cerror, set_err("Out of memory"));
    }
#else
    sdrv = STR2CSTR(drv);
#endif
    if (!succeeded(e->henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		   SQLAllocConnect(e->henv, &dbc), &msg, "SQLAllocConnect")) {
#ifdef UNICODE
	uc_free(sdrv);
#endif
	rb_raise(Cerror, "%s", msg);
    }
    if (!succeeded(e->henv, dbc, SQL_NULL_HSTMT,
		   SQLDriverConnect(dbc, NULL, (SQLTCHAR *) sdrv, SQL_NTS,
				    NULL, 0, NULL, SQL_DRIVER_NOPROMPT),
		   &msg,
		   "SQLDriverConnect")) {
#ifdef UNICODE
	uc_free(sdrv);
#endif
	callsql(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT,
		SQLFreeConnect(dbc), "SQLFreeConnect");
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    uc_free(sdrv);
#endif
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

    while (!list_empty(&p->stmts)) {
	STMT *q = list_first(&p->stmts);

	if (q->self == Qnil) {
	    rb_fatal("RubyODBC: invalid stmt in dropall");
	}
	stmt_drop(q->self);
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
dbc_disconnect(int argc, VALUE *argv, VALUE self)
{
    DBC *p = get_dbc(self);
    VALUE nodrop = Qfalse;
    char *msg;

    rb_scan_args(argc, argv, "01", &nodrop);
    if (!RTEST(nodrop)) {
	dbc_dropall(self);
    }
    if (p->hdbc == SQL_NULL_HDBC) {
	return Qtrue;
    }
    if (list_empty(&p->stmts)) {
	callsql(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		SQLDisconnect(p->hdbc), "SQLDisconnect");
	if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		       SQLFreeConnect(p->hdbc), &msg, "SQLFreeConnect")) {
	    rb_raise(Cerror, "%s", msg);
	}
	p->hdbc = SQL_NULL_HDBC;
	unlink_dbc(p);
	start_gc();
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
make_coltypes(SQLHSTMT hstmt, int ncols, char **msgp)
{
    int i;
    COLTYPE *ret = NULL;
    SQLINTEGER type, size;
    
    for (i = 0; i < ncols; i++) {
	SQLUSMALLINT ic = i + 1;

	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLColAttributes(hstmt, ic,
					SQL_COLUMN_TYPE, NULL, 0, NULL,
					&type),
		       msgp,
		       "SQLColAttributes(SQL_COLUMN_TYPE)")) {
	    return ret;
	}
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLColAttributes(hstmt, ic,
					SQL_COLUMN_DISPLAY_SIZE,
					NULL, 0, NULL, &size),
		       msgp,
		       "SQLColAttributes(SQL_COLUMN_DISPLAY_SIZE)")) {
	    return ret;
	}
    }
    ret = ALLOC_N(COLTYPE, ncols);
    if (ret == NULL) {
	if (msgp != NULL) {
	    *msgp = set_err("Out of memory");
	}
	return NULL;
    }
    for (i = 0; i < ncols; i++) {
	SQLUSMALLINT ic = i + 1;

	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		SQLColAttributes(hstmt, ic,
				 SQL_COLUMN_TYPE,
				 NULL, 0, NULL, &type),
		"SQLColAttributes(SQL_COLUMN_TYPE)");
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		SQLColAttributes(hstmt, ic,
				 SQL_COLUMN_DISPLAY_SIZE,
				 NULL, 0, NULL, &size),
		"SQLColAttributes(SQL_COLUMN_DISPLAY_SIZE)");
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
#ifdef SQL_TYPE_DATE
	case SQL_TYPE_DATE:
#endif
	    type = SQL_C_DATE;
	    size = sizeof (DATE_STRUCT);
	    break;
	case SQL_TIME:
#ifdef SQL_TYPE_TIME
	case SQL_TYPE_TIME:
#endif
	    type = SQL_C_TIME;
	    size = sizeof (TIME_STRUCT);
	    break;
	case SQL_TIMESTAMP:
#ifdef SQL_TYPE_TIMESTAMP
	case SQL_TYPE_TIMESTAMP:
#endif
	    type = SQL_C_TIMESTAMP;
	    size = sizeof (TIMESTAMP_STRUCT);
	    break;
	case SQL_LONGVARBINARY:
	case SQL_LONGVARCHAR:
#ifdef UNICODE
	case SQL_WLONGVARCHAR:
	    type = SQL_C_WCHAR;
#else
	    type = SQL_C_CHAR;
#endif
	    size = SQL_NO_TOTAL;
	    break;
	default:
#ifdef UNICODE
	    type = SQL_C_WCHAR;
	    if (size != SQL_NO_TOTAL) {
		size *= sizeof (SQLWCHAR);
		size += sizeof (SQLWCHAR);
	    }
#else
	    type = SQL_C_CHAR;
	    if (size != SQL_NO_TOTAL) {
		size += 1;
	    }
#endif
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
make_pinfo(SQLHSTMT hstmt, int nump, char **msgp)
{
    int i;
    PINFO *pinfo = NULL;

    pinfo = ALLOC_N(PINFO, nump);
    if (pinfo == NULL) {
	if (msgp != NULL) {
	    *msgp = set_err("Out of memory");
	}
	return NULL;
    }
    for (i = 0; i < nump; i++) {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLDescribeParam(hstmt, (SQLUSMALLINT) (i + 1),
					&pinfo[i].type, &pinfo[i].coldef,
					&pinfo[i].scale,
					&pinfo[i].nullable),
		       NULL,
		       "SQLDescribeParam")) {
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
    char *msg = NULL;

    Data_Get_Struct(dbc, DBC, p);
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		   SQLNumParams(hstmt, &nump), NULL, "SQLNumParams")) {
	nump = 0;
    }
    if (nump > 0) {
	pinfo = make_pinfo(hstmt, nump, &msg);
	if (pinfo == NULL) {
	    goto error;
	}
    }
    if ((mode & MAKERES_PREPARE) ||
	!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		   SQLNumResultCols(hstmt, &cols), NULL, "SQLNumResultCols")) {
	cols = 0;
    }
    if (cols > 0) {
	coltypes = make_coltypes(hstmt, cols, &msg);
	if (coltypes == NULL) {
	    goto error;
	}
    }
    if (result == Qnil) {
	result = Data_Make_Struct(Cstmt, STMT, mark_stmt, free_stmt, q);
	tracemsg(2, fprintf(stderr, "ObjAlloc: STMT %p\n", q););
	list_init(&q->link, offsetof(STMT, link));
	q->self = result;
	q->dbc = Qnil;
	q->dbcp = NULL;
	q->pinfo = NULL;
	q->coltypes = NULL;
	q->colnames = q->dbufs = NULL;
	q->fetchc = 0;
	q->upc = p->upc;
	q->usef = 0;
	rb_iv_set(q->self, "@_a", rb_ary_new());
	rb_iv_set(q->self, "@_h", rb_hash_new());
	q->dbc = dbc;
	link_stmt(q, p);
    } else {
	Data_Get_Struct(result, STMT, q);
	free_stmt_sub(q);
	if (q->dbc != dbc) {
	    unlink_stmt(q);
	    q->dbc = dbc;
	    link_stmt(q, p);
	}
    }
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
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
	    SQLFreeStmt(hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
    if (result != Qnil) {
	Data_Get_Struct(result, STMT, q);
	if (q->hstmt == hstmt) {
	    q->hstmt = SQL_NULL_HSTMT;
	    unlink_stmt(q);
	}
    }
    if (pinfo) {
	xfree(pinfo);
    }
    if (coltypes) {
	xfree(coltypes);
    }
    rb_raise(Cerror, "%s", msg);
    return Qnil;
}

static char *
upcase_if(char *string, int upc)
{
    if (upc && string) {
	unsigned char *p = string;

	while (*p) {
#ifdef UNICODE
	    if (*p < 0x80 && ISLOWER(*p))
#else
	    if (ISLOWER(*p))
#endif
	    {
		*p = toupper(*p);
	    }
	    ++p;
	}
    }
    return string;
}

/*
 *----------------------------------------------------------------------
 *
 *      Constructor: make column from statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_col(SQLHSTMT hstmt, int i, int upc)
{
    VALUE obj, v;
    SQLUSMALLINT ic = i + 1;
    SQLINTEGER iv;
#ifdef UNICODE
    SQLWCHAR name[SQL_MAX_MESSAGE_LENGTH];
#else
    char name[SQL_MAX_MESSAGE_LENGTH];
#endif
    char *msg;
    SQLSMALLINT name_len;

    name[0] = 0;
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		   SQLColAttributes(hstmt, ic, SQL_COLUMN_LABEL, name,
				    (SQLSMALLINT) sizeof (name),
				    &name_len, NULL),
		   &msg,
		   "SQLColAttributes(SQL_COLUMN_LABEL)")) {
	rb_raise(Cerror, "%s", msg);
    }
    obj = rb_obj_alloc(Ccolumn);
    if (name_len >= sizeof (name)) {
	name_len = sizeof (name) - 1;
    }
    if (name_len > 0) {
	name[name_len / sizeof (name[0])] = 0;
    }
#ifdef UNICODE
    if (upc) {
	int len = uc_strlen(name);
	char tmpbuf[1];
	char *tmp = xmalloc(len);

	if (!tmp) {
	    tmp = tmpbuf;
	    len = 0;
	}
	mkutf(tmp, name, len);
	rb_iv_set(obj, "@name", rb_tainted_str_new2(upcase_if(tmp, 1)));
	if (tmp && tmp != tmpbuf) {
	    xfree(tmp);
	}
    } else {
	rb_iv_set(obj, "@name", uc_tainted_str_new2(name));
    }
#else
    rb_iv_set(obj, "@name", rb_tainted_str_new2(upcase_if(name, upc)));
#endif
    v = Qnil;
    name[0] = 0;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_TABLE_NAME, name,
				   (SQLSMALLINT) sizeof (name),
				   &name_len, NULL),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_TABLE_NAME)")) {
	if (name_len > sizeof (name)) {
	    name_len = sizeof (name) - 1;
	}
	if (name_len > 0) {
	    name[name_len / sizeof (name[0])] = 0;
	}
#ifdef UNICODE
	v = uc_tainted_str_new2(name);
#else
	v = rb_tainted_str_new2(name);
#endif
    }
    rb_iv_set(obj, "@table", v);
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_TYPE, NULL,
				   0, NULL, &iv),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_TYPE)")) {
	v = INT2NUM(iv);
    } else {
	v = INT2NUM(SQL_UNKNOWN_TYPE);
    }
    rb_iv_set(obj, "@type", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic,
#if (ODBCVER >= 0x0300)
				   SQL_DESC_LENGTH,
#else
				   SQL_COLUMN_LENGTH,
#endif
				   NULL, 0, NULL, &iv),
		  NULL,
#if (ODBCVER >= 0x0300)
		  "SQLColAttributes(SQL_DESC_LENGTH)"
#else
		  "SQLColAttributes(SQL_COLUMN_LENGTH)"
#endif
	)) {
	v = INT2NUM(iv);
    } else if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
			 SQLColAttributes(hstmt, ic,
					  SQL_COLUMN_DISPLAY_SIZE, NULL,
					  0, NULL, &iv),
			 NULL,
			 "SQLColAttributes(SQL_COLUMN_DISPLAY_SIZE)")) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@length", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_NULLABLE, NULL,
				   0, NULL, &iv),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_NULLABLE)")) {
	v = (iv == SQL_NO_NULLS) ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@nullable", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_SCALE, NULL,
				   0, NULL, &iv),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_SCALE)")) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@scale", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_PRECISION, NULL,
				   0, NULL, &iv),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_PRECISION)")) {
	v = INT2NUM(iv);
    }
    rb_iv_set(obj, "@precision", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_SEARCHABLE, NULL,
				   0, NULL, &iv),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_SEARCHABLE)")) {
	v = (iv == SQL_NO_NULLS) ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@searchable", v);
    v = Qnil;
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		  SQLColAttributes(hstmt, ic, SQL_COLUMN_UNSIGNED, NULL,
				   0, NULL, &iv),
		  NULL,
		  "SQLColAttributes(SQL_COLUMN_UNSIGNED)")) {
	v = (iv == SQL_NO_NULLS) ? Qfalse : Qtrue;
    }
    rb_iv_set(obj, "@unsigned", v);
    return obj;
}

/*
 *----------------------------------------------------------------------
 *
 *      Constructor: make parameter from statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_par(STMT *q, int i)
{
    VALUE obj;
    int v;

    obj = rb_obj_alloc(Cparam);
    v = q->pinfo ? q->pinfo[i].type : SQL_VARCHAR;
    rb_iv_set(obj, "@type", INT2NUM(v));
    v = q->pinfo ? q->pinfo[i].coldef : 0;
    rb_iv_set(obj, "@precision", INT2NUM(v));
    v = q->pinfo ? q->pinfo[i].scale : 0;
    rb_iv_set(obj, "@scale", INT2NUM(v));
    v = q->pinfo ? q->pinfo[i].nullable : SQL_NULLABLE_UNKNOWN;
    rb_iv_set(obj, "@nullable", INT2NUM(v));
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
    VALUE which = Qnil, which2 = Qnil, which3 = Qnil;
#ifdef UNICODE
    SQLWCHAR *swhich = NULL, *swhich2 = NULL;
#else
    char *swhich = NULL, *swhich2 = NULL;
#endif
    char *msg, *argspec = NULL;
    SQLHSTMT hstmt;
    int needstr = 1, itype = SQL_ALL_TYPES;
    int iid = SQL_BEST_ROWID, iscope = SQL_SCOPE_CURROW;

    if (p->hdbc == SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("No connection"));
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
    case INFO_FORKEYS:
    case INFO_PROCCOLS:
	argspec = "11";
	break;
    case INFO_SPECCOLS:
	argspec = "12";
	break;
    default:
	rb_raise(Cerror, set_err("Invalid info mode"));
	break;
    }
    rb_scan_args(argc, argv, argspec, &which, &which2, &which3);
    if (which != Qnil) {
	if (needstr) {
	    Check_Type(which, T_STRING);
#ifdef UNICODE
	    swhich = (SQLWCHAR *) which;
#else
	    swhich = STR2CSTR(which);
#endif
	} else {
	    itype = NUM2INT(which);
	}
    }
    if (which2 != Qnil) {
	if (mode == INFO_SPECCOLS) {
	    iid = NUM2INT(which2);
	} else if (mode != INFO_INDEXES) {
	    Check_Type(which2, T_STRING);
#ifdef UNICODE
	    swhich2 = (SQLWCHAR *) which2;
#else
	    swhich2 = STR2CSTR(which2);
#endif
	}
    }
#ifdef UNICODE
    if (swhich) {
	swhich = uc_from_utf(STR2CSTR((VALUE) swhich));
	if (!swhich) {
	    rb_raise(Cerror, set_err("Out of memory"));
	}
    }
    if (swhich2) {
	swhich2 = uc_from_utf(STR2CSTR((VALUE) swhich2));
	if (!swhich2) {
	    uc_free(swhich);
	    rb_raise(Cerror, set_err("Out of memory"));
	}
    }
#endif
    if (which3 != Qnil) {
	iscope = NUM2INT(which3);
    }
    if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		   SQLAllocStmt(p->hdbc, &hstmt), &msg, "SQLAllocStmt")) {
#ifdef UNICODE
	uc_free(swhich);
	uc_free(swhich2);
#endif
	rb_raise(Cerror, "%s", msg);
    }
    switch (mode) {
    case INFO_TABLES:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLTables(hstmt, NULL, 0, NULL, 0,
				 swhich, SQL_NTS, NULL, 0),
		       &msg, "SQLTables")) {
	    goto error;
	}
	break;
    case INFO_COLUMNS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLColumns(hstmt, NULL, 0, NULL, 0,
				  swhich, SQL_NTS, NULL, 0),
		       &msg, "SQLColumns")) {
	    goto error;
	}
	break;
    case INFO_PRIMKEYS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLPrimaryKeys(hstmt, NULL, 0, NULL, 0,
				      swhich, SQL_NTS),
		       &msg, "SQLPrimaryKeys")) {
	    goto error;
	}
	break;
    case INFO_INDEXES:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLStatistics(hstmt, NULL, 0, NULL, 0,
				     swhich, SQL_NTS, (SQLUSMALLINT)
				     (RTEST(which2) ?
				      SQL_INDEX_UNIQUE : SQL_INDEX_ALL),
				     SQL_ENSURE),
		       &msg, "SQLStatistics")) {
	    goto error;
	}
	break;
    case INFO_TYPES:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLGetTypeInfo(hstmt, (SQLSMALLINT) itype),
		       &msg, "SQLGetTypeInfo")) {
	    goto error;
	}
	break;
    case INFO_FORKEYS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLForeignKeys(hstmt, NULL, 0, NULL, 0, swhich,
				      SQL_NTS, NULL, 0, NULL, 0,
				      swhich2, SQL_NTS),
		       &msg, "SQLForeignKeys")) {
	    goto error;
	}
	break;
    case INFO_TPRIV:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLTablePrivileges(hstmt, NULL, 0, NULL, 0,
					  (swhich == NULL) ? NULL : swhich,
					  SQL_NTS),
		       &msg, "SQLTablePrivileges")) {
	    goto error;
	}
	break;
    case INFO_PROCS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLProcedures(hstmt, NULL, 0, NULL, 0,
				     swhich, SQL_NTS),
		       &msg, "SQLProcedures")) {
	    goto error;
	}
	break;
    case INFO_PROCCOLS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLProcedureColumns(hstmt, NULL, 0, NULL, 0,
					   swhich, SQL_NTS,
					   swhich2, SQL_NTS),
		       &msg, "SQLProcedureColumns")) {
	    goto error;
	}
	break;
    case INFO_SPECCOLS:
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLSpecialColumns(hstmt, (SQLUSMALLINT) iid,
					 NULL, 0, NULL, 0,
					 swhich, SQL_NTS,
					 (SQLUSMALLINT) iscope,
					 SQL_NULLABLE),
		       &msg, "SQLSpecialColumns")) {
	    goto error;
	}
	break;
    }
#ifdef UNICODE
    uc_free(swhich);
    uc_free(swhich2);
#endif
    return make_result(self, hstmt, Qnil, MAKERES_BLOCK);
error:
#ifdef UNICODE
    uc_free(swhich);
    uc_free(swhich2);
#endif
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
	    SQLFreeStmt(hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
    rb_raise(Cerror, "%s", msg);
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

static VALUE
dbc_speccols(int argc, VALUE *argv, VALUE self)
{
    return dbc_info(argc, argv, self, INFO_SPECCOLS);
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
    char *msg;

    e = get_env(self);
    if (rb_obj_is_kind_of(self, Cenv) != Qtrue) {
	DBC *d;

	d = get_dbc(self);
	dbc = d->hdbc;
    }
    if (!succeeded(e->henv, dbc, SQL_NULL_HSTMT,
		   SQLTransact(e->henv, dbc, (SQLUSMALLINT) what),
		   &msg, "SQLTransact")) {
	rb_raise(Cerror, "%s", msg);
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
	rb_raise(rb_eArgError, "block required");
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

#if (ODBCVER >= 0x0300)
static VALUE
do_attr(int argc, VALUE *argv, VALUE self, int op)
{
    SQLHENV henv = NULL;
    VALUE val;
    SQLINTEGER v, l;
    char *msg;

    if (self != Modbc) {
	henv = get_env(self)->henv;
    }
    rb_scan_args(argc, argv, "01", &val);
    if (val == Qnil) {
	if (!succeeded(henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		       SQLGetEnvAttr(henv, (SQLINTEGER) op,
				     (SQLPOINTER) &v, sizeof (v), &l),
		       &msg, "SQLGetEnvAttr(%d)", op)) {
	    rb_raise(Cerror, "%s", msg);
	}
	return rb_int2inum(v);
    }
    v = NUM2INT(val);
    if (!succeeded(henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
		   SQLSetEnvAttr(henv, (SQLINTEGER) op, (SQLPOINTER) v,
				 SQL_IS_INTEGER),
		   &msg, "SQLSetEnvAttr(%d)", op)) {
	rb_raise(Cerror, "%s", msg);
    }
    return Qnil;
}
#endif

static VALUE
env_cpooling(int argc, VALUE *argv, VALUE self)
{
#if (ODBCVER >= 0x0300)
    return do_attr(argc, argv, self, SQL_ATTR_CONNECTION_POOLING);
#else
    rb_raise(Cerror, set_err("Unsupported in ODBC < 3.0"));
    return Qnil;
#endif
}

static VALUE
env_cpmatch(int argc, VALUE *argv, VALUE self)
{
#if (ODBCVER >= 0x0300)
    return do_attr(argc, argv, self, SQL_ATTR_CP_MATCH);
#else
    rb_raise(Cerror, set_err("Unsupported in ODBC < 3.0"));
    return Qnil;
#endif
}

static VALUE
env_odbcver(int argc, VALUE *argv, VALUE self)
{
#if (ODBCVER >= 0x0300)
    return do_attr(argc, argv, self, SQL_ATTR_ODBC_VERSION);
#else
    VALUE val;

    rb_scan_args(argc, argv, "01", &val);
    if (val == Qnil) {
	return rb_int2inum(ODBCVER >> 8);
    }
    rb_raise(Cerror, set_err("Unsupported in ODBC < 3.0"));
#endif
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
    char *msg;

    rb_scan_args(argc, argv, "01", &val);
    p = get_dbc(self);
    if (p->hdbc == SQL_NULL_HDBC) {
	rb_raise(Cerror, set_err("No connection"));
    }
    if (val == Qnil) {
	if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		       SQLGetConnectOption(p->hdbc, (SQLUSMALLINT) op,
					   (SQLPOINTER) &v),
		       &msg, "SQLGetConnectOption(%d)", op)) {
	    rb_raise(Cerror, "%s", msg);
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
	if (op == SQL_ROWSET_SIZE) {
	    rb_raise(Cerror, set_err("Read only attribute"));
	}
	break;

    default:
	return Qnil;
    }
    if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		   SQLSetConnectOption(p->hdbc, (SQLUSMALLINT) op,
				       (SQLUINTEGER) v),
		   &msg, "SQLSetConnectOption(%d)", op)) {
	rb_raise(Cerror, "%s", msg);
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
 *      Scan literal date/time/timestamp to TIMESTAMP_STRUCT.
 *
 *----------------------------------------------------------------------
 */

static int
scan_dtts(VALUE str, int do_d, int do_t, TIMESTAMP_STRUCT *ts)
{
    int yy = 0, mm = 0, dd = 0, hh = 0, mmm = 0, ss = 0, ff = 0, i;
    char c, *cstr = STR2CSTR(str);

    memset(ts, 0, sizeof (TIMESTAMP_STRUCT));
    if ((sscanf(cstr, "{ts '%d-%d-%d %d:%d:%d.%d' %c",
		&yy, &mm, &dd, &hh, &mmm, &ss, &ff, &c) == 8 ||
	 sscanf(cstr, "{ts '%d-%d-%d %d:%d:%d' %c",
		&yy, &mm, &dd, &hh, &mmm, &ss, &c) == 7) &&
	 c == '}') {
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	ts->hour = hh;
	ts->minute = mmm;
	ts->second = ss;
	ts->fraction = ff;
	return 1;
    }
    if (do_d && sscanf(cstr, "{d '%d-%d-%d' %c",
		       &yy, &mm, &dd, &c) == 4 && c == '}') {
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	return 1;
    }
    if (do_t && sscanf(cstr, "{t '%d:%d:%d' %c",
		       &hh, &mmm, &ss, &c) == 4 && c == '}') {
	ts->hour = yy;
	ts->minute = mmm;
	ts->second = ss;
	return 1;
    }
    ff = ss = 0;
    i = sscanf(cstr, "%d-%d-%d %d:%d:%d%c%d",
	       &yy, &mm, &dd, &hh, &mmm, &ss, &c, &ff);
    if (i >= 5) {
	if (i > 6 && !strchr(". \t", c)) {
	    goto next;
	}
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	ts->hour = hh;
	ts->minute = mmm;
	ts->second = ss;
	ts->fraction = ff;
	return 1;
    }
next:
    ff = ss = 0;
    if (do_d && sscanf(cstr, "%d-%d-%d", &yy, &mm, &dd) == 3) {
	ts->year = yy;
	ts->month = mm;
	ts->day = dd;
	return 1;
    }
    if (do_t && sscanf(cstr, "%d:%d:%d", &hh, &mmm, &ss) == 3) {
	ts->hour = hh;
	ts->minute = mmm;
	ts->second = ss;
	return 1;
    }
    return 0;
}

/*
 *----------------------------------------------------------------------
 *
 *      Date methods.
 *
 *----------------------------------------------------------------------
 */

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
date_alloc(VALUE self)
{
    DATE_STRUCT *date;
    VALUE obj = Data_Make_Struct(self, DATE_STRUCT, 0, xfree, date);

    memset(date, 0, sizeof (*date));
    return obj;
}
#else
static VALUE
date_new(int argc, VALUE *argv, VALUE self)
{
    DATE_STRUCT *date;
    VALUE obj = Data_Make_Struct(self, DATE_STRUCT, 0, xfree, date);
    
    rb_obj_call_init(obj, argc, argv);
    return obj;
}
#endif

static VALUE
date_load1(VALUE self, VALUE str, int load)
{
    TIMESTAMP_STRUCT tss;

    if (scan_dtts(str, 1, 0, &tss)) {
	DATE_STRUCT *date;
	VALUE obj;

	if (load) {
	    obj = Data_Make_Struct(self, DATE_STRUCT, 0, xfree, date);
	} else {
	    obj = self;
	    Data_Get_Struct(self, DATE_STRUCT, date);
	}
	date->year = tss.year;
	date->month = tss.month;
	date->day = tss.day;
	return obj;
    }
    if (load > 0) {
	rb_raise(rb_eTypeError, "marshaled ODBC::Date format error");
    }
    return Qnil;
}

static VALUE
date_load(VALUE self, VALUE str)
{
    return date_load1(self, str, 1);
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
    } else if (argc == 1 && rb_obj_is_kind_of(y, rb_cString) == Qtrue) {
	if (date_load1(self, y, 0) != Qnil) {
	    return self;
	}
    }
    Data_Get_Struct(self, DATE_STRUCT, date);
    date->year  = (y == Qnil) ? 0 : NUM2INT(y);
    date->month = (m == Qnil) ? 0 : NUM2INT(m);
    date->day   = (d == Qnil) ? 0 : NUM2INT(d);
    return self;
}

static VALUE
date_clone(VALUE self)
{
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    VALUE obj = rb_obj_alloc(CLASS_OF(self));
    DATE_STRUCT *date1, *date2;

    Data_Get_Struct(self, DATE_STRUCT, date1);
    Data_Get_Struct(obj, DATE_STRUCT, date2);
    *date2 = *date1;
    return obj;
#else
    return date_new(1, &self, CLASS_OF(self));
#endif
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

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
time_alloc(VALUE self)
{
    TIME_STRUCT *time;
    VALUE obj = Data_Make_Struct(self, TIME_STRUCT, 0, xfree, time);

    memset(time, 0, sizeof (*time));
    return obj;
}
#else
static VALUE
time_new(int argc, VALUE *argv, VALUE self)
{
    TIME_STRUCT *time;
    VALUE obj = Data_Make_Struct(self, TIME_STRUCT, 0, xfree, time);
    
    rb_obj_call_init(obj, argc, argv);
    return obj;
}
#endif

static VALUE
time_load1(VALUE self, VALUE str, int load)
{
    TIMESTAMP_STRUCT tss;

    if (scan_dtts(str, 0, 1, &tss)) {
	TIME_STRUCT *time;
	VALUE obj;
       
	if (load) {
	    obj = Data_Make_Struct(self, TIME_STRUCT, 0, xfree, time);
	} else {
	    obj = self;
	    Data_Get_Struct(self, TIME_STRUCT, time);
	}
	time->hour = tss.hour;
	time->minute = tss.minute;
	time->second = tss.second;
	return obj;
    }
    if (load > 0) {
	rb_raise(rb_eTypeError, "marshaled ODBC::Time format error");
    }
    return Qnil;
}

static VALUE
time_load(VALUE self, VALUE str)
{
    return time_load1(self, str, 1);
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
    } else if (argc == 1 && rb_obj_is_kind_of(h, rb_cString) == Qtrue) {
	if (time_load1(self, h, 0) != Qnil) {
	    return self;
	}
    }
    Data_Get_Struct(self, TIME_STRUCT, time);
    time->hour   = (h == Qnil) ? 0 : NUM2INT(h);
    time->minute = (m == Qnil) ? 0 : NUM2INT(m);
    time->second = (s == Qnil) ? 0 : NUM2INT(s);
    return self;
}

static VALUE
time_clone(VALUE self)
{
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    VALUE obj = rb_obj_alloc(CLASS_OF(self));
    TIME_STRUCT *time1, *time2;

    Data_Get_Struct(self, TIME_STRUCT, time1);
    Data_Get_Struct(obj, TIME_STRUCT, time2);
    *time2 = *time1;
    return obj;
#else
    return time_new(1, &self, CLASS_OF(self));
#endif
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
time_min(int argc, VALUE *argv, VALUE self)
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
time_sec(int argc, VALUE *argv, VALUE self)
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

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
static VALUE
timestamp_alloc(VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, xfree, ts);

    memset(ts, 0, sizeof (*ts));
    return obj;
}
#else
static VALUE
timestamp_new(int argc, VALUE *argv, VALUE self)
{
    TIMESTAMP_STRUCT *ts;
    VALUE obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, xfree, ts);

    rb_obj_call_init(obj, argc, argv);
    return obj;
}
#endif

static VALUE
timestamp_load1(VALUE self, VALUE str, int load)
{
    TIMESTAMP_STRUCT tss;

    if (scan_dtts(str, !load, !load, &tss)) {
	TIMESTAMP_STRUCT *ts;
	VALUE obj;

	if (load) {
	    obj = Data_Make_Struct(self, TIMESTAMP_STRUCT, 0, xfree, ts);
	} else {
	    obj = self;
	    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
	}
	*ts = tss;
	return obj;
    }
    if (load > 0) {
	rb_raise(rb_eTypeError, "marshaled ODBC::TimeStamp format error");
    }
    return Qnil;
}

static VALUE
timestamp_load(VALUE self, VALUE str)
{
    return timestamp_load1(self, str, 1);
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
    } else if (argc == 1 && rb_obj_is_kind_of(y, rb_cString) == Qtrue) {
	if (timestamp_load1(self, y, 0) != Qnil) {
	    return self;
	}
    }
    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts);
    ts->year     = (y  == Qnil) ? 0 : NUM2INT(y);
    ts->month    = (m  == Qnil) ? 0 : NUM2INT(m);
    ts->day      = (d  == Qnil) ? 0 : NUM2INT(d);
    ts->hour     = (hh == Qnil) ? 0 : NUM2INT(hh);
    ts->minute   = (mm == Qnil) ? 0 : NUM2INT(mm);
    ts->second   = (ss == Qnil) ? 0 : NUM2INT(ss);
    ts->fraction = (f  == Qnil) ? 0 : NUM2INT(f);
    return self;
}

static VALUE
timestamp_clone(VALUE self)
{
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    VALUE obj = rb_obj_alloc(CLASS_OF(self));
    TIMESTAMP_STRUCT *ts1, *ts2;

    Data_Get_Struct(self, TIMESTAMP_STRUCT, ts1);
    Data_Get_Struct(obj, TIMESTAMP_STRUCT, ts2);
    *ts2 = *ts1;
    return obj;
#else
    return timestamp_new(1, &self, CLASS_OF(self));
#endif
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
timestamp_min(int argc, VALUE *argv, VALUE self)
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
timestamp_sec(int argc, VALUE *argv, VALUE self)
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
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	q->hstmt = SQL_NULL_HSTMT;
	unlink_stmt(q);
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
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_CLOSE), "SQLFreeStmt(SQL_CLOSE)");
    }
    free_stmt_sub(q);
    return self;
}

static VALUE
stmt_cancel(VALUE self)
{
    STMT *q;
    char *msg;

    Data_Get_Struct(self, STMT, q);
    if (q->hstmt != SQL_NULL_HSTMT) {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		       SQLCancel(q->hstmt), &msg, "SQLCancel")) {
	    rb_raise(Cerror, "%s", msg);
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
    char *msg;

    Data_Get_Struct(self, STMT, q);
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLRowCount(q->hstmt, &rows), &msg, "SQLRowCount")) {
	rb_raise(Cerror, "%s", msg);
    }
    return INT2NUM(rows);
}

static VALUE
stmt_nparams(VALUE self)
{
    STMT *q;

    Data_Get_Struct(self, STMT, q);
    return INT2FIX(q->nump);
}

static VALUE
stmt_cursorname(int argc, VALUE *argv, VALUE self)
{
    VALUE cn = Qnil;
    STMT *q;
#ifdef UNICODE
    SQLWCHAR cname[SQL_MAX_MESSAGE_LENGTH];
    SQLWCHAR *cp;
#else
    char cname[SQL_MAX_MESSAGE_LENGTH];
    char *cp;
#endif
    char *msg;
    SQLSMALLINT cnLen = 0;

    rb_scan_args(argc, argv, "01", &cn);
    Data_Get_Struct(self, STMT, q);
    if (cn == Qnil) {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		       SQLGetCursorName(q->hstmt, (SQLTCHAR *) cname,
					(SQLSMALLINT) sizeof (cname), &cnLen),
		       &msg, "SQLGetCursorName")) {
	    rb_raise(Cerror, "%s", &msg);
	}
#ifdef UNICODE
	cnLen = (cnLen == 0) ? uc_strlen(cname) : (cnLen / sizeof (SQLWCHAR));
	return uc_tainted_str_new(cname, cnLen);
#else
	cnLen = (cnLen == 0) ? strlen(cname) : cnLen;
	return rb_tainted_str_new(cname, cnLen);
#endif
    }
    if (TYPE(cn) != T_STRING) {
	cn = rb_any_to_s(cn);
    }
#ifdef UNICODE
    cp = uc_from_utf(STR2CSTR(cn));
    if (!cp) {
	rb_raise(Cerror, set_err("Out of memory"));
    }
#else
    cp = STR2CSTR(cn);
#endif
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLSetCursorName(q->hstmt, cp, SQL_NTS),
		   &msg, "SQLSetCursorName")) {
#ifdef UNICODE
	uc_free(cp);
#endif
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    uc_free(cp);
#endif
    return cn;
}

static VALUE
stmt_column(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    VALUE col;

    rb_scan_args(argc, argv, "1", &col);
    Check_Type(col, T_FIXNUM);
    Data_Get_Struct(self, STMT, q);
    return make_col(q->hstmt, FIX2INT(col), q->upc);
}

static VALUE
stmt_columns(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    int i;
    VALUE res, as_ary = Qfalse;

    rb_scan_args(argc, argv, "01", &as_ary);
    Data_Get_Struct(self, STMT, q);
    if (rb_block_given_p()) {
	for (i = 0; i < q->ncols; i++) {
	    rb_yield(make_col(q->hstmt, i, q->upc));
	}
	return self;
    }
    if (RTEST(as_ary)) {
	res = rb_ary_new2(q->ncols);
    } else {
	res = rb_hash_new();
    }
    for (i = 0; i < q->ncols; i++) {
	VALUE obj;

	obj = make_col(q->hstmt, i, q->upc);
	if (RTEST(as_ary)) {
	    rb_ary_store(res, i, obj);
	} else {
	    VALUE name = rb_iv_get(obj, "@name");

	    if (rb_funcall(res, rb_intern("key?"), 1, name) == Qtrue) {
		char buf[32];

		sprintf(buf, "#%d", i);
		name = rb_str_dup(name);
		name = rb_obj_taint(rb_str_cat2(name, buf));
	    }
	    rb_hash_aset(res, name, obj);
	}
    }
    return res;
}

static VALUE
stmt_param(int argc, VALUE *argv, VALUE self)
{
    STMT *q;
    VALUE par;
    int i;

    rb_scan_args(argc, argv, "1", &par);
    Check_Type(par, T_FIXNUM);
    Data_Get_Struct(self, STMT, q);
    i = FIX2INT(par);
    if (i < 0 || i >= q->nump) {
	rb_raise(Cerror, set_err("Parameter out of bounds"));
    }
    return make_par(q, i);
}

static VALUE
stmt_params(VALUE self)
{
    STMT *q;
    int i;
    VALUE res;

    Data_Get_Struct(self, STMT, q);
    if (rb_block_given_p()) {
	for (i = 0; i < q->nump; i++) {
	    rb_yield(make_par(q, i));
	}
	return self;
    }
    res = rb_ary_new2(q->nump);
    for (i = 0; i < q->nump; i++) {
	VALUE obj;

	obj = make_par(q, i);
	rb_ary_store(res, i, obj);
    }
    return res;
}

static VALUE
do_fetch(STMT *q, int mode)
{
    int i, offc;
    char **bufs, *msg;
    VALUE res;

    if (q->ncols <= 0) {
	rb_raise(Cerror, set_err("No columns in result set"));
    }
    if (++q->fetchc >= 500) {
	q->fetchc = 0;
	start_gc();
    }
    bufs = q->dbufs;
    if (bufs == NULL) {
	int need = sizeof (char *) * q->ncols;
	char *p;

	for (i = 0; i < q->ncols; i++) {
	    if (q->coltypes[i].size != SQL_NO_TOTAL) {
		need += LEN_ALIGN(q->coltypes[i].size);
	    }
	}
	p = ALLOC_N(char, need);
	if (p == NULL) {
	    rb_raise(Cerror, set_err("Out of memory"));
	}
	q->dbufs = bufs = (char **) p;
	p += sizeof (char *) * q->ncols;
	for (i = 0; i < q->ncols; i++) {
	    int len = q->coltypes[i].size;

	    if (len == SQL_NO_TOTAL) {
		bufs[i] = NULL;
	    } else {
		bufs[i] = p;
		p += LEN_ALIGN(len);
	    }
	}
    }
    switch (mode & DOFETCH_MODES) {
    case DOFETCH_HASH:
    case DOFETCH_HASH2:
	if (q->colnames == NULL) {
	    int need = sizeof (char *) * 4 * q->ncols;
	    char **na, *p;
#ifdef UNICODE
	    SQLWCHAR name[SQL_MAX_MESSAGE_LENGTH];
#else
	    char name[SQL_MAX_MESSAGE_LENGTH];
#endif
	    SQLSMALLINT name_len;

	    for (i = 0; i < q->ncols; i++) {
		name[0] = 0;
		if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			       SQLColAttributes(q->hstmt,
						(SQLUSMALLINT) (i + 1),
						SQL_COLUMN_TABLE_NAME,
						name,
						sizeof (name),
						&name_len, NULL),
			       &msg,
			       "SQLColAttributes(SQL_COLUMN_TABLE_NAME)")) {
		    rb_raise(Cerror, "%s", msg);
		}
		if (name_len >= sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
#ifdef UNICODE
		need += 6 * (uc_strlen(name) + 1);
#else
		need += 2 * (strlen(name) + 1);
#endif
		name[0] = 0;
		if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			       SQLColAttributes(q->hstmt,
						(SQLUSMALLINT) (i + 1),
						SQL_COLUMN_LABEL, name,
						sizeof (name),
						&name_len, NULL),
			       &msg,
			       "SQLColAttributes(SQL_COLUMN_LABEL)")) {
		    rb_raise(Cerror, "%s", msg);
		}
		if (name_len >= sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
#ifdef UNICODE
		need += 12 * (uc_strlen(name) + 1);
#else
		need += strlen(upcase_if(name, 1)) + 1 +
			strlen(name) + 1;
#endif
	    }
	    p = ALLOC_N(char, need);
	    if (p == NULL) {
		rb_raise(Cerror, set_err("Out of memory"));
	    }
	    na = (char **) p;
	    p += sizeof (char *) * 4 * q->ncols;
	    for (i = 0; i < q->ncols; i++) {
		char *p0;

		name[0] = 0;
		callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
					 SQL_COLUMN_TABLE_NAME, name,
					 sizeof (name), &name_len, NULL),
			"SQLColAttributes(SQL_COLUMN_TABLE_NAME)");
		if (name_len >= sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
		na[i + q->ncols] = p;
#ifdef UNICODE
		p += mkutf(p, name, uc_strlen(name));
#else
		strcpy(p, name);
#endif
		strcat(p, ".");
		p += strlen(p);
		p0 = p;
		name[0] = 0;
		callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			SQLColAttributes(q->hstmt, (SQLUSMALLINT) (i + 1),
					 SQL_COLUMN_LABEL, name,
					 sizeof (name), &name_len, NULL),
			"SQLColAttributes(SQL_COLUMN_LABEL)");
		if (name_len >= sizeof (name)) {
		    name_len = sizeof (name) - 1;
		}
		if (name_len > 0) {
		    name[name_len / sizeof (name[0])] = 0;
		}
		na[i] = p;
#ifdef UNICODE
		p += mkutf(p, name, uc_strlen(name)) + 1;
#else
		strcpy(p, name);
		p += strlen(p) + 1;
#endif
		na[i + 3 * q->ncols] = p;
		strcpy(p, na[i + q->ncols]);
		p += p0 - na[i + q->ncols];
		strcpy(p, upcase_if(na[i], 1));
		na[i + 2 * q->ncols] = p;
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
    offc = q->upc ? (2 * q->ncols) : 0;
    for (i = 0; i < q->ncols; i++) {
	SQLINTEGER curlen;
	SQLSMALLINT type = q->coltypes[i].type;
	VALUE v, name;
	char *valp, *freep = NULL;
	SQLINTEGER totlen;

	curlen = q->coltypes[i].size;
	if (curlen == SQL_NO_TOTAL) {
	    totlen = 0;
	    valp = ALLOC_N(char, SEGSIZE + 1);
	    freep = valp;
	    while (curlen == SQL_NO_TOTAL || curlen > SEGSIZE) {
		int ret;

		ret = succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
				SQLGetData(q->hstmt, (SQLUSMALLINT) (i + 1),
					   type, (SQLPOINTER) (valp + totlen),
#ifdef UNICODE
					   (type == SQL_C_CHAR ||
					    type == SQL_C_WCHAR) ?
					     SEGSIZE + sizeof (SQLWCHAR) :
					     SEGSIZE,
#else
					   (type == SQL_C_CHAR) ?
					     SEGSIZE + 1 : SEGSIZE,
#endif
					   &curlen), &msg, "SQLGetData");
		if (!ret) {
		    xfree(valp);
		    rb_raise(Cerror, "%s", msg);
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
		    rb_raise(Cerror, set_err("Out of memory"));
		}
		freep = valp;
	    }
	    if (totlen > 0) {
		curlen = totlen;
	    }
	} else {
	    totlen = curlen;
	    valp = bufs[i];
	    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			   SQLGetData(q->hstmt, (SQLUSMALLINT) (i + 1), type,
				      (SQLPOINTER) valp, totlen, &curlen),
			   &msg, "SQLGetData")) {
		rb_raise(Cerror, "%s", msg);
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

		    v = Data_Make_Struct(Cdate, DATE_STRUCT, 0, xfree, date);
		    *date = *(DATE_STRUCT *) valp;
		}
		break;
	    case SQL_C_TIME:
		{
		    TIME_STRUCT *time;

		    v = Data_Make_Struct(Ctime, TIME_STRUCT, 0, xfree, time);
		    *time = *(TIME_STRUCT *) valp;
		}
		break;
	    case SQL_C_TIMESTAMP:
		{
		    TIMESTAMP_STRUCT *ts;

		    v = Data_Make_Struct(Ctimestamp, TIMESTAMP_STRUCT,
					 0, xfree, ts);
		    *ts = *(TIMESTAMP_STRUCT *) valp;
		}
		break;
#ifdef UNICODE
	    case SQL_C_WCHAR:
		v = uc_tainted_str_new((SQLWCHAR *) valp,
				       curlen / sizeof (SQLWCHAR));
		break;
#endif
	    default:
		v = rb_tainted_str_new(valp, curlen);
		break;
	    }
	}
	if (freep) {
	    xfree(freep);
	}
	switch (mode & DOFETCH_MODES) {
	case DOFETCH_HASH:
	    name = rb_tainted_str_new2(q->colnames[i + offc]);
	    goto doaset;
	case DOFETCH_HASH2:
	    name = rb_tainted_str_new2(q->colnames[i + offc + q->ncols]);
	doaset:
	    if (rb_funcall(res, rb_intern("key?"), 1, name) == Qtrue) {
		char buf[32];

		sprintf(buf, "#%d", i);
		name = rb_str_cat2(name, buf);
	    }
	    rb_hash_aset(res, name, v);
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
    SQLRETURN ret;
    char *msg, *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    if (q->usef) {
	goto usef;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_NEXT)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_NEXT, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_NEXT)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_NEXT, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    if (err != NULL && strncmp(err, "IM001", 5) == 0) {
usef:
	/* Fallback to SQLFetch() when others not implemented */
	msg = "SQLFetch";
	q->usef = 1;
	ret = SQLFetch(q->hstmt);
	if (ret == SQL_NO_DATA) {
	    (void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	    return Qnil;
	}
	if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret,
		      &err, msg)) {
	    return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
	}
    }
    rb_raise(Cerror, "%s", err);
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
    SQLRETURN ret;
    char *msg, *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_FIRST)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_FIRST)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, "%s", err);
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
    SQLRETURN ret;
    int idir, ioffs = 1;
    char msg[128], *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    rb_scan_args(argc, argv, "11", &dir, &offs);
    idir = NUM2INT(dir);
    if (offs != Qnil) {
	ioffs = NUM2INT(offs);
    }
    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
#if (ODBCVER < 0x0300)
    sprintf(msg, "SQLExtendedFetch(%d)", idir);
    ret = SQLExtendedFetch(q->hstmt, (SQLSMALLINT) idir, (SQLINTEGER) ioffs,
			   &nRows, rowStat);
#else
    sprintf(msg, "SQLFetchScroll(%d)", idir);
    ret = SQLFetchScroll(q->hstmt, (SQLSMALLINT) idir, (SQLINTEGER) ioffs);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, DOFETCH_ARY | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, "%s", err);
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
    int i, max = 0, all = arg == Qnil;
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
    return (i == 0) ? Qnil : res;
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
    SQLRETURN ret;
    int mode = stmt_hash_mode(argc, argv, self);
    char *msg, *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
    if (q->usef) {
	goto usef;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_NEXT)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_NEXT, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_NEXT)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_NEXT, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
    }
    if (err != NULL && strncmp(err, "IM001", 5) == 0) {
usef:
	/* Fallback to SQLFetch() when others not implemented */
	msg = "SQLFetch";
	q->usef = 1;
	ret = SQLFetch(q->hstmt);
	if (ret == SQL_NO_DATA) {
	    (void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	    return Qnil;
	}
	if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret,
		      &err, msg)) {
	    return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
	}
    }
    rb_raise(Cerror, "%s", err);
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
    SQLRETURN ret;
    int mode = stmt_hash_mode(argc, argv, self);
    char *msg, *err;
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
    if (q->ncols <= 0) {
	return Qnil;
    }
#if (ODBCVER < 0x0300)
    msg = "SQLExtendedFetch(SQL_FETCH_FIRST)";
    ret = SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows, rowStat);
#else
    msg = "SQLFetchScroll(SQL_FETCH_FIRST)";
    ret = SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0);
#endif
    if (ret == SQL_NO_DATA) {
	(void) tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, msg);
	return Qnil;
    }
    if (succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt, ret, &err, msg)) {
	return do_fetch(q, mode | (bang ? DOFETCH_BANG : 0));
    }
    rb_raise(Cerror, "%s", err);
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
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    Data_Get_Struct(self, STMT, q);
#if (ODBCVER < 0x0300)
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows,
				     rowStat),
		    "SQLExtendedFetch(SQL_FETCH_FIRST)"))
#else
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0),
		    "SQLFetchScroll(SQL_FETCH_FIRST)"))
#endif
    {
    case SQL_NO_DATA:
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
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
#if (ODBCVER < 0x0300)
    SQLUINTEGER nRows;
    SQLUSMALLINT rowStat[1];
#endif

    withtab = (mode == DOFETCH_HASH2) ? Qtrue : Qfalse;
    Data_Get_Struct(self, STMT, q);
#if (ODBCVER < 0x0300)
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLExtendedFetch(q->hstmt, SQL_FETCH_FIRST, 0, &nRows,
				     rowStat),
		    "SQLExtendedFetch(SQL_FETCH_FIRST)"))
#else
    switch (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLFetchScroll(q->hstmt, SQL_FETCH_FIRST, 0),
		    "SQLFetchScroll(SQL_FETCH_FIRST)"))
#endif
    {
    case SQL_NO_DATA:
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
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
stmt_more_results(VALUE self)
{
    STMT *q;

    if (rb_block_given_p()) {
	rb_raise(rb_eArgError, "block not allowed");
    }
    Data_Get_Struct(self, STMT, q);
    if (q->hstmt == SQL_NULL_HSTMT) {
	return Qfalse;
    }
    switch (tracesql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		     SQLMoreResults(q->hstmt), "SQLMoreResults")) {
    case SQL_NO_DATA:
	return Qfalse;
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
	make_result(q->dbc, q->hstmt, self, 0);
	break;
    default:
	rb_raise(Cerror, "%s",
		 get_err(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt));
    }
    return Qtrue;
}

static VALUE
stmt_prep_int(int argc, VALUE *argv, VALUE self, int mode)
{
    DBC *p = get_dbc(self);
    STMT *q = NULL;
    VALUE sql, dbc, stmt;
    SQLHSTMT hstmt;
#ifdef UNICODE
    SQLWCHAR *ssql = NULL;
#else
    char *ssql = NULL;
#endif
    char *csql = NULL, *msg = NULL;

    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	Data_Get_Struct(self, STMT, q);
	if (q->hstmt == SQL_NULL_HSTMT) {
	    if (!succeeded(SQL_NULL_HENV, p->hdbc, q->hstmt,
			   SQLAllocStmt(p->hdbc, &q->hstmt),
			   &msg, "SQLAllocStmt")) {
		rb_raise(Cerror, "%s", msg);
	    }
	} else if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
			      SQLFreeStmt(q->hstmt, SQL_CLOSE),
			      &msg, "SQLFreeStmt(SQL_CLOSE)")) {
	    rb_raise(Cerror, "%s", msg);
	}
	hstmt = q->hstmt;
	stmt = self;
	dbc = q->dbc;
    } else {
	if (!succeeded(SQL_NULL_HENV, p->hdbc, SQL_NULL_HSTMT,
		       SQLAllocStmt(p->hdbc, &hstmt),
		       &msg, "SQLAllocStmt")) {
	    rb_raise(Cerror, "%s", msg);
	}
	stmt = Qnil;
	dbc = self;
    }
    rb_scan_args(argc, argv, "1", &sql);
    Check_Type(sql, T_STRING);
#ifdef UNICODE
    csql = STR2CSTR(sql);
    ssql = uc_from_utf(csql);
    if (!ssql) {
	rb_raise(Cerror, set_err("Out of memory"));
    }
#else
    csql = ssql = STR2CSTR(sql);
#endif
    if ((mode & MAKERES_EXECD)) {
	if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		       SQLExecDirect(hstmt, ssql, SQL_NTS),
		       &msg, "SQLExecDirect('%s')", csql)) {
	    goto sqlerr;
	}
    } else if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
			  SQLPrepare(hstmt, ssql, SQL_NTS),
			  &msg, "SQLPrepare('%s')", csql)) {
sqlerr:
#ifdef UNICODE
	uc_free(ssql);
#endif
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
		SQLFreeStmt(hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	if (q) {
	    q->hstmt = SQL_NULL_HSTMT;
	    unlink_stmt(q);
	}
	rb_raise(Cerror, "%s", msg);
    } else {
	mode |= MAKERES_PREPARE;
    }
#ifdef UNICODE
    uc_free(ssql);
#endif
    return make_result(dbc, hstmt, stmt, mode);
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
#ifdef UNICODE
    SQLWCHAR *up;
#endif

    Data_Get_Struct(self, STMT, q);
    if (argc > q->nump) {
	rb_raise(Cerror, set_err("Too much parameters"));
    }
    if (q->hstmt == SQL_NULL_HSTMT) {
	rb_raise(Cerror, set_err("Stale ODBC::Statement"));
    }
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLFreeStmt(q->hstmt, SQL_CLOSE),
		   &msg, "SQLFreeStmt(SQL_CLOSE)")) {
	goto error;
    }
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
	    SQLFreeStmt(q->hstmt, SQL_RESET_PARAMS),
	    "SQLFreeStmt(SQL_RESET_PARMS)");
    for (i = 0; i < argc; i++) {
	SQLPOINTER valp = (SQLPOINTER) &q->pinfo[i].buffer;
	SQLSMALLINT ctype, stype;
	SQLINTEGER vlen, rlen;
	SQLUINTEGER coldef;

	q->pinfo[i].tofree = 0;
	switch (TYPE(argv[i])) {
	case T_STRING:
#ifdef UNICODE
	    ctype = SQL_C_WCHAR;
	    up = uc_from_utf(STR2CSTR(argv[i]));
	    if (!up) {
		for (--i; i >= 0; i--) {
		    if (q->pinfo[i].tofree) {
			uc_free(*(SQLWCHAR **) &q->pinfo[i].buffer);
		    }
		}
		rb_raise(Cerror, set_err("Out of memory"));
	    }
	    *(SQLWCHAR **) valp = up;
	    rlen = uc_strlen(up) * sizeof (SQLWCHAR);
	    vlen = rlen + sizeof (SQLWCHAR);
	    q->pinfo[i].tofree = 1;
#else
	    ctype = SQL_C_CHAR;
	    valp = (SQLPOINTER) STR2CSTR(argv[i]);
	    rlen = strlen((char *) valp);
	    vlen = rlen + 1;
#endif
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
#ifdef UNICODE
	if (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLBindParameter(q->hstmt, (SQLUSMALLINT) (i + 1),
				     SQL_PARAM_INPUT,
				     ctype, stype, coldef, q->pinfo[i].scale,
				     (ctype == SQL_C_WCHAR) ?
				     *(SQLWCHAR **) valp : valp,
				     vlen, &q->pinfo[i].rlen),
		    "SQLBindParameter(SQL_PARAM_INPUT)")
	    == SQL_ERROR)
#else
	if (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLBindParameter(q->hstmt, (SQLUSMALLINT) (i + 1),
				     SQL_PARAM_INPUT,
				     ctype, stype, coldef, q->pinfo[i].scale,
				     valp, vlen, &q->pinfo[i].rlen),
		    "SQLBindParameter(SQL_PARAM_INPUT)")
	    == SQL_ERROR)
#endif
	{
	    goto error;
	}
    }
    for (; i < q->nump; i++) {
	q->pinfo[i].tofree = 0;
	q->pinfo[i].rlen = SQL_NULL_DATA;
	if (callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		    SQLBindParameter(q->hstmt, (SQLUSMALLINT) (i + 1),
				     SQL_PARAM_INPUT, SQL_C_CHAR,
				     q->pinfo[i].type,
				     q->pinfo[i].coldef, q->pinfo[i].scale,
				     NULL, 0, &q->pinfo[i].rlen),
		    "SQLBindParameter(SQL_PARAM_INPUT)")
	    == SQL_ERROR) {
	    goto error;
	}
    }
    if (!succeeded(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		   SQLExecute(q->hstmt), &msg, "SQLExecute")) {
error:
#ifdef UNICODE
	for (i = 0; i < q->nump; i++) {
	    if (q->pinfo[i].tofree) {
		uc_free(*(SQLWCHAR **) &q->pinfo[i].buffer);
	    }
	}
#endif
	callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
		SQLFreeStmt(q->hstmt, SQL_DROP), "SQLFreeStmt(SQL_DROP)");
	q->hstmt = SQL_NULL_HSTMT;
	unlink_stmt(q);
	rb_raise(Cerror, "%s", msg);
    }
#ifdef UNICODE
    for (i = 0; i < q->nump; i++) {
	if (q->pinfo[i].tofree) {
	    uc_free(*(SQLWCHAR **) &q->pinfo[i].buffer);
	}
    }
#endif
    callsql(SQL_NULL_HENV, SQL_NULL_HDBC, q->hstmt,
	    SQLFreeStmt(q->hstmt, SQL_RESET_PARAMS),
	    "SQLFreeStmt(SQL_RESET_PARAMS)");
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
    if (argc == 1) {
	return stmt_prep_int(1, argv, self,
			     MAKERES_EXECD | MAKERES_BLOCK);
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
    if (argc == 1) {
	stmt = stmt_prep_int(1, argv, self,
			     MAKERES_EXECD | MAKERES_BLOCK | MAKERES_NOCLOSE);
    } else {
	stmt = stmt_prep_int(1, argv, self, 0);
	stmt_exec_int(argc - 1, argv + 1, stmt,
		      MAKERES_BLOCK | MAKERES_NOCLOSE);
    }
    return rb_ensure(stmt_nrows, stmt, stmt_drop, stmt);
}

static VALUE
stmt_ignorecase(int argc, VALUE *argv, VALUE self)
{
    VALUE onoff = Qnil;
    int *flag = NULL;

    rb_scan_args(argc, argv, "01", &onoff);
    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	STMT *q;

	Data_Get_Struct(self, STMT, q);
	flag = &q->upc;
    } else if (rb_obj_is_kind_of(self, Cdbc) == Qtrue) {
	DBC *p;

	Data_Get_Struct(self, DBC, p);
	flag = &p->upc;
    } else {
	rb_raise(rb_eTypeError, "ODBC::Statement or ODBC::Database expected");
	return Qnil;
    }
    if (argc > 0) {
	*flag = RTEST(onoff);
    }
    return *flag ? Qtrue : Qfalse;
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
    VALUE stmt = (argc > 0) ? argv[0] : Qnil;

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
    rb_raise(rb_eArgError, "block required");
    return Qnil;
}

static VALUE
stmt_procwrap(int argc, VALUE *argv, VALUE self)
{
    VALUE stmt = Qnil;

    rb_scan_args(argc, argv, "01", &stmt);
    if (rb_obj_is_kind_of(self, Cstmt) == Qtrue) {
	if (stmt != Qnil) {
	    rb_raise(rb_eArgError, "wrong # arguments");
	}
	stmt = self;
    } else if (rb_obj_is_kind_of(stmt, Cstmt) != Qtrue) {
	rb_raise(rb_eTypeError, "need ODBC::Statement as argument");
    }
    return rb_funcall(Cproc, rb_intern("new"), 1, stmt);
}

/*
 *----------------------------------------------------------------------
 *
 *      Module functions.
 *
 *----------------------------------------------------------------------
 */

static VALUE
mod_dbcdisc(VALUE dbc)
{
    return dbc_disconnect(0, NULL, dbc);
}

static VALUE
mod_connect(int argc, VALUE *argv, VALUE self)
{
    VALUE dbc = dbc_new(argc, argv, self);

    if (rb_block_given_p()) {
	return rb_ensure(rb_yield, dbc, mod_dbcdisc, dbc);
    }
    return dbc;
}

static VALUE
mod_2time(int argc, VALUE *argv, VALUE self)
{
    VALUE a1, a2;
    VALUE y, m, d, hh, mm, ss, us;
    int once = 0;

    rb_scan_args(argc, argv, "11", &a1, &a2);
again:
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
    if (!once && (m = timestamp_load1(Ctimestamp, a1, -1)) != Qnil) {
	a1 = m;
	once++;
	goto again;
    }
    if (!once && (m = date_load1(Cdate, a1, -1)) != Qnil) {
	a1 = m;
	if (argc > 1 && (m = time_load1(Ctime, a2, -1)) != Qnil) {
	    a2 = m;
	}
	once++;
	goto again;
    }
    if (!once && (m = time_load1(Ctime, a1, -1)) != Qnil) {
	a1 = m;
	if (argc > 1 && (m = date_load1(Cdate, a2, -1)) != Qnil) {
	    a2 = m;
	}
	once++;
	goto again;
    }
    rb_raise(rb_eTypeError,
	     "expecting ODBC::TimeStamp or ODBC::Date/Time or String");
    return Qnil;
}

static VALUE
mod_2date(VALUE self, VALUE arg)
{
    VALUE y, m, d;
    int once = 0;

again:
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
    if (!once &&
	((m = date_load1(Cdate, arg, -1)) != Qnil ||
	 (m = timestamp_load1(Ctimestamp, arg, -1)) != Qnil)) {
	arg = m;
	once++;
	goto again;
    }
    rb_raise(rb_eTypeError, "expecting ODBC::Date/Timestamp or String");
    return Qnil;
}

static VALUE
mod_trace(int argc, VALUE *argv, VALUE self)
{
    VALUE v = Qnil;

    rb_scan_args(argc, argv, "01", &v);
#ifdef TRACING
    if (argc > 0) {
	tracing = NUM2INT(v);
    }
    return INT2NUM(tracing);
#else
    return INT2NUM(0);
#endif
}

/*
 *----------------------------------------------------------------------
 *
 *      Table of constants.
 *
 *----------------------------------------------------------------------
 */

#define O_CONST(x)    { #x, x }
#define O_CONSTU(x)   { #x, SQL_UNKNOWN_TYPE }
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
#ifdef SQL_WCHAR
    O_CONST(SQL_WCHAR),
#else
    O_CONSTU(SQL_WCHAR),
#endif
#ifdef SQL_WVARCHAR
    O_CONST(SQL_WVARCHAR),
#else
    O_CONSTU(SQL_WVARCHAR),
#endif
#ifdef SQL_WLONGVARCHAR
    O_CONST(SQL_WLONGVARCHAR),
#else
    O_CONSTU(SQL_WLONGVARCHAR),
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
    O_CONST2(SQL_CP_STRICT_MATCH, 0),
    O_CONST2(SQL_CP_RELAXED_MATCH, 0),
    O_CONST2(SQL_CP_MATCH_DEFAULT, 0),
#endif
#ifdef SQL_SCOPE_CURROW
    O_CONST(SQL_SCOPE_CURROW),
#else
    O_CONST2(SQL_SCOPE_CURROW, 0),
#endif
#ifdef SQL_SCOPE_TRANSACTION
    O_CONST(SQL_SCOPE_TRANSACTION),
#else
    O_CONST2(SQL_SCOPE_TRANSACTION, 0),
#endif
#ifdef SQL_SCOPE_SESSION
    O_CONST(SQL_SCOPE_SESSION),
#else
    O_CONST2(SQL_SCOPE_SESSION, 0),
#endif
#ifdef SQL_BEST_ROWID
    O_CONST(SQL_BEST_ROWID),
#else
    O_CONST2(SQL_BEST_ROWID, 0),
#endif
#ifdef SQL_ROWVER
    O_CONST(SQL_ROWVER),
#else
    O_CONST2(SQL_ROWVER, 0),
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
#ifdef UNICODE
Init_odbc_utf8()
#else
Init_odbc()
#endif
{
    int i;
    char *modname = "ODBC";
    ID id = rb_intern(modname);
    VALUE v = Qnil;

    rb_require("date");
    rb_cDate = rb_eval_string("Date");

    if (rb_const_defined(rb_cObject, id)) {
	v = rb_const_get(rb_cObject, id); 
	if (TYPE(v) != T_MODULE) {
	    rb_raise(rb_eTypeError, "%s already defined", modname);
	}
    }
    if (v != Qnil) {
#ifdef UNICODE
	modname = "ODBC_UTF8";
#else
	modname = "ODBC_NONE";
#endif
    }
    Modbc = rb_define_module(modname);
    Cobj = rb_define_class_under(Modbc, "Object", rb_cObject);
    rb_define_class_variable(Cobj, "@@error", Qnil);
    rb_define_class_variable(Cobj, "@@info", Qnil);

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

    Cparam = rb_define_class_under(Modbc, "Parameter", Cobj);
    rb_attr(Cparam, rb_intern("type"), 1, 0, 0);
    rb_attr(Cparam, rb_intern("precision"), 1, 0, 0);
    rb_attr(Cparam, rb_intern("scale"), 1, 0, 0);
    rb_attr(Cparam, rb_intern("nullable"), 1, 0, 0);

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
    rb_define_module_function(Modbc, "trace", mod_trace, -1);
    rb_define_module_function(Modbc, "connect", mod_connect, -1);
    rb_define_module_function(Modbc, "datasources", dbc_dsns, 0);
    rb_define_module_function(Modbc, "drivers", dbc_drivers, 0);
    rb_define_module_function(Modbc, "error", dbc_error, 0);
    rb_define_module_function(Modbc, "info", dbc_warn, 0);
    rb_define_module_function(Modbc, "newenv", env_new, 0);
    rb_define_module_function(Modbc, "to_time", mod_2time, -1);
    rb_define_module_function(Modbc, "to_date", mod_2date, 1);
    rb_define_module_function(Modbc, "connection_pooling", env_cpooling, -1);
    rb_define_module_function(Modbc, "connection_pooling=", env_cpooling, -1);
    rb_define_module_function(Modbc, "raise", dbc_raise, 1);

    /* singleton methods and constructors */
    rb_define_singleton_method(Cobj, "error", dbc_error, 0);
    rb_define_singleton_method(Cobj, "info", dbc_warn, 0);
    rb_define_singleton_method(Cobj, "raise", dbc_raise, 1);
    rb_define_alloc_func(Cenv, env_new);
    rb_define_singleton_method(Cenv, "connect", dbc_new, -1);
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Cdbc, dbc_alloc);
#else
    rb_define_alloc_func(Cdbc, dbc_new);
#endif
    rb_define_alloc_func(Cdsn, dsn_new);
    rb_define_alloc_func(Cdrv, drv_new);
    rb_define_method(Cdsn, "initialize", dsn_init, 0);
    rb_define_method(Cdrv, "initialize", drv_init, 0);

    /* common (Cobj) methods */
    rb_define_method(Cobj, "error", dbc_error, 0);
    rb_define_method(Cobj, "info", dbc_warn, 0);
    rb_define_method(Cobj, "raise", dbc_raise, 1);

    /* common (Cenv) methods */
    rb_define_method(Cenv, "connect", dbc_new, -1);
    rb_define_method(Cenv, "environment", env_of, 0);
    rb_define_method(Cenv, "transaction", dbc_transaction, 0);
    rb_define_method(Cenv, "commit", dbc_commit, 0);
    rb_define_method(Cenv, "rollback", dbc_rollback, 0);
    rb_define_method(Cenv, "connection_pooling", env_cpooling, -1);
    rb_define_method(Cenv, "connection_pooling=", env_cpooling, -1);
    rb_define_method(Cenv, "cp_match", env_cpmatch, -1);
    rb_define_method(Cenv, "cp_match=", env_cpmatch, -1);
    rb_define_method(Cenv, "odbc_version", env_odbcver, -1);
    rb_define_method(Cenv, "odbc_version=", env_odbcver, -1);

    /* management things (odbcinst.h) */
    rb_define_module_function(Modbc, "add_dsn", dbc_adddsn, -1);
    rb_define_module_function(Modbc, "config_dsn", dbc_confdsn, -1);
    rb_define_module_function(Modbc, "del_dsn", dbc_deldsn, -1);

    /* connection (database) methods */
    rb_define_method(Cdbc, "initialize", dbc_connect, -1);
    rb_define_method(Cdbc, "connect", dbc_connect, -1);
    rb_define_method(Cdbc, "connected?", dbc_connected, 0);
    rb_define_method(Cdbc, "drvconnect", dbc_drvconnect, 1);
    rb_define_method(Cdbc, "drop_all", dbc_dropall, 0);
    rb_define_method(Cdbc, "disconnect", dbc_disconnect, -1);
    rb_define_method(Cdbc, "tables", dbc_tables, -1);
    rb_define_method(Cdbc, "columns", dbc_columns, -1);
    rb_define_method(Cdbc, "primary_keys", dbc_primkeys, -1);
    rb_define_method(Cdbc, "indexes", dbc_indexes, -1);
    rb_define_method(Cdbc, "types", dbc_types, -1);
    rb_define_method(Cdbc, "foreign_keys", dbc_forkeys, -1);
    rb_define_method(Cdbc, "table_privileges", dbc_tpriv, -1);
    rb_define_method(Cdbc, "procedures", dbc_procs, -1);
    rb_define_method(Cdbc, "procedure_columns", dbc_proccols, -1);
    rb_define_method(Cdbc, "special_columns", dbc_speccols, -1);
    rb_define_method(Cdbc, "prepare", stmt_prep, -1);
    rb_define_method(Cdbc, "run", stmt_run, -1);
    rb_define_method(Cdbc, "do", stmt_do, -1);
    rb_define_method(Cdbc, "proc", stmt_proc, 1);

    /* connection options */
    rb_define_method(Cdbc, "autocommit", dbc_autocommit, -1);
    rb_define_method(Cdbc, "autocommit=", dbc_autocommit, -1);
    rb_define_method(Cdbc, "concurrency", dbc_concurrency, -1);
    rb_define_method(Cdbc, "concurrency=", dbc_concurrency, -1);
    rb_define_method(Cdbc, "maxrows", dbc_maxrows, -1);
    rb_define_method(Cdbc, "maxrows=", dbc_maxrows, -1);
    rb_define_method(Cdbc, "timeout", dbc_timeout, -1);
    rb_define_method(Cdbc, "timeout=", dbc_timeout, -1);
    rb_define_method(Cdbc, "maxlength", dbc_maxlength, -1);
    rb_define_method(Cdbc, "maxlength=", dbc_maxlength, -1);
    rb_define_method(Cdbc, "rowsetsize", dbc_rowsetsize, -1);
    rb_define_method(Cdbc, "rowsetsize=", dbc_rowsetsize, -1);
    rb_define_method(Cdbc, "cursortype", dbc_cursortype, -1);
    rb_define_method(Cdbc, "cursortype=", dbc_cursortype, -1);
    rb_define_method(Cdbc, "noscan", dbc_noscan, -1);
    rb_define_method(Cdbc, "noscan=", dbc_noscan, -1);
    rb_define_method(Cdbc, "ignorecase", stmt_ignorecase, -1);
    rb_define_method(Cdbc, "ignorecase=", stmt_ignorecase, -1);

    /* statement methods */
    rb_define_method(Cstmt, "drop", stmt_drop, 0);
    rb_define_method(Cstmt, "close", stmt_close, 0);
    rb_define_method(Cstmt, "cancel", stmt_cancel, 0);
    rb_define_method(Cstmt, "column", stmt_column, -1);
    rb_define_method(Cstmt, "columns", stmt_columns, -1);
    rb_define_method(Cstmt, "parameter", stmt_param, -1);
    rb_define_method(Cstmt, "parameters", stmt_params, 0);
    rb_define_method(Cstmt, "ncols", stmt_ncols, 0);
    rb_define_method(Cstmt, "nrows", stmt_nrows, 0);
    rb_define_method(Cstmt, "nparams", stmt_nparams, 0);
    rb_define_method(Cstmt, "cursorname", stmt_cursorname, -1);
    rb_define_method(Cstmt, "cursorname=", stmt_cursorname, -1);
    rb_define_method(Cstmt, "fetch", stmt_fetch, 0);
    rb_define_method(Cstmt, "fetch!", stmt_fetch_bang, 0);
    rb_define_method(Cstmt, "fetch_first", stmt_fetch_first, 0);
    rb_define_method(Cstmt, "fetch_first!", stmt_fetch_first_bang, 0);
    rb_define_method(Cstmt, "fetch_scroll", stmt_fetch_scroll, -1);
    rb_define_method(Cstmt, "fetch_scroll!", stmt_fetch_scroll_bang, -1);
    rb_define_method(Cstmt, "fetch_hash", stmt_fetch_hash, -1);
    rb_define_method(Cstmt, "fetch_hash!", stmt_fetch_hash_bang, -1);
    rb_define_method(Cstmt, "fetch_first_hash", stmt_fetch_first_hash, 0);
    rb_define_method(Cstmt, "fetch_many", stmt_fetch_many, 1);
    rb_define_method(Cstmt, "fetch_all", stmt_fetch_all, 0);
    rb_define_method(Cstmt, "each", stmt_each, 0);
    rb_define_method(Cstmt, "each_hash", stmt_each_hash, -1);
    rb_define_method(Cstmt, "execute", stmt_exec, -1);
    rb_define_method(Cstmt, "make_proc", stmt_procwrap, -1);
    rb_define_method(Cstmt, "more_results", stmt_more_results, 0);
    rb_define_singleton_method(Cstmt, "make_proc", stmt_procwrap, -1);

    /* data type methods */
#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Cdate, date_alloc);
#else
    rb_define_singleton_method(Cdate, "new", date_new, -1);
#endif
    rb_define_singleton_method(Cdate, "_load", date_load, 1);
    rb_define_method(Cdate, "initialize", date_init, -1);
    rb_define_method(Cdate, "clone", date_clone, 0);
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

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Ctime, time_alloc);
#else
    rb_define_singleton_method(Ctime, "new", time_new, -1);
#endif
    rb_define_singleton_method(Ctime, "_load", time_load, 1);
    rb_define_method(Ctime, "initialize", time_init, -1);
    rb_define_method(Ctime, "clone", time_clone, 0);
    rb_define_method(Ctime, "to_s", time_to_s, 0);
    rb_define_method(Ctime, "_dump", time_dump, 1);
    rb_define_method(Ctime, "inspect", time_inspect, 0);
    rb_define_method(Ctime, "hour", time_hour, -1);
    rb_define_method(Ctime, "minute", time_min, -1);
    rb_define_method(Ctime, "second", time_sec, -1);
    rb_define_method(Ctime, "hour=", time_hour, -1);
    rb_define_method(Ctime, "minute=", time_min, -1);
    rb_define_method(Ctime, "second=", time_sec, -1);
    rb_define_method(Ctime, "<=>", time_cmp, 1);

#ifdef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_define_alloc_func(Ctimestamp, timestamp_alloc);
#else
    rb_define_singleton_method(Ctimestamp, "new", timestamp_new, -1);
#endif
    rb_define_singleton_method(Ctimestamp, "_load", timestamp_load, 1);
    rb_define_method(Ctimestamp, "initialize", timestamp_init, -1);
    rb_define_method(Ctimestamp, "clone", timestamp_clone, 0);
    rb_define_method(Ctimestamp, "to_s", timestamp_to_s, 0);
    rb_define_method(Ctimestamp, "_dump", timestamp_dump, 1);
    rb_define_method(Ctimestamp, "inspect", timestamp_inspect, 0);
    rb_define_method(Ctimestamp, "year", timestamp_year, -1);
    rb_define_method(Ctimestamp, "month", timestamp_month, -1);
    rb_define_method(Ctimestamp, "day", timestamp_day, -1);
    rb_define_method(Ctimestamp, "hour", timestamp_hour, -1);
    rb_define_method(Ctimestamp, "minute", timestamp_min, -1);
    rb_define_method(Ctimestamp, "second", timestamp_sec, -1);
    rb_define_method(Ctimestamp, "fraction", timestamp_fraction, -1);
    rb_define_method(Ctimestamp, "year=", timestamp_year, -1);
    rb_define_method(Ctimestamp, "month=", timestamp_month, -1);
    rb_define_method(Ctimestamp, "day=", timestamp_day, -1);
    rb_define_method(Ctimestamp, "hour=", timestamp_hour, -1);
    rb_define_method(Ctimestamp, "minute=", timestamp_min, -1);
    rb_define_method(Ctimestamp, "second=", timestamp_sec, -1);
    rb_define_method(Ctimestamp, "fraction=", timestamp_fraction, -1);
    rb_define_method(Ctimestamp, "<=>", timestamp_cmp, 1);

    /* procedure methods */
    rb_define_method(Cproc, "initialize", stmt_proc_init, -1);
    rb_define_method(Cproc, "call", stmt_proc_call, -1);
    rb_define_method(Cproc, "[]", stmt_proc_call, -1);
#ifndef HAVE_RB_DEFINE_ALLOC_FUNC
    rb_enable_super(Cproc, "call");
    rb_enable_super(Cproc, "[]");
#endif

    /* constants */
    for (i = 0; o_const[i].name != NULL; i++) {
	rb_define_const(Modbc, o_const[i].name, INT2NUM(o_const[i].value));
    }

#ifdef TRACING
    if (ruby_verbose) {
	tracing = -1;
    }
#endif
}
