/*
 * odbc.c --
 *
 *	ODBC-Ruby binding
 *
 * $Id: odbc.c,v 1.2 2001/05/13 06:10:05 chw Exp chw $
 */

#include "ruby.h"
#include <sql.h>
#include <sqlext.h>
#include <odbcinst.h>

typedef struct {
    VALUE self;
    SQLHENV env;
    SQLHDBC dbc;
    int nstmt;
    int maydisc;
} RODBC;

typedef struct {
    RODBC *odbc;
    SQLHSTMT stmt;
    int nump;
    int *pinfo;
    int ncols;
    int *coltypes;
} RSTMT;

static VALUE Codbc;
static VALUE Cstmt;
static VALUE Cerror;
static VALUE Cdsn;
static VALUE Cdrv;

/*
 * Forward declarations.
 */

static VALUE stmt_exec(int argc, VALUE *argv, VALUE self);


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
 *      GC free callback for RODBC.
 *
 *----------------------------------------------------------------------
 */

static void
free_odbc(RODBC *p)
{
    if (p->dbc != SQL_NULL_HDBC) {
        SQLDisconnect(p->dbc);
        SQLFreeConnect(p->dbc);
	p->dbc = SQL_NULL_HDBC;
    }
    if (p->env != SQL_NULL_HENV) {
        SQLFreeEnv(p->env);
	p->env = SQL_NULL_HENV;
    }
}

/*
 *----------------------------------------------------------------------
 *
 *      GC free callback for RODBC.
 *
 *----------------------------------------------------------------------
 */

static void
mark_odbc(RODBC *p)
{
    if (p->dbc != SQL_NULL_HDBC ||
	p->env != SQL_NULL_HENV ||
	p->nstmt != 0) {
        rb_gc_mark(p->self);
    }
}

/*
 *----------------------------------------------------------------------
 *
 *      GC free callback for statement.
 *
 *----------------------------------------------------------------------
 */

static void
free_stmt(RSTMT *q)
{
    if (q->stmt != SQL_NULL_HSTMT) {
        RODBC *p = q->odbc;

        SQLFreeStmt(q->stmt, SQL_DROP);
	q->stmt = SQL_NULL_HSTMT;
	if (q->pinfo) {
	    xfree(q->pinfo);
	    q->pinfo = NULL;
	}
	if (q->coltypes) {
	    xfree(q->coltypes);
	    q->coltypes = NULL;
	}
	p->nstmt--;
	if (p->nstmt == 0 && p->maydisc) {
	    if (p->dbc != SQL_NULL_HDBC) {
	        SQLDisconnect(p->dbc);
		SQLFreeConnect(p->dbc);
		p->dbc = SQL_NULL_HDBC;
	    }
	}
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
    rb_cvar_set(Codbc, rb_intern("@@error"), v);
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
get_err(env, dbc, stmt)
{
    char msg[SQL_MAX_MESSAGE_LENGTH];
    char st[6];
    char buf[32];
    int err;
    SQLINTEGER nerr;
    SQLSMALLINT len;
    VALUE v;

    switch (err = SQLError(env, dbc, stmt, st, &nerr, msg, sizeof (msg) - 1,
			    &len)) {
    case SQL_SUCCESS:
        v = rb_str_new2(st);
	sprintf(buf, " (%d) ", (int) nerr);
	v = rb_str_cat2(v, buf);
	v = rb_str_cat(v, msg, len);
	break;
    case SQL_NO_DATA_FOUND:
        v = Qnil;
	break;
    default:
        sprintf(msg, "unknown error %d", err);
        v = rb_str_new2(msg);
	break;
    }
    rb_cvar_set(Codbc, rb_intern("@@error"), v);
    return v == Qnil ? NULL : STR2CSTR(v);
}

/*
 *----------------------------------------------------------------------
 *
 *      Obtain array of known DSNs.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_dsns(VALUE self)
{
    int result = 0;
    char dsn[SQL_MAX_DSN_LENGTH], descr[256];
    SWORD dsnLen, descrLen;
    int first = 1;
    VALUE aret;
    SQLHENV env = SQL_NULL_HENV;

    SQLAllocEnv(&env);
    if (env == SQL_NULL_HENV) {
        rb_raise(Cerror, set_err("cannot allocate SQLHENV"));
    }
    aret = rb_ary_new();
    while ((result = SQLDataSources(env, first ?
				    SQL_FETCH_FIRST : SQL_FETCH_NEXT,
				    (UCHAR *) dsn, sizeof (dsn), &dsnLen,
				    (UCHAR *) descr, sizeof (descr),
				    &descrLen))
	   == SQL_SUCCESS) {
        VALUE odsn = rb_obj_alloc(Cdsn);

	rb_iv_set(odsn, "@name", rb_str_new(dsn, dsnLen));
	rb_iv_set(odsn, "@descr", rb_str_new(descr, descrLen));
	rb_ary_push(aret, odsn);
	first = 0;
    }
    SQLFreeEnv(env);
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
odbc_drivers(VALUE self)
{
    int result = 0;
    char driver[256], attrs[1024], *attr;
    SWORD driverLen, attrsLen;
    int first = 1;
    VALUE aret;
    SQLHENV env = SQL_NULL_HENV;

    SQLAllocEnv(&env);
    if (env == SQL_NULL_HENV) {
        rb_raise(Cerror, set_err("cannot allocate SQLHENV"));
    }
    aret = rb_ary_new();
    while ((result = SQLDrivers(env, first ?
				SQL_FETCH_FIRST : SQL_FETCH_NEXT,
				(UCHAR *) driver, sizeof (driver), &driverLen,
				(UCHAR *) attrs, sizeof (attrs), &attrsLen))
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
    SQLFreeEnv(env);
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
    VALUE drv, attr, issys, astr, a, x;

    rb_scan_args(argc, argv, "12", &drv, &attr, &issys);
    if (TYPE(drv) == Cdrv) {
        x = rb_iv_get(drv, "@name");
	a = rb_iv_get(drv, "@attrs");
        if (issys != Qnil) {
	    rb_raise(rb_eArgError, "wrong # of arguments");
	}
        issys = attr;
	drv = x;
	attr = a;
    }
    if (issys != Qnil && issys != Qtrue && issys != Qfalse) {
        rb_raise(rb_eTypeError, "expecting boolean argument");
    }
    Check_Type(drv, T_STRING);
    Check_Type(attr, T_HASH);
    if (issys == Qtrue) {
        switch (op) {
	case ODBC_ADD_DSN:	op = ODBC_ADD_SYS_DSN; break;
	case ODBC_CONFIG_DSN:	op = ODBC_CONFIG_SYS_DSN; break;
	case ODBC_REMOVE_DSN:	op = ODBC_REMOVE_SYS_DSN; break;
	}
    }
    astr = rb_str_new2("");
    a = rb_funcall(attr, rb_intern("keys"), 0, NULL);
    while ((x = rb_ary_shift(a)) != Qnil) {
        VALUE v = rb_hash_aref(attr, x);

	astr = rb_str_concat(astr, x);
	astr = rb_str_cat2(astr, "=");
	astr = rb_str_concat(astr, v);
	astr = rb_str_cat(astr, "", 1);
    }
    astr = rb_str_cat(astr, "", 1);
    if (SQLConfigDataSource(NULL, op, STR2CSTR(drv), STR2CSTR(astr))) {
        return Qnil;
    }
    rb_raise(Cerror, set_err("DSN configuration error"));
}

static VALUE
odbc_adddsn(int argc, VALUE *argv, VALUE self)
{
    return conf_dsn(argc, argv, self, ODBC_ADD_DSN);
}

static VALUE
odbc_confdsn(int argc, VALUE *argv, VALUE self)
{
    return conf_dsn(argc, argv, self, ODBC_CONFIG_DSN);
}

static VALUE
odbc_deldsn(int argc, VALUE *argv, VALUE self)
{
    return conf_dsn(argc, argv, self, ODBC_REMOVE_DSN);
}


/*
 *----------------------------------------------------------------------
 *
 *      ODBC instance initializer.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_new(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE obj;

    obj = Data_Make_Struct(self, RODBC, mark_odbc, free_odbc, p);
    p->self = obj;
    p->env = SQL_NULL_HENV;
    p->dbc = SQL_NULL_HDBC;
    p->nstmt = 0;
    p->maydisc = 0;
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
odbc_connect(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE dsn, user, passwd;
    char *sdsn, *suser = "", *spasswd = "";
    SQLHDBC dbc;

    rb_scan_args(argc, argv, "12", &dsn, &user, &passwd);
    if (CLASS_OF(dsn) == Cdsn) {
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
    Data_Get_Struct(self, RODBC, p);
    if (p->nstmt != 0) {
        rb_raise(Cerror, set_err("already connected with active SQLHSTMT(s)"));
    }
    if (p->env == SQL_NULL_HENV) {
        SQLAllocEnv(&p->env);
	if (p->env == SQL_NULL_HENV) {
	    rb_raise(Cerror, set_err("cannot allocate SQLHENV"));
	}
    }
    if (SQLAllocConnect(p->env, &dbc) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(p->env, NULL, NULL));
    }
    if (SQLConnect(dbc, (UCHAR *) sdsn, SQL_NTS, (UCHAR *) suser, SQL_NTS,
		   (UCHAR *) spasswd, SQL_NTS) != SQL_SUCCESS) {
        char *msg = get_err(p->env, dbc, NULL);

	SQLFreeConnect(dbc);
	rb_raise(Cerror, msg);
    }
    if (p->dbc != SQL_NULL_HDBC) {
        SQLDisconnect(p->dbc);
        SQLFreeConnect(p->dbc);
	p->dbc = SQL_NULL_HDBC;
    }
    p->dbc = dbc;
    return Qnil;
}

static VALUE
odbc_drvconnect(VALUE self, VALUE drv)
{
    RODBC *p;
    char *sdrv;
    SQLHDBC dbc;

    if (CLASS_OF(drv) == Cdrv) {
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
    Data_Get_Struct(self, RODBC, p);
    if (p->nstmt != 0) {
        rb_raise(Cerror, set_err("already connected with active SQLHSTMT(s)"));
    }
    if (p->env == SQL_NULL_HENV) {
        SQLAllocEnv(&p->env);
	if (p->env == SQL_NULL_HENV) {
	    rb_raise(Cerror, set_err("cannot allocate SQLHENV"));
	}
    }
    if (SQLAllocConnect(p->env, &dbc) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(p->env, NULL, NULL));
    }
    if (SQLDriverConnect(dbc, NULL, (UCHAR *) sdrv, SQL_NTS,
			 NULL, 0, NULL, SQL_DRIVER_NOPROMPT) != SQL_SUCCESS) {
        char *msg = get_err(p->env, dbc, NULL);

	SQLFreeConnect(dbc);
	rb_raise(Cerror, msg);
    }
    if (p->dbc != SQL_NULL_HDBC) {
        SQLDisconnect(p->dbc);
        SQLFreeConnect(p->dbc);
	p->dbc = SQL_NULL_HDBC;
    }
    p->dbc = dbc;
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Disconnect from data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_disconnect(VALUE self)
{
    RODBC *p;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc != SQL_NULL_HDBC && p->nstmt == 0) {
        SQLDisconnect(p->dbc);
        SQLFreeConnect(p->dbc);
	p->dbc = SQL_NULL_HDBC;
    } else {
        p->maydisc++;
    }
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Fill column type array for statement.
 *
 *----------------------------------------------------------------------
 */

static int *
make_coltypes(SQLHSTMT stmt, int ncols)
{
    int i, dummy, *ret = NULL;
    
    for (i = 1; i <= ncols; i++) {
        if (SQLColAttributes(stmt, i, SQL_COLUMN_TYPE,
			     NULL, 0, NULL, (void *) &dummy) != SQL_SUCCESS) {
	    return ret;
	}
    }
    ret = ALLOC_N(int, ncols);
    for (i = 1; i <= ncols; i++) {
        SQLColAttributes(stmt, i, SQL_COLUMN_TYPE, NULL, 0, NULL,
			 (void *) &dummy);
	switch (dummy) {
	case SQL_SMALLINT:
	case SQL_INTEGER:
	    dummy = SQL_C_LONG;
	    break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
	case SQL_REAL:
	    dummy = SQL_C_DOUBLE;
	    break;
	default:
	    dummy = SQL_C_CHAR;
	    break;
	}
	ret[i - 1] = dummy;
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

static int *
make_pinfo(SQLHSTMT stmt, int nump)
{
    int i, *pinfo = NULL;

    pinfo = ALLOC_N(int, nump * 4);
    for (i = 0; i < nump; i++) {
        SQLSMALLINT type, scale, nullable;
	SQLINTEGER col;

        switch (SQLDescribeParam(stmt, i + 1, &type, &col, &scale,
				 &nullable)) {
	case SQL_SUCCESS:
	    pinfo[i * 4 + 0] = type;
	    pinfo[i * 4 + 1] = col;
	    pinfo[i * 4 + 2] = scale;
	    pinfo[i * 4 + 3] = nullable;
	    break;
	default:
	    pinfo[i * 4 + 0] = SQL_VARCHAR;
	    pinfo[i * 4 + 1] = 0;
	    pinfo[i * 4 + 2] = 0;
	    pinfo[i * 4 + 3] = SQL_NULLABLE_UNKNOWN;
	    break;
	}
    }
    return pinfo;
}

/*
 *----------------------------------------------------------------------
 *
 *      Create ODBCStmt with result.
 *
 *----------------------------------------------------------------------
 */

static VALUE
make_result(RODBC *p, SQLHSTMT stmt, VALUE result)
{
    RSTMT *q;
    SQLSMALLINT cols, nump;
    int *pinfo = NULL, *coltypes = NULL;
    char *msg;

    if (SQLNumParams(stmt, &nump) != SQL_SUCCESS) {
        nump = 0;
    }
    if (nump > 0) {
        pinfo = make_pinfo(stmt, nump);
	if (pinfo == NULL) {
	    goto error;
	}
    }
    if (SQLNumResultCols(stmt, &cols) != SQL_SUCCESS) {
        cols = 0;
    }
    if (cols > 0) {
        coltypes = make_coltypes(stmt, cols);
	if (coltypes == NULL) {
	    goto error;
	}
    }
    if (result == Qnil) {
        result = Data_Make_Struct(Cstmt, RSTMT, 0, free_stmt, q);
    } else {
        Data_Get_Struct(result, RSTMT, q);
	if (q->pinfo != NULL) {
	    xfree(q->pinfo);
	}
	if (q->coltypes != NULL) {
	    xfree(q->coltypes);
	}
    }
    p->nstmt++;
    q->odbc = p;
    q->stmt = stmt;
    q->nump = nump;
    q->pinfo = pinfo;
    q->ncols = cols;
    q->coltypes = coltypes;
    return result;
error:
    msg = get_err(NULL, NULL, stmt); 
    SQLFreeStmt(stmt, SQL_DROP);
    if (result != Qnil) {
        Data_Get_Struct(result, RSTMT, q);
	if (q->stmt == stmt) {
	    q->stmt = SQL_NULL_HSTMT;
	}
    }
    rb_raise(Cerror, msg);
    return Qnil;
}

/*
 *----------------------------------------------------------------------
 *
 *      Query tables of data source.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_tables(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE tab;
    char *stab = NULL;
    SQLHSTMT stmt;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    rb_scan_args(argc, argv, "01", &tab);
    if (tab != Qnil) {
        Check_Type(tab, T_STRING);
        stab = STR2CSTR(tab);
    }
    if (SQLAllocStmt(p->dbc, &stmt) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
    }
    if (SQLTables(stmt, NULL, 0, NULL, 0,
		  stab == NULL ? NULL : stab, SQL_NTS, NULL, 0)
	!= SQL_SUCCESS) {
        char *msg = get_err(NULL, NULL, stmt);

        SQLFreeStmt(stmt, SQL_DROP);
        rb_raise(Cerror, msg);
    }
    return make_result(p, stmt, Qnil);
}

/*
 *----------------------------------------------------------------------
 *
 *      Query columns of data source or tables.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_columns(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE col;
    char *scol = NULL;
    SQLHSTMT stmt;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    rb_scan_args(argc, argv, "01", &col);
    if (col != Qnil) {
        Check_Type(col, T_STRING);
        scol = STR2CSTR(col);
    }
    if (SQLAllocStmt(p->dbc, &stmt) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
    }
    if (SQLColumns(stmt, NULL, 0, NULL, 0,
		   scol == NULL ? NULL : scol, SQL_NTS,
		   NULL, 0)
	!= SQL_SUCCESS) {
        char *msg = get_err(NULL, NULL, stmt);

        SQLFreeStmt(stmt, SQL_DROP);
        rb_raise(Cerror, msg);
    }
    return make_result(p, stmt, Qnil);
}

/*
 *----------------------------------------------------------------------
 *
 *      Query indexes of table.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_indexes(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE tab;
    char *stab;
    SQLHSTMT stmt;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    rb_scan_args(argc, argv, "1", &tab);
    Check_Type(tab, T_STRING);
    stab = STR2CSTR(tab);
    if (SQLAllocStmt(p->dbc, &stmt) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
    }
    if (SQLStatistics(stmt, NULL, 0, NULL, 0, stab, SQL_NTS,
		      SQL_INDEX_ALL, SQL_ENSURE) != SQL_SUCCESS) {
        char *msg = get_err(NULL, NULL, stmt);

        SQLFreeStmt(stmt, SQL_DROP);
        rb_raise(Cerror, msg);
    }
    return make_result(p, stmt, Qnil);
}

/*
 *----------------------------------------------------------------------
 *
 *      Query type information of driver.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_types(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE type;
    int itype = SQL_ALL_TYPES;
    SQLHSTMT stmt;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    rb_scan_args(argc, argv, "01", &type);
    if (type != Qnil) {
        itype = NUM2INT(type);
    }
    if (SQLAllocStmt(p->dbc, &stmt) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
    }
    if (SQLGetTypeInfo(stmt, itype) != SQL_SUCCESS) {
        char *msg = get_err(NULL, NULL, stmt);

        SQLFreeStmt(stmt, SQL_DROP);
        rb_raise(Cerror, msg);
    }
    return make_result(p, stmt, Qnil);
}

/*
 *----------------------------------------------------------------------
 *
 *      ODBC prepare method: constructs/prepares statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_prep(int argc, VALUE *argv, VALUE self)
{
    RODBC *p;
    VALUE sql;
    char *ssql = NULL;
    SQLHSTMT stmt;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    rb_scan_args(argc, argv, "1", &sql);
    Check_Type(sql, T_STRING);
    ssql = STR2CSTR(sql);
    if (SQLAllocStmt(p->dbc, &stmt) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
    }
    if (SQLPrepare(stmt, ssql, SQL_NTS) != SQL_SUCCESS) {
        char *msg = get_err(NULL, NULL, stmt);

        SQLFreeStmt(stmt, SQL_DROP);
        rb_raise(Cerror, msg);
    }
    return make_result(p, stmt, Qnil);
}

/*
 *----------------------------------------------------------------------
 *
 *      ODBC run method: constructs/prepares/executes statement.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_run(int argc, VALUE *argv, VALUE self)
{
    return stmt_exec(argc - 1, argv + 1, odbc_prep(1, argv, self));
}

/*
 *----------------------------------------------------------------------
 *
 *      Transaction stuff.
 *
 *----------------------------------------------------------------------
 */

static VALUE
odbc_commit(VALUE self)
{
    RODBC *p;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    if (SQLTransact(p->env, p->dbc, SQL_COMMIT) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(p->env, p->dbc, NULL));
    }
    return Qnil;
}

static VALUE
odbc_rollback(VALUE self)
{
    RODBC *p;

    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    if (SQLTransact(p->env, p->dbc, SQL_ROLLBACK) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(p->env, p->dbc, NULL));
    }
    return Qnil;
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
    RODBC *p;
    VALUE val;
    SQLINTEGER v;

    rb_scan_args(argc, argv, "01", &val);
    Data_Get_Struct(self, RODBC, p);
    if (p->dbc == SQL_NULL_HDBC) {
        rb_raise(Cerror, set_err("no connection"));
    }
    if (val == Qnil) {
        if (SQLGetConnectOption(p->dbc, op, &v) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
	}
    }
    switch (op) {
    case SQL_AUTOCOMMIT:
    case SQL_NOSCAN:
        if (val == Qnil) {
	    return v ? Qtrue : Qfalse;
	}
	if (val == Qtrue) {
	    v = 1;
	} else if (val == Qfalse) {
	    v = 0;
	} else {
	    rb_raise(rb_eTypeError, "expecting boolean argument");
	}
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
    if (SQLSetConnectOption(p->dbc, op, v) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
    }
    return Qnil;
}

static VALUE
odbc_autocommit(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_AUTOCOMMIT);
}

static VALUE
odbc_concurrency(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_CONCURRENCY);
}

static VALUE
odbc_maxrows(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_MAX_ROWS);
}

static VALUE
odbc_timeout(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_QUERY_TIMEOUT);
}

static VALUE
odbc_maxlength(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_MAX_LENGTH);
}

static VALUE
odbc_rowsetsize(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_ROWSET_SIZE);
}

static VALUE
odbc_cursortype(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_CURSOR_TYPE);
}

static VALUE
odbc_noscan(int argc, VALUE *argv, VALUE self)
{
    return do_option(argc, argv, self, SQL_NOSCAN);
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
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    free_stmt(q);
    return Qnil;
}

static VALUE
stmt_close(VALUE self)
{
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    if (q->stmt != SQL_NULL_HSTMT) {
        SQLFreeStmt(q->stmt, SQL_CLOSE);
	if (q->pinfo) {
	    xfree(q->pinfo);
	    q->pinfo = NULL;
	}
	if (q->coltypes) {
	    xfree(q->coltypes);
	    q->coltypes = NULL;
	}
    }
    return Qnil;
}

static VALUE
stmt_ncols(VALUE self)
{
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    return INT2FIX(q->ncols);
}

static VALUE
stmt_nrows(VALUE self)
{
    RSTMT *q;
    SQLINTEGER rows;

    Data_Get_Struct(self, RSTMT, q);
    if (SQLRowCount(q->stmt, &rows) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
    }
    return INT2FIX(rows);
}

static VALUE
stmt_columns(VALUE self)
{
    RSTMT *q;
    SQLSMALLINT i;
    char colname[256];
    VALUE res;

    Data_Get_Struct(self, RSTMT, q);
    res = rb_ary_new2(q->ncols);
    for (i = 1; i <= q->ncols; i++) {
        if (SQLColAttribute(q->stmt, i, SQL_DESC_LABEL, colname,
			    sizeof (colname), NULL, NULL) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
	}
	rb_ary_push(res, rb_str_new2(colname));
    }
    return res;
}

static VALUE
do_fetch(RSTMT *q)
{
    int i, dummy, *lens;
    char **bufs;
    VALUE res;

    bufs = alloca(sizeof (char *) * q->ncols);
    lens = alloca(sizeof (int) * q->ncols);
    for (i = 1; i <= q->ncols; i++) {
        int type = q->coltypes[i - 1];

        if (SQLGetData(q->stmt, i, type, &dummy, type == SQL_C_CHAR ? 1 : 0,
		       (void *) &lens[i - 1]) == SQL_ERROR) {
	    rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
	}
	if (type == SQL_C_CHAR) {
	    lens[i - 1] += 1;
	}
	bufs[i - 1] = alloca(lens[i - 1]);
    }
    res = rb_ary_new2(q->ncols);
    for (i = 1; i <= q->ncols; i++) {
        int isnull = SQL_NULL_DATA, type = q->coltypes[i - 1];
	VALUE v;

        SQLGetData(q->stmt, i, type, bufs[i - 1], lens[i - 1],
		   (void *) &isnull);
	if (isnull == SQL_NULL_DATA) {
	    v = Qnil;
	} else {
	    switch (type) {
	    case SQL_C_LONG:
	        v = INT2NUM(*((int *) bufs[i - 1]));
		break;
	    case SQL_C_DOUBLE:
	        v = rb_float_new(*((double *) bufs[i - 1]));
		break;
	    default:
	        v = rb_str_new(bufs[i - 1], lens[i - 1] - 1);
		break;
	    }
	}
	rb_ary_push(res, v);
    }
    return res;
}

static VALUE
stmt_fetch(VALUE self)
{
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    if (q->ncols <= 0) {
        return Qnil;
    }
    switch (SQLFetch(q->stmt)) {
    case SQL_NO_DATA:
        return Qnil;
    case SQL_SUCCESS:
        return do_fetch(q);
        break;
    default:
        rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
    }
}

static VALUE
stmt_fetch_first(VALUE self)
{
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    if (q->ncols <= 0) {
        return Qnil;
    }
    switch (SQLFetchScroll(q->stmt, SQL_FETCH_FIRST, 0)) {
    case SQL_NO_DATA:
        return Qnil;
    case SQL_SUCCESS:
        return do_fetch(q);
        break;
    default:
        rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
    }
}

static VALUE
stmt_fetch_last(VALUE self)
{
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    if (q->ncols <= 0) {
        return Qnil;
    }
    switch (SQLFetchScroll(q->stmt, SQL_FETCH_LAST, 0)) {
    case SQL_NO_DATA:
        return Qnil;
    case SQL_SUCCESS:
        return do_fetch(q);
        break;
    default:
        rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
    }
}

static VALUE
stmt_fetch_prior(VALUE self)
{
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    if (q->ncols <= 0) {
        return Qnil;
    }
    switch (SQLFetchScroll(q->stmt, SQL_FETCH_PRIOR, 0)) {
    case SQL_NO_DATA:
        return Qnil;
    case SQL_SUCCESS:
        return do_fetch(q);
        break;
    default:
        rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
    }
}

static VALUE
stmt_each(VALUE self)
{
    VALUE row;
    int first;
    RSTMT *q;

    Data_Get_Struct(self, RSTMT, q);
    switch (SQLFetchScroll(q->stmt, SQL_FETCH_FIRST, 0)) {
    case SQL_NO_DATA:
    case SQL_SUCCESS:
        first = 1;
        break;
    default:
        first = 0;
    }
    while ((row = first ? stmt_fetch_first(self) : stmt_fetch(self)) != Qnil) {
        first = 0;
        rb_yield(row);
    }
    return self;
}

static VALUE
stmt_prep(int argc, VALUE *argv, VALUE self)
{
    RSTMT *q;
    VALUE sql;
    char *ssql = NULL;

    Data_Get_Struct(self, RSTMT, q);
    rb_scan_args(argc, argv, "1", &sql);
    Check_Type(sql, T_STRING);
    ssql = STR2CSTR(sql);
    if (q->stmt == SQL_NULL_HSTMT) {
        RODBC *p = q->odbc;

        if (SQLAllocStmt(p->dbc, &q->stmt) != SQL_SUCCESS) {
	    rb_raise(Cerror, get_err(NULL, p->dbc, NULL));
	}
    } else if (SQLFreeStmt(q->stmt, SQL_CLOSE) != SQL_SUCCESS) {
        rb_raise(Cerror, get_err(NULL, NULL, q->stmt));
    }
    if (SQLPrepare(q->stmt, ssql, SQL_NTS) != SQL_SUCCESS) {
        char *msg = get_err(NULL, NULL, q->stmt);

        SQLFreeStmt(q->stmt, SQL_DROP);
	q->stmt = SQL_NULL_HSTMT;
        rb_raise(Cerror, msg);
    }
    return make_result(q->odbc, q->stmt, self);
}

static VALUE
stmt_exec(int argc, VALUE *argv, VALUE self)
{
    RSTMT *q;
    int i;
    char *msg;

    Data_Get_Struct(self, RSTMT, q);
    if (argc > q->nump) {
        rb_raise(Cerror, set_err("too much parameters"));
    }
    for (i = 0; i < argc; i++) {
        char buf[32];
        void *valp = (void *) buf;
	int vlen, ctype;
	SQLINTEGER rlen;

        switch (TYPE(argv[i])) {
	case T_STRING:
	    ctype = SQL_C_CHAR;
	    valp = (void *) STR2CSTR(argv[i]);
	    rlen = strlen((char *) valp);
	    vlen = rlen + 1;
	    break;
	case T_FIXNUM:
	    ctype = SQL_C_LONG;
	    *(int *) valp = FIX2INT(argv[i]);
	    rlen = 1;
	    vlen = sizeof (int);
	    break;
	case T_FLOAT:
	case T_BIGNUM:
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
	    ctype = SQL_C_CHAR;
	    valp = (void *) STR2CSTR(rb_any_to_s(argv[i]));
	    rlen = strlen((char *) valp);
	    vlen = rlen + 1;
	    break;
	}
        if (SQLBindParameter(q->stmt, i + 1, SQL_PARAM_INPUT,
			     ctype, q->pinfo[i * 4 + 0],
			     q->pinfo[i * 4 + 1], q->pinfo[i * 4 + 2],
			     valp, vlen, &rlen) != SQL_SUCCESS) {
	    goto error;
	}
    }
    for (; i < q->nump; i++) {
        SQLINTEGER isnull = SQL_NULL_DATA;

        if (SQLBindParameter(q->stmt, i + 1, SQL_PARAM_INPUT,
			     SQL_C_CHAR, q->pinfo[i * 4 + 0],
			     q->pinfo[i * 4 + 1], q->pinfo[i * 4 + 2],
			     NULL, 0, &isnull) != SQL_SUCCESS) {
	    goto error;
	}
    }
    if (SQLExecute(q->stmt) != SQL_SUCCESS) {
error:
        msg = get_err(NULL, NULL, q->stmt);
        SQLFreeStmt(q->stmt, SQL_DROP);
	q->stmt = SQL_NULL_HSTMT;
        rb_raise(Cerror, msg);
    }
    return make_result(q->odbc, q->stmt, self);
}

/*
 *----------------------------------------------------------------------
 *
 *      Initialize this module.
 *
 *----------------------------------------------------------------------
 */

void
Init_odbc()
{
    Codbc = rb_define_class("ODBC", rb_cObject);
    rb_define_class_variable(Codbc, "@@error", Qnil);

    Cstmt = rb_define_class("ODBCStmt", rb_cObject);

    Cdsn = rb_define_class("ODBCDSN", rb_cObject);
    rb_funcall(Cdsn, rb_intern("attr_accessor"), 2,
	       rb_str_new2("name"), rb_str_new2("descr"));

    Cdrv = rb_define_class("ODBCDriver", rb_cObject);
    rb_funcall(Cdrv, rb_intern("attr_accessor"), 2,
	       rb_str_new2("name"), rb_str_new2("attrs"));

    Cerror = rb_define_class("ODBCError", rb_eStandardError);

    /* singleton methods and constructors */
    rb_define_singleton_method(Codbc, "new", odbc_new, -1);
    rb_define_singleton_method(Codbc, "datasources", odbc_dsns, 0);
    rb_define_singleton_method(Codbc, "drivers", odbc_drivers, 0);
    rb_define_singleton_method(Cdsn, "new", dsn_new, 0);
    rb_define_singleton_method(Cdrv, "new", drv_new, 0);
    rb_define_method(Cdsn, "initialize", dsn_init, 0);
    rb_define_method(Cdrv, "initialize", drv_init, 0);

    /* management things (odbcinst.h) */
    rb_define_singleton_method(Codbc, "add_dsn", odbc_adddsn, -1);
    rb_define_singleton_method(Codbc, "config_dsn", odbc_confdsn, -1);
    rb_define_singleton_method(Codbc, "del_dsn", odbc_deldsn, -1);

    /* connection methods */
    rb_define_method(Codbc, "initialize", odbc_connect, -1);
    rb_define_method(Codbc, "connect", odbc_connect, -1);
    rb_define_method(Codbc, "drvconnect", odbc_drvconnect, 1);
    rb_define_method(Codbc, "disconnect", odbc_disconnect, 0);
    rb_define_method(Codbc, "tables", odbc_tables, -1);
    rb_define_method(Codbc, "columns", odbc_columns, -1);
    rb_define_method(Codbc, "indexes", odbc_indexes, -1);
    rb_define_method(Codbc, "types", odbc_types, -1);
    rb_define_method(Codbc, "prepare", odbc_prep, -1);
    rb_define_method(Codbc, "run", odbc_run, -1);
    rb_define_method(Codbc, "commit", odbc_commit, 0);
    rb_define_method(Codbc, "rollback", odbc_rollback, 0);

    /* connection options */
    rb_define_method(Codbc, "autocommit", odbc_autocommit, -1);
    rb_define_method(Codbc, "concurrency", odbc_concurrency, -1);
    rb_define_method(Codbc, "maxrows", odbc_maxrows, -1);
    rb_define_method(Codbc, "timeout", odbc_timeout, -1);
    rb_define_method(Codbc, "maxlength", odbc_maxlength, -1);
    rb_define_method(Codbc, "rowsetsize", odbc_rowsetsize, -1);
    rb_define_method(Codbc, "cursortype", odbc_cursortype, -1);
    rb_define_method(Codbc, "noscan", odbc_noscan, -1);

    /* statement methods */
    rb_define_method(Cstmt, "drop", stmt_drop, 0);
    rb_define_method(Cstmt, "close", stmt_close, 0);
    rb_define_method(Cstmt, "columns", stmt_columns, 0);
    rb_define_method(Cstmt, "ncols", stmt_ncols, 0);
    rb_define_method(Cstmt, "nrows", stmt_nrows, 0);
    rb_define_method(Cstmt, "fetch", stmt_fetch, 0);
    rb_define_method(Cstmt, "fetch_first", stmt_fetch_first, 0);
    rb_define_method(Cstmt, "fetch_last", stmt_fetch_last, 0);
    rb_define_method(Cstmt, "fetch_prior", stmt_fetch_prior, 0);
    rb_define_method(Cstmt, "each", stmt_each, 0);
    rb_define_method(Cstmt, "prepare", stmt_prep, -1);
    rb_define_method(Cstmt, "execute", stmt_exec, -1);

    /* some useful constants */
    rb_define_const(Codbc, "SQL_CURSOR_FORWARD_ONLY",
		    INT2NUM(SQL_CURSOR_FORWARD_ONLY));
    rb_define_const(Codbc, "SQL_CURSOR_KEYSET_DRIVEN",
		    INT2NUM(SQL_CURSOR_KEYSET_DRIVEN));
    rb_define_const(Codbc, "SQL_CURSOR_DYNAMIC",
		    INT2NUM(SQL_CURSOR_DYNAMIC));
    rb_define_const(Codbc, "SQL_CURSOR_STATIC",
		    INT2NUM(SQL_CURSOR_STATIC));
    rb_define_const(Codbc, "SQL_CONCUR_READ_ONLY",
		    INT2NUM(SQL_CONCUR_READ_ONLY));
    rb_define_const(Codbc, "SQL_CONCUR_LOCK",
		    INT2NUM(SQL_CONCUR_LOCK));
    rb_define_const(Codbc, "SQL_CONCUR_ROWVER",
		    INT2NUM(SQL_CONCUR_ROWVER));
    rb_define_const(Codbc, "SQL_CONCUR_VALUES",
		    INT2NUM(SQL_CONCUR_VALUES));
}
