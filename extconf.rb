require 'mkmf'

dir_config("odbc")
have_header("sql.h")
have_header("sqlext.h")
have_header("odbcinst.h")
have_library("odbc")
create_makefile("odbc")
