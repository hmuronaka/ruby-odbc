require 'mkmf'

dir_config("odbc")
have_header("sql.h")
have_header("sqlext.h")
have_header("odbcinst.h")

# mingw untested !!!
if PLATFORM =~ /(mswin32|mingw|cygwin)/ then
  have_library("odbc32", "")
  have_library("odbccp32", "")
  have_library("user32", "")
else
  have_library("odbc", "SQLAllocConnect")
end

create_makefile("odbc")
