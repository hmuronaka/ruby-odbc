require 'mkmf'

dir_config("odbc")
have_header("sql.h")
have_header("sqlext.h")
$have_odbcinst_h = have_header("odbcinst.h")

# mingw untested !!!
if PLATFORM =~ /(mswin32|mingw|cygwin)/ then
  have_library("odbc32", "")
  have_library("odbccp32", "")
  have_library("user32", "")
  if PLATFORM =~ /(mingw|cygwin)/ then
    $have_odbcinst_h && 
      $defs.push("-DHAVE_SQLCONFIGDATASOURCEW")
  else
    $have_odbcinst_h && 
      have_func("SQLConfigDataSourceW", "odbcinst.h")
  end
else
  have_library("odbc", "SQLAllocConnect") ||
    have_library("iodbc", "SQLAllocConnect")
  $have_odbcinst_h &&
    have_library("odbcinst", "SQLConfigDataSource")
  $have_odbcinst_h &&
    have_func("SQLConfigDataSourceW", "odbcinst.h")
end

create_makefile("odbc_utf8")
