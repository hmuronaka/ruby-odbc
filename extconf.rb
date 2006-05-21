require 'mkmf'

def have_library_ex(lib, func="main", headers=nil)
  checking_for "#{func}() in -l#{lib}" do
    libs = append_library($libs, lib)
    if !func.nil? && !func.empty? && COMMON_LIBS.include?(lib)
      true
    elsif try_func(func, libs, headers)
      $libs = libs
      true
    else
      false
    end
  end
end
 
dir_config("odbc")
have_header("sql.h")
have_header("sqlext.h")
begin
  if PLATFORM !~ /(mingw|cygwin)/ then
    header = "sqltypes.h"
  else
    header = ["windows.h", "sqltypes.h"]
  end
  if defined? have_type
    have_type("SQLTCHAR", header)
  else
    throw
  end
rescue
  puts "WARNING: please check sqltypes.h for SQLTCHAR manually,"
  puts "WARNING: if defined, modify CFLAGS in Makefile to contain"
  puts "WARNING: the option -DHAVE_TYPE_SQLTCHAR"
end
$have_odbcinst_h = have_header("odbcinst.h")

if PLATFORM =~ /mswin32/ then
  if !have_library_ex("odbc32", "SQLAllocConnect", "sql.h") ||
     !have_library_ex("odbccp32", "SQLConfigDataSource", "odbcinst.h") ||
     !have_library_ex("odbccp32", "SQLInstallerError", "odbcinst.h") ||
     !have_library("user32", "CharUpper") then
    puts "Can not locate odbc libraries"
    exit 1
  end
  have_func("SQLInstallerError", "odbcinst.h")
# mingw untested !!!
elsif PLATFORM =~ /(mingw|cygwin)/ then
  have_library("odbc32", "")
  have_library("odbccp32", "")
  have_library("user32", "")
else
  have_library("odbc", "SQLAllocConnect") ||
    have_library("iodbc", "SQLAllocConnect")
  ($have_odbcinst_h &&
    have_library("odbcinst", "SQLConfigDataSource")) ||
  ($have_odbcinst_h &&
    have_library("iodbcinst", "SQLConfigDataSource"))
  $have_odbcinst_h &&
    have_func("SQLInstallerError", "odbcinst.h")
end

create_makefile("odbc")
