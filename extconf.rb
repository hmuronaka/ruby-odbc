require 'mkmf'

def have_library_ex(lib, func="main", headers=nil)
  checking_for "#{func}() in -l#{lib}" do
    libs = append_library($libs, lib)
    if func && func != "" && COMMON_LIBS.include?(lib)
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
$have_odbcinst_h = have_header("odbcinst.h")

if PLATFORM =~ /mswin32/ then
  if !have_library_ex("odbc32", "SQLAllocConnect", "sql.h") ||
  !have_library_ex("odbccp32", "SQLConfigDataSource", "odbcinst.h") ||
  !have_library("user32", "CharUpper") then
    puts "Can not locate odbc libraries"
    exit 1
  end
# mingw untested !!!
elsif PLATFORM =~ /(mingw|cygwin)/ then
  have_library("odbc32", "")
  have_library("odbccp32", "")
  have_library("user32", "")
else
  have_library("odbc", "SQLAllocConnect") ||
    have_library("iodbc", "SQLAllocConnect")
  $have_odbcinst_h &&
    have_library("odbcinst", "SQLConfigDataSource")
end

create_makefile("odbc")
