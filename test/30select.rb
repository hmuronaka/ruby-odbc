$q = $c.prepare("select id,str from test")

if $q.column(0).name != "id" then raise "fetch failed" end
if $q.column(1).name != "str" then raise "fetch failed" end

$q.execute
if $q.fetch != [1, "foo"] then raise "fetch: failed" end
if $q.fetch != [2, "bar"] then raise "fetch: failed" end
if $q.fetch != [3, "FOO"] then raise "fetch: failed" end
if $q.fetch != [4, "BAR"] then raise "fetch: failed" end
if $q.fetch != nil then raise "fetch: failed" end
$q.close
if $q.execute.entries != [[1, "foo"], [2, "bar"], [3, "FOO"], [4, "BAR"]] then
  raise "fetch: failed"
end


a = []
$q.execute {|r| a=r.entries}
if a.size != 4 then raise "fetch: failed" end

a = []
$q.execute.each {|r| a.push(r)}
if a.size != 4 then raise "fetch: failed" end

a = []
$q.execute.each_hash {|r| a.push(r)}
if a.size != 4 then raise "fetch: failed" end
