q = $c.prepare("select * from test")
q.execute
if q.nrows != 2 then raise "nrows: failed" end
if q.fetch != [1, "foo"] then raise "fetch: failed" end
if q.fetch != [2, "bar"] then raise "fetch: failed" end
if q.fetch != nil then raise "fetch: failed" end
q.close
