$q = $c.run("update test set id=0, str='hoge'")
if $q.nrows != 4 then raise "update: failed" end
