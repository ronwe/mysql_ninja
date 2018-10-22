
let Analytic = require('../lib/select_analytic.js')
//.getTableNames
//.getWhereCondition
//.setCacheTables

var sql = 'select * from u_merchant_user u,d_spot_collection_dir as d where d.mid = u.mid and u.mid>1000 order by u.mid desc limit 10';

var sql = 'select * from lavaradio.u_merchant_user u join d_spot_collection_dir as d on d.mid = u.mid where u.mid>1000 and u.agent_id in("1,2" ,100 ) or u.create_time between 1000 and 2000 order by u.mid desc limit 10';


var sql = 'select abc from u_merchant_user,jointable x where aaa = x.abc and b like \'abc\' or b not like "2%"  and b <-1 '

var sql = `select * from u_merchant_user u,d_spot_collection_dir as d where
	   d.mid = u.mid and
		   u.mid>1000 order by u.mid desc limit 10`
var sql = `select * from u_merchant_user u,d_spot_collection_dir as d where
	   d.mid = u.mid`

var sql = `select * from u_merchant_user u left join d_spot_collection_dir as d on 
	   d.mid = u.mid
	   where d.create_time < 1000 and u.type in (1,2,4) or u.level = 1
	   `
var sql = `select * from u_merchant_user u,d_spot_collection_dir as d where
	   d.mid = concat(u.mid ,"1 and 2") and
		   1=1 order by u.mid desc limit 10`


var sql = 'select abc from u_merchant_user where  b is not null and b <-1 '

var sql = 'select abc from table where  fielda between 1 and 100 and fieldb <-1 or fieldc="fielda" '

/*
var sql = `select abc from u_merchant_user u join jointable x
on x.a = u.b 
where  u.b = " sth join sth" and u.b <-1` 
*/
console.log('\x1b[0m')
var tables = Analytic.getTableNames(sql)
	,where = Analytic.getWhereCondition(sql , tables)

console.log('\x1b[31mtables:' , tables, '\nwhere:' ,JSON.stringify(where,null,4))