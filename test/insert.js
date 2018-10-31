let Analytic = require('../lib/select_analytic.js')
let sql = []

sql.push(`insert into u_table  (flda,fldb) select bflda,bfldb from subtable`)
sql.push(`insert into u_table  (flda,fldb,fldc,fldd,flde,fldf , fldg) values (124,"a(fx, value) bc" , 10 , ' values ' , ' values 2' ,99, 100)`)
sql.push(`insert into dbone.u_table  (flda,fldb,fldc) values (124,"a(b)c","789") ,(456,"efg","0"), ("rfg",     999 ,"rrr")`)

sql.push(`insert into dbone.u_table  (flda,fldb,fldc) values (124,"a(b)c",789) ,(456,"efg",0)`)

sql.push(`insert into dbone.u_table  (flda,fldb,fldc) values (124,'a(b)c"',789) ,(456,"ef'g",0)`)
let _sql = sql.pop()
console.log('\x1b[0m')
var tables = Analytic.getTableNames(_sql,'insert','dbone')
	,where = Analytic.getInsertRecord(_sql , tables)

console.log('\x1b[31mtables:' , tables, '\nnew:' ,JSON.stringify(where,null,4))
