let Analytic = require('../lib/select_analytic.js')
let sql = []

sql.push(`delete from u_table  where fielda=1`)

let _sql = sql.pop()
console.log('\x1b[0m')
var tables = Analytic.getTableNames(_sql,'delete')
	,where = Analytic.getWhereCondition(_sql , tables)

console.log('\x1b[31mtables:' , tables, '\nwhere:' ,JSON.stringify(where,null,4))
