let Parser = require('../lib/parser.js')
let sql = []

sql.push(`update u_table set ab=1 where fielda=1`)

let _sql = sql.pop()
console.log('\x1b[0m')
var tables = Parser.getTableNames(_sql,'update','dbone')
	,where = Parser.getWhereCondition(_sql , tables)

console.log('\x1b[31mtables:' , tables, '\nwhere:' ,JSON.stringify(where,null,4))
