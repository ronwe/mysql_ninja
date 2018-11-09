
let Conf = require('./conf.json')
	,CacheTables = Conf.cache_tables 
	,Parser = require('./lib/parser.js')

let getAfffectTableName = Parser.getAfffectTableName
	,getWhereCondition = Parser.getWhereCondition
	,getInsertRecord = Parser.getInsertRecord
	,getUpdatedField = Parser.getUpdatedField

function checkAffect(query,cbk){
	let tables = Parser.getTableNames(query.sql,query.type , query.default_db)
		,affected
	switch(query.type){
		case 'update':
		case 'delete':
			affected = getWhereCondition(query.sql,tables)
			/// getUpdatedField
			break
		case 'replace':
		case 'insert':
			affected = getInsertRecord(query.sql,tables)
			break
	}
	///TODO getUpdatedField
	///TODO 处理影响到的缓存
	///TODO 数据变动消息通知
	ckb( affected)
}
/*
 * 分析影响字段 
 * return {表a: {
 *			___ : [true], 全部
 *			字段1:[1,3,4],
 *			字段2:['blahblah']
 *			字段3:[true]
 *		}}
 * */

function getWhere(query,tables ){
	if (!query.id) return false
	///let state = _CacheAble.get(query.id) 

	let sql = query.sql
	return getWhereCondition(sql , tables)
}
function isQueryCacheAble(query ,cbk){
	let _to_cache
		,tables

	//TODO 子查询需要取出处理 暂时不支持
	if (query.sql.toLowerCase().split(' from ').length != 2){
		_to_cache = false
	}else{
		tables = getAfffectTableName('select' , query.sql , query.default_db)
		//console.log('tables' ,tables)
		/*所有表都在名单中才缓存*/
		_to_cache = tables && tables.length && tables.every(table => {
			return table.name in CacheTables
		})
	}

	if (!_to_cache) {
		return cbk(false)
	}else{
		/*分析字段 分析不了的不cache 并标注*/
		let _where = getWhere(query, tables)
		if ('object' === typeof _where){
			return cbk(_where) 
		}else{
			return cbk(false)
		}
	}
	//测试	
	return cbk(false)
}
process.on('message', (msg) => {
	if (msg.query){
		isQueryCacheAble(msg.query,function(ret){
    		process.send({
				code : 0,
				input : msg.query,
				result : ret
			})
		})
	}else if(msg.affect){
		checkAffect(msg.query, function(ret){
    		process.send({
				code : 0,
				input : msg.query,
				affected : ret
			})
		})
	}
})
