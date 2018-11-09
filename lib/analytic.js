let Lib = require('./lib.js')
	,Conf = require('../conf.json')
	,Parser = require('./parser.js')
	,Lru = require('./lru.js')
let Child = require('child_process')
	,path = require('path')

let getAfffectTableName = Parser.getAfffectTableName
	,getWhereCondition = Parser.getWhereCondition
	,getInsertRecord = Parser.getInsertRecord
	,getUpdatedField = Parser.getUpdatedField

let _CacheAble = new Lru(Conf.analytic || 1000) 

let _CACHE_STATE = {
	"UNCACHE" : -1,
	"CACHED" : 2,
	"PROCESSING" : 1,
}


let analytic_child = Child.fork(path.resolve(__dirname,'../analytic_child.js'))


//const MATCHALL = true 
function throwErr(err){
	throw 'unsupport operation : ' + err.toString()
}

//检查影响范围
function dataChange(query){
	if (!query.id) return false
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
	console.log(query , affected)
	
}

function isQueryCacheAble(query){
	if (!query.id) return false

	let state = _CacheAble.get(query.id)
	if (_CACHE_STATE.UNCACHE === state) return false

	if (_CACHE_STATE.CACHED === state) return true 

	if (_CACHE_STATE.PROCESSING === state) return false
	_CacheAble.set(query.id, _CACHE_STATE.PROCESSING)
	
	analytic_child.on('message', (msg) => {
		if (msg && msg.code === 0){
			if (msg.result){
				let result = msg.result
				if (false === result){
					_CacheAble.set(query.id, _CACHE_STATE.UNCACHE)
				}else{
					_CacheAble.set(query.id,_CACHE_STATE.CACHED)
				}
			}
		}
	})

	analytic_child.send({query:query})
	//TODO 放到子进程里
	return

}

function setCached(query ){
	if (!query.id) return false
	_CacheAble.set(query.id , _CACHE_STATE.CACHED)
}


function wrap(sql,_default_db,type){
	let id = Lib.md5(sql)
	return {
		id : id,
		default_db : _default_db || '',
		type : type ? type.toLowerCase() : '',
		sql : sql,
	}
}


exports.setCacheTables = function(conf){
	//TODO set to child process
	throw 'uncomplete'
	//CacheTables = conf
}


exports.wrap = wrap
exports.setCached = setCached
//TODO 错误的sql
exports.isCacheAble = isQueryCacheAble 
exports.dataChange = dataChange 
exports.getUseDB = Parser.getUseDB 
