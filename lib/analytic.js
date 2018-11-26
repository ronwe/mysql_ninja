let Lib = require('./lib.js')
	,Conf = require('../conf.json')
	,Parser = require('./parser.js')
	,Lru = require('./lru.js')
	,Cache = require('./cache/index.js') 
	,ComQuery = require('./parse/comquery.js')

let Child = require('child_process')
	,Path = require('path')

let _CacheAble = new Lru(Conf.analytic || 1000) 
	,getQueryCom = Parser.getQueryCom
	,CacheTables = Conf.cache_tables 

let _CACHE_STATE = {
	"UNCACHE" : -1,
	"CACHED" : 2,
	"PROCESSING" : 1,
}


let analytic_child = Child.fork(Path.resolve(__dirname,'../analytic_child.js'))


analytic_child.on('message', (msg) => {
	if (msg && msg.code === 0){
		if (msg.id_name){
			let query = msg.input
				,id_name = msg.id_name

			if (false === id_name){
				_CacheAble.set(query.id, _CACHE_STATE.UNCACHE)
			}else{
				_CacheAble.set(query.id, id_name)
			}
		}else if (msg.affected){
			let result = msg.affected
				,query = msg.input
			//find influence
			Cache.clean(result)
		}
	}else{
		console.log('child fetal' , msg)
	}
})


//const MATCHALL = true 
function throwErr(err){
	throw 'unsupport operation : ' + err.toString()
}

//检查影响范围
function dataChange(query){
	if (!query.id) return false
	///analytic_child.send({affect:query})
}

function getChangeQueryCom(query ,type){
	if (!query.id) return false
	let query_sql = getQueryCom(query.sql,type , query.default_db ,CacheTables)
		,query_com = new ComQuery(query_sql)
	return {sql : query_sql , buff :query_com.write()}
}

function isQueryCacheAble(query){
	if (!query.id) return false

	let state = _CacheAble.get(query.id)
	if (_CACHE_STATE.UNCACHE === state) return false
	if (_CACHE_STATE.PROCESSING === state) return false
	if ('object' === typeof state) return state 

	_CacheAble.set(query.id, _CACHE_STATE.PROCESSING)

	analytic_child.send({query:query})
	//TODO 放到子进程里
	return

}

function setCached(query ){
	if (!query.id) return false
	//保存where条件.数据更新时比对
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

function fillCache(query , result ){
	if (!query || !query.influence) return false
	
	query.key_where = {}
	
	for (let _i = 0 , _j = result.columns.length ; _i < _j ;_i++){
		let col_name = result.columns[_i]
		if (col_name in query.influence){
			query.key_where[query.influence[col_name].name] = {
				table : query.influence[col_name].table,
				data : result.columns_values[_i]
			}
		}
	}

	Cache.set(query, [result.head , result.body])
	///analytic_child.send({fill:query , row_head: row_head , row_body:row_body})

}

exports.setCacheTables = function(conf){
	//TODO set to child process
	throw 'uncomplete'
	//CacheTables = conf
}

exports.fillCache = fillCache
exports.wrap = wrap
exports.setCached = setCached
//TODO 错误的sql
exports.isCacheAble = isQueryCacheAble 
exports.dataChange = dataChange 
exports.getChangeQueryCom = getChangeQueryCom
exports.getUseDB = Parser.getUseDB 
