//TODO 动态加载更新
let Lib = require('./lib.js')
let Cache = require('./select_cache.js')

let Conf = require('../conf.json')

let CacheTables = Conf.cache_tables 

let _cacheable = {} 

let _CACHE_STATE = {
	"UNCACHE" : -1,
	"CACHED" : 2,
	"PROCESSING" : 1,
}

const MATCHALL = true 
function throwErr(err){
	throw err
}

/*分析from 的表名*/
function getAfffectTableName(type , sql){
	sql = sql.toLowerCase()
	switch(type){
		case 'select':
			let sql_part = sql.split(/\b(where|limit|order|group|limit|on)\b/,1)[0]
			sql_part = sql_part.split('from')[1].trim().replace(' as ',' ').replace(/\ +/g,' ')
			//console.log('sql_part',sql_part)
			let table_reg =/(?:^|,|join\ )([\w\.]+)(\ \w+)?/g
			let tables = []
			sql_part.replace(table_reg , function(a,b,c){
				//可能跨库
				b = b.trim().split('.',2)
				let db
				if (b.length > 1){
					db = b[0]
					b = b[1]
				}else {
					b = b[0]
				}
				//todo 根据use调整默认数据库
				//now_db

				tables.push({
					'db' : db || '',
					'name' : b,
					'alias' : c ? c.trim() : b
				})
			})
			return tables 
			break
		case 'update':
			break
		case 'delete':
			break
		case 'insert':
			break

	}

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
	if (_CACHE_STATE.PROCESSING === _cacheable[query.id]) return false
	_cacheable[query.id] = _CACHE_STATE.PROCESSING

	let sql = query.sql
	return _get_where(sql , tables)
}

/*
 * 将别名转为表名 
 * 
 */
function _processField(field, alias_map, table_map ,db_map,default_table){
	let field_part = field.split('.',2)		
		,table
		,field_name
	if (field_part.length === 2){
		table = alias_map[field_part[0]] 
		if (!table){
			if (field_part[0] in table_map){
				table = field_part[0] 
			}else{
				return false
			}
		}
		field_name = field_part[1]
	}else if (field_part.length === 1){
		field_name = field_part[0]
		table = default_table
	}else{
		return false
	}
	return {
		table : table
		,field_name : field_name	
		,db : db_map[table] || ''
	}
}

//get all fields
function _getFields(sql){
	let sqld = sql.replace(/(\"|\').*?\1/,'--')
	sqld = sqld.replace(/\bbetween .*? and /ig,'between')
	//console.log('sqld',sqld)
	let field_part = sqld.replace(/\bor\b/g,'and').split(/\band\b/i)
	let fields = []
	field_part.forEach( field_p => {
		let field = field_p.trim().match(/^([\w\.]+)( |>|<|=)/)	
		if (field && fields.indexOf(field[1]) === -1){
			fields.push(field[1])
		}
	})
	return fields
}
/*
 * in 
 * between
 * >
 * >=
 * <
 * <=
 * =
 * !=     #miss
 * not in #miss 
 * 未完成is (not) null
 * */

function _explodeWhere(sql){
	let fields = _getFields(sql)
	let ret = {}		
	let sql_split = sql.split(new RegExp('\\b(' + fields.join('|') + ')\\b'))

	function process_value(value ,val){
		value = value.trim()
		val = val || {}


		//TODO 和时间相关的不能缓存
		function putNewVal(type ,new_val,is_array){
			if ('eq' === type || 'ne' === type ){
				if (!(type in val)) val[type] = []
				if (is_array){
					val[type] = val[type].concat(new_val)
				}else{
					val[type].push(new_val.trim())
				}
			}else if ('lt' === type ){
				if (!(type in val)) {
					 val[type] = new_val 
				}else{
					val[type] = Math.min(val[type],	new_val)
				}
			}else if ('gt' === type ){
				if (!(type in val)) {
					 val[type] = new_val 
				}else{
					val[type] = Math.max(val[type],	new_val)
				}
			}else if ('lk' === type || 'nl' === type){
				if (!(type in val)) val[type] = []
				val[type].push(new_val.trim())
			}else if ('null' === type ){
				val[type] = new_val 
			}
		}

		if (value.indexOf('in') === 0 || value.indexOf('like') === 0 || value.indexOf('not') === 0){
			// in ,not in,like ,not like			
			let type
			let _is_not = false

			if (value.indexOf('not') === 0){
				_is_not = true
				value = value.slice(3).trim()
			}

			let newval
			if (value.indexOf('in') === 0){
				type = _is_not ? 'ne' : 'eq'
				value = value.slice(2).trim().replace(/^\(/,'').replace(/\)$/,'')

				let is_comfuse = value.match(/('|")[^\1]+\1/g) 
				if (is_comfuse){
					is_comfuse = !is_comfuse.every( s => s.indexOf(',') === -1)
				}
				if (is_comfuse){
					var p = new Function("return [" + value + "]")
					newval = p() 
				}else {
					newval = value.split(',') 
				}
			}else if(value.indexOf('like') === 0){
				type = _is_not ? 'nl' : 'lk'
				newval = value.slice(4).trim().replace(/^["']/,'').replace(/["']$/,'').trim()
				if (newval.indexOf('%') === -1){
					type = 'eq'
				}
			}else{
				throwErr(value)
			}

			putNewVal(type , newval , true)

		}else if (value.match(/^is +(not +)?null$/i)){
			//is | is not
			value = value.toLowerCase().slice(2).trim()
			type = 'null'
			let _v = true
			if (value.indexOf('not') === 0){
				_v = false
			}	
			putNewVal(type , _v)

		}else if (value.indexOf('between') === 0){
			value = value.replace(/^between\b/,'').trim()
			value = value.split(' and ')
			
			putNewVal('lt' , value[0])
			putNewVal('gt' , value[1])

			putNewVal('eq' , value[0])
			putNewVal('eq' , value[1])
		}else if (value.indexOf('>=') === 0 ){
			value = value.slice(2)
			putNewVal('gt' , value)
			putNewVal('eq' , value)
		}else if (value.indexOf('<=') === 0 ){
			value = value.slicea(2)
			putNewVal('lt' , value)
			putNewVal('eq' , value)
		}else if (value.indexOf('>') === 0 ){
			value = value.slice(1)
			putNewVal('gt' , value)
		}else if (value.indexOf('<') === 0 ){
			value = value.slice(1)
			putNewVal('lt' , value)
		}else if (value.indexOf('=') === 0 ){
			value = value.slice(1)
			putNewVal('eq' , value)
		}else if (value.indexOf('!=') === 0  || value.indexOf('<>') === 0){
			value = value.slice(2)
			putNewVal('ne' , value)
		}else{
			throwErr(value)
		}
		return val
	}

	
	for (var i = 1;i < sql_split.length; i +=2){ 
		let field = sql_split[i]	
			,value = sql_split[i+1]
		value = value.trim().replace(/\b(and|or)$/g,'')
		value = process_value(value ,ret[field])
		if (!ret[field] ) {
			ret[field] = value
		}else {
			Object.assign(ret[field], value)	
		}
	}
	console.log('ret' , ret)

	return  ret	
}

function _get_where(sql,tables){
	let table_hash = {}
		,table_alias_hash = {}
		,table_db_hash = {}
		,table_check
		,ret_all = {}
	table_check = tables.every(t => {
		if (table_alias_hash[t.alias] || table_hash[t.name]){
			//如果别名冲突，或者跨库有同名表会放弃
			return false
		} 
		table_hash[t.name] =  t.alias
		table_alias_hash[t.alias] =  t.name
		table_db_hash[t.name] = t.db
		return true
	})

	if (!table_check) {
		console.error('table confict')
		return false
	}
	
	sql = sql.split(/(limit|order|group|limit)/i,1)[0]
	sql = sql.split(/from/i)[1]

	let part_where = []
	//拆分 where 
	sql = sql.split(/where/i)
	
	part_where = part_where.concat(_explodeWhere(sql[1]))

	sql = sql[0]
	//join on
	//todo 查询条件里有" join " 会有bug
	let part_join = sql.split(/ join /i)
	for (let i = 1 ;i < part_join.length; i++){
		// join xxx on abc
		let part_on = part_join[1].split(' on ')[1]
		part_where = part_where.concat(_explodeWhere(part_on))
	}
	
	let result_where = {}
	//console.log('table_alias_hash' ,table_alias_hash)
	/*
	 * where 包括 eq 数组，lt < ,gt >,like ,not like ,in,not in ,is ,is not
	 */

	/*
	 * return {表a: {
	 *  ___ : [true], 全部
	 *  字段1:[1,3,4],
	 */
	function putResult(field , val){
		let cur
		if (!ret_all[field.table]){
			ret_all[field.table] = {} 
		}
		cur = ret_all[field.table]
		cur[field.field_name] = val === null ? MATCHALL : val
		cur.db = field.db
	}

	console.log('tables' ,tables)
	let _default_table = tables[0].name
	let _table_match_all = [] 
	let _where_check = part_where.every(where => {
		let _succ = true
		for (let field in where){
			let field_val = where[field]
			let _field_processed = _processField(field , table_alias_hash ,table_hash,table_db_hash ,_default_table)		
			if (!_field_processed) {
				_succ = false
				break	
			}

			 //值里的跨表
			let _val_to_process = []
			let _put_to_collect = true

			if (field_val.lt) _val_to_process.push(field_val.lt)
			if (field_val.gt) _val_to_process.push(field_val.gt)
			if (field_val.eq) _val_to_process = _val_to_process.concat(field_val.eq)
			_val_to_process.every( item => {
				let field_on_othertable = item.toString().match(/\b[\w\_]+\.[\w\_]+\b/)    
				//如果有b.field1 = a.field1 则同时放入
				if (field_on_othertable){
					let _processed = _processField(field_on_othertable[0], table_alias_hash , table_hash ,table_db_hash,_default_table)		
					if (!_processed) {
						_succ = false
						return false	
					}
					_table_match_all.push(_field_processed)
					_table_match_all.push(_processed)

					//有跨表的where 不放到集合里
					_put_to_collect = false
				}else {		
					//如果字段包括时间计算 now() 则不缓存 
					if (item.indexOf('now()') !== -1){
						_succ = false
						return false	
					}
				}
				return true
			})

			if (_put_to_collect){
				putResult(_field_processed , field_val)
			}
		}
		return _succ 
	})

	if (!_where_check){
		return false
	}
	if (_table_match_all.length){
		_table_match_all.forEach(item => {
			let cur = ret_all[item.table]
			let _where_all = {
				all : true,
				db  : item.db
			}
			if (cur) {
				if (cur[item.field_name]){
					return
				}else{
					//如果这个表没有其它查询条件，则放入matchall, 有插入或者更新字段则缓存失效
					let _where_tables = Object.keys(cur)
					if (_where_tables.length === 1 && _where_tables[0] === 'db'){
						cur[item.field_name] = _where_all
					}
				}
			}else{
				ret_all[item.table] = {}
				ret_all[item.table][item.field_name] = _where_all
			}
		})
	}

	//如果有多个表 但字段未指定表则失效
	
	//console.log('>>>',sql ,tables)
	return ret_all
}

function isCacheAble(query){
	if (!query.id) return false

	if (_cacheable[query.id] === _CACHE_STATE.UNCACHE) return false

	//TODO 放到mongodb
	if (_cacheable[query.id] === _CACHE_STATE.CACHED) return true 

	let _to_cache
	//TODO 子查询需要取出处理 暂时不支持
	if (query.sql.toLowerCase().split(' from ').length > 2){
		_to_cache = false
	}else{
		let tables = getAfffectTableName('select' , query.sql)
		console.log('tables' ,tables)
		/*所有表都在名单中才缓存*/
		_to_cache = tables && tables.length && tables.every(table => {
			return table.name in CacheTables
		})
	}

	if (!_to_cache) {
		_cacheable[query.id] = _CACHE_STATE.UNCACHE
		return false
	}else{
		/*分析字段 分析不了的不cache 并标注*/
		let _where = getWhere(query, tables)
		if ('object' === typeof _where){
			return true
		}else{
			return false
		}
	}
	//测试	
	return false
}

function setCache(query ,buff){
	Cache.set(query, buff)
}

function wrap(sql){
	let id = Lib.md5(sql)
	return {
		id : id,
		sql : sql
	}
}

exports.getWhereCondition = function(sql ,tables){
	return _get_where(sql ,tables)
}

exports.getTableNames = function(sql){
	return getAfffectTableName('select',sql)
}
exports.setCacheTables = function(conf){
	CacheTables = conf
}

exports.wrap = wrap
exports.setCache = setCache
exports.isCacheAble = isCacheAble
