let Lib = require('./lib.js')
	,Cache = require('./cache.js')
	,Conf = require('../conf.json')
	,Parser = require('./parser.js')

let CacheTables = Conf.cache_tables 
	,getAfffectTableName = Parser.getAfffectTableName
	,getWhereCondition = Parser.getWhereCondition
	,getInsertRecord = Parser.getInsertRecord
	,getUpdatedField = Parser.getUpdatedField

let _cacheable = {} 

let _CACHE_STATE = {
	"UNCACHE" : -1,
	"CACHED" : 2,
	"PROCESSING" : 1,
}

//const MATCHALL = true 
function throwErr(err){
	throw 'unsupport operation : ' + err.toString()
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
	return getWhereCondition(sql , tables)
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

/*
 * get all fields
 * 如果有字段在右侧的 比如 1 = fielda 暂时解析不了 应标记为不缓存todo 
 */
function _getFields(sql){
	let sqld = sql.replace(/(\"|\').*?\1/,'--')
	sqld = sqld.replace(/\bbetween .*? and /ig,'between')
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

function tryConvertStr(str){
	if ('string' !== typeof str) return str
	if (str.match(/^('|")/)){
		str = str.slice(1,-1)
	}else{
		if (/^(\-)?[\d|\.]+$/.test(str)) {
			str = str * 1
		}
	}
	return str

}
/*
 * in 
 * between
 * >
 * >=
 * <
 * <=
 * =
 * !=     
 * not in 
 * is (not) null
 * */

function _explodeWhere(sql){
	let fields = _getFields(sql)
	let ret = {}		


	function process_value(value ,val){
		value = value.trim()
		val = val || {}


		//TODO 和时间相关的不能缓存
		function putNewVal(type ,new_val,is_array){
			if (!is_array){
				//convert new_val to real string|number
				new_val = new_val.toString().trim()
				new_val = tryConvertStr(new_val)
			}
			if ('eq' === type || 'ne' === type ){
				if (!(type in val)) val[type] = []
				if (is_array){
					val[type] = val[type].concat(new_val)
				}else{
					val[type].push(new_val)
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
				val[type].push(new_val)
			}else if ('null' === type ){
				val[type] = new_val 
			}
		}

		if (value.indexOf('in') === 0 || value.indexOf('like') === 0 || value.indexOf('not') === 0){
			// in ,not in,like ,not like			
			let type
			let _is_not = false
			let _is_array = false

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
				_is_array = true
			}else if(value.indexOf('like') === 0){
				type = _is_not ? 'nl' : 'lk'
				newval = value.slice(4).trim()

				if (newval.indexOf('%') === -1){
					type = 'eq'
				}
			}else{
				throwErr(value)
			}

			putNewVal(type , newval )

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

	//console.log('sql' , sql)
	let _sql_tomatch = sql
	let reg = new RegExp('^(' + fields.join('|') + ')\\b *(=|>|<|is|in|not|between|\!=)','i')		
	while (true){
		_sql_tomatch = _sql_tomatch.trim()
		if (!_sql_tomatch.length) break
		let _seek_field  = reg.exec(_sql_tomatch) 
		if (!_seek_field) break

		let field = _seek_field[1]
		let value

		_sql_tomatch = _sql_tomatch.slice(_seek_field.index + _seek_field[1].length ).trim()
		//console.log('_sql_tomatch: [%s][%s]',field, _sql_tomatch)
		let leap = 0
		if (/^between\b/.test(_sql_tomatch)){
			//is between need one and
			leap = 1
		}
		//找到引号之后最近的and 或 or

		let _seek_reg = /("|').*?\1|\b(and|or)\b/i	
		function _findValue(){
			let _seek_parta = "" 
			if (!_sql_tomatch) return '' 
			let __seek_value = _seek_reg.exec(_sql_tomatch)
			//console.log('_seek_parta' ,_seek_parta,'|||',_sql_tomatch)
			//console.log( '__seek_value' ,  __seek_value)
			if (__seek_value){
				let _seek_value_part = __seek_value[0]
				let _parta_index_right = __seek_value.index + _seek_value_part.length
				if ('and' === _seek_value_part || 'or' === _seek_value_part){
					_seek_parta = _sql_tomatch.slice( 0 ,__seek_value.index)
					_sql_tomatch = _sql_tomatch.slice( _parta_index_right )

					if (leap-- > 0){
						_seek_parta += _seek_value_part
						_seek_parta += _findValue()
					}
					return  _seek_parta 
				}else{
					_seek_parta = _sql_tomatch.slice( 0 , _parta_index_right)
					_sql_tomatch = _sql_tomatch.slice(_parta_index_right)
					_seek_parta += _findValue()
					return _seek_parta
				}
			}else{
				_seek_parta = _sql_tomatch
				_sql_tomatch = ''
				return  _seek_parta
			}
		}

		value = _findValue()

		//console.log('fv',field, value )
		value = process_value(value ,ret[field])
		if (!ret[field] ) {
			ret[field] = value
		}else {
			Object.assign(ret[field], value)	
		}
	}

	return  ret	
}

/*
!insert into table  values (...) 
!insert into table  set field=value 
insert into table (...) values (...)
insert into table (...) values (...), (....)
!insert into table (...) select ... from another_table 
*/
function _get_newvalue(sql , tables){
	let table = tables[0]
		,table_name = table.name	
		,ret = []

	//console.log(sql)
	let inserted = sql.split(table_name)[1].trim()
		,fields = inserted.match(/\(([^\)]+)\) +values/i)

	if (!fields) return false

	inserted = inserted.slice(fields[1].length + 2).trim()
	fields = fields[1].split(',').map(v => v.trim())

	let op_type = inserted.trim().split(' ',2)[0]
	inserted = inserted.slice(op_type.length).trim()
	if ('values' === op_type){
		//TODO 判断是否多值
		inserted = inserted.slice(1,-1).trim()
		///console.log('newval' ,inserted)
		inserted = _extra_value(inserted , fields.length)
	}else {
		//需要异步查询 暂不支持
		return false
	}

	function getRowObj(fields ,values){
		let row = {}	
		for(let i = 0,j = fields.length;i < j;i++){
			row[fields[i]] = values[i]
		}
		return row
	}

	if (inserted.is_multi){
		inserted.values.forEach(row => {
			ret.push(getRowObj(fields ,row))	
		})
		
	}else{
		ret.push(getRowObj(fields ,inserted.values))
	}
	return ret
}

function _extra_value(inserted , field_len){
	//console.log( 'inserted',inserted)
	let _seek_reg = /("|').*?\1|\,/i	
		,seeked
		,values = []
		,leap = false
		,count = 0

	function putValueIn(element){
		values.push(tryConvertStr(element))
		count++
	}
	while (seeked = _seek_reg.exec(inserted)){
		//console.log('seeked',seeked)
		let pos = seeked.index
		seeked = seeked[0]
		///console.log('input', inserted ,leap , count)
		///console.log('seeked' ,seeked)
		if (seeked === ','){
			let element = inserted.slice(0,pos).trim()
			///console.log('val' ,element)
			inserted = inserted.slice(pos + 1)
			//如果是多行记录，判断这个,是否是折行
			if (count && (count+1) % field_len === 0) {
				element = element.replace(/\)$/,'').trim()
				inserted = inserted.replace(/^\(/,'').trim()
			}
				
			if (leap){
				leap = false
				if (count && count % field_len === 0) {
					inserted = inserted.replace(/^\(/,'').trim()
				}
			}else{
				putValueIn(element)
			}
		}else{
			///console.log('val' ,seeked)
			putValueIn(seeked)
			inserted = inserted.slice(pos + seeked.length  )
			///console.log('left' , inserted)
			leap = true
		}
		inserted = inserted.trim()
	
		if (!inserted.length) break
	}
	if (inserted) values.push(tryConvertStr(inserted.trim()))
	let row_nums = values.length / field_len
		,is_multi = false
	if (row_nums > 1){
		//多值
		if (values.length % field_len === 0) {	
			let all_values = []
			for (let i = 0 ;i < row_nums;i++){
				let row_item = values.splice(0,field_len)
				all_values.push(row_item)
			}
			values = all_values
			is_multi = true
		}else{
			return false
		}
	}
	//console.log('values' ,values)
	return {values : values, is_multi : is_multi}

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

	if (_cacheable[query.id] === _CACHE_STATE.UNCACHE) return false

	//TODO 放到mongodb
	if (_cacheable[query.id] === _CACHE_STATE.CACHED) return true 

	let _to_cache
	let tables
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
	_cacheable[query.id] = _CACHE_STATE.CACHED
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
	CacheTables = conf
}


exports.wrap = wrap
exports.setCache = setCache
//TODO 错误的sql
exports.isCacheAble = isQueryCacheAble 
exports.dataChange = dataChange 
exports.getUseDB = Parser.getUseDB 
