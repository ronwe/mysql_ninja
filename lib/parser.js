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
/*分析from 的表名*/
function _get_afffect_tableName(type , sql ,default_db){
	sql = sql.toLowerCase()
	let sql_part
	switch(type){
		case 'select':
			sql_part = sql.split(/\b(where|limit|order|group|limit|on)\b/,1)[0]
				
			sql_part = sql_part.split('from')
			if (sql_part.length !== 2) return false
			
			sql_part = sql_part[1].trim().replace(' as ',' ').replace(/\ +/g,' ')
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
					'db' : db || default_db,
					'name' : b,
					'alias' : c ? c.trim() : b
				})
			})
			return tables 
			break
		case 'update':
			sql_part = sql.split(' where ',1)[0]
			sql_part = sql_part.split('update ')
			if (sql_part.length !== 2) return false
			sql_part = sql_part[1]
			sql_part = sql_part.split(' set ')
			if (sql_part.length !== 2) return false

			let update_table = sql_part[0].trim()
			let update_db
			if (update_table.indexOf('.') > 0){
				update_table = update_table.split('.',2)
				update_db = update_table[0]
				update_table = update_table[1]
			}
			return [{
				'db' : update_db || default_db || '',
				'name' : update_table
			}]	
			break
		case 'delete':
			sql_part = sql.split(' where ',1)[0]
			sql_part = sql_part.split('delete ')
			if (sql_part.length !== 2) return false
			sql_part = ' ' + sql_part[1] 
			sql_part = sql_part.split(' from ')
			if (sql_part.length !== 2) return false

			let delete_table = sql_part[1].trim()
				,delete_db
			if (delete_table.indexOf('.') > 0){
				delete_table = delete_table.split('.',2)
				delete_db = delete_table[0]
				delete_table = delete_table[1]
			}
			return [{
				'db' : delete_db || default_db || '',
				'name' : delete_table 
			}]	
			break
		case 'insert':
			sql_part = sql.replace(/^insert +into +/ , '')
			let insert_table = sql_part.split(' ')[0]
				,insert_db
			if (insert_table.indexOf('.') > 0){
				insert_table = insert_table.split('.',2)
				insert_db = insert_table[0]
				insert_table = insert_table[1]
			}
			return [{
				'db' : insert_db || default_db || '',
				'name' : insert_table
			}]
			break

	}

}

function _get_where(sql,tables) {
	if (!tables) return false
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
	
	sql = sql.replace(/[\n\t]/g,' ').split(/(limit|order|group|limit)/i,1)[0]
	sql = sql.split(/\b(from|update)\b/i)[2]


	let part_where = []
	//拆分 where 
	sql = sql.split(/where/i)
	
	if (sql[1]){
		part_where = part_where.concat(_explodeWhere(sql[1]))
	}
	sql = sql[0]
	//join on
	//todo 查询条件里有" join " 会有bug
	let part_join = sql.split(/ join /i)
	//console.log('sql' , part_join)
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
	 *  }
	 */
	function putResult(field , val){
		let cur
		if (!ret_all[field.table]){
			ret_all[field.table] = {} 
		}
		cur = ret_all[field.table]
		cur[field.field_name] = val
		cur.db = field.db
	}


	//console.log('tables' ,tables)
	let _default_table = tables[0].name
	let _table_match_all = [] 
	//console.log(JSON.stringify(part_where))
	let _where_check = part_where.every(where => {
		let _succ = true
		for (let field in where){
			/// 字段在右侧的缓存不了
			if (!/^[a-z][\w\.]+$/.test(field)) {
				_succ = false
				break	
			}
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
					if (item.toString().indexOf('now()') !== -1){
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
	//console.log('ret_all' , ret_all)
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
	//如果没有查询条件 需要设定为全部	
	if (Object.keys(ret_all).length === 0 ){
		tables.forEach(table => {
			ret_all[table.name] = {
				all: true,
				db : table.db
			}
		})
	}
	
	//console.log('>>>',sql ,tables)
	return ret_all
}

exports.getUseDB = function getUseDB(sql){
	return sql.replace(/^use\b/,'').trim().toLowerCase()	
}

exports.getWhereCondition = _get_where
exports.getInsertRecord = _get_newvalue
exports.getAfffectTableName = _get_afffect_tableName 

exports.getTableNames = function(sql ,type , default_db){
	return _get_afffect_tableName(type || 'select',sql , default_db)
}
