let Lib = require('../lib.js')
	,LruCache = require('../lru.js')
	,Conf = require('../../conf.json')
	,FileStore = require('./file.js')
	,Nedb = require('nedb')
	,Path = require('path')

var MongoClient = require('mongodb').MongoClient
	,db
var url = "mongodb://172.24.0.162:27017/";

MongoClient.connect(url, function(err, _db) {
	if (err) throw err;
	db = _db.db("lavaradio").collection("sss")
	console.log ("数据库已创建!");
});
//TODO 索引从内存获得 body放入存储中
let CacheCan  = new LruCache(10000)
	,BodyCan  = new LruCache(1000)
	,MemCan = {}

let db_path = Path.resolve(__dirname,'../data', Conf.query_db || 'query.db')
/*
const db = new Nedb({
	filename: db_path ,
	autoload: true
})
*/

function IDS(p){
	return Lib.md5(p.join ? p.join('') : JSON.stringify(p))
}

function set(query, buff ,cbk){
	return new Promise((resolve,reject) => {
		let id = query.id
		//MemCan[id] = buff

		FileStore.put(id, buff,function(err , pieces){
			if (err) return reject(err)

			let _saved = Object.assign({},query.key_where,{
				_id : query.id ,
				//default_db : query.default_db ,
				_sql : query.sql,
				fd : pieces
			})
			function dbWrited(){
				CacheCan.set(id ,{
					fd : pieces
				})
				BodyCan.set(IDS(pieces) , buff)
				resolve()
			}
			db.findOne({_id:_saved._id} ,function(err , rows){
				if (rows){
					dbWrited()
				}else{
					db.insertOne(_saved ,dbWrited)
				}
			})
		})
	})
}

function get(query){
	if (!query.id) return false
	let _cache = CacheCan.get(query.id)
	if (_cache){
		return _cache
	}
	return false
}

function readBody(cache_pieces){
	return new Promise((resolve,reject) => {
	/*
		db.findOne({
			fd : cache_id
		},(err, ret) => {
			if (err ) return reject(err)
			resolve(ret.data.map( p => Buffer.from(p)))
		})
		resolve(MemCan[cache_id])
	*/
		let _body_inmem = BodyCan.get(IDS(cache_pieces) )
		if (_body_inmem) return resolve(_body_inmem)
		FileStore.get(cache_pieces, function(err,body){
			if (err) return reject(err)
			resolve( body)
		})
	})
}

function del(query,doc){
	if (!query.id) return false
	let _cache = doc || CacheCan.get(query.id)
	///console.log('del ' , query.id ,_cache)
	CacheCan.del(query.id)
	db.remove({_id : query.id}, (err ,ret) =>{
	})
	FileStore.remove( _cache.fd)
}

function findAndClean(where){
	let _q = {}
		,_filed = Object.keys(where.where)[0]
		,_qkey = where.table.name + ':' + _filed
	_q[_qkey] = {}
	_q[_qkey]['$in']  = where.where[_filed]

	//console.log('where' ,where, _q)
	db.find(_q ,{_id: 1,fd:1} , (err , rows) => {
		if (err) return
		rows.forEach( row => {
			console.log(row)

			del({id : row._id},row)
		})
	})

}

function despatch(){
	// [ 'eq' , 'ne' , 'lt', 'gt' , 'lk' , 'nl' , 'null']
	let _q = {}
		,_table_db_map  = {}

	function getQField(_fld ,table, fname, type){
		let _name_new = table + '.' + fname + '.' + type
		if (_name_new in _fld) return _fld[_name_new]
		_fld[_name_new] = {}
		return _fld[_name_new]
	}

	///console.log('where' , where )
	for(let table in where){
		for (let field in where[table]){
			if ('db' === field) {
				_table_db_map[table] = where[table].db
			//	_q[table + '.db'] = where[table].db
				continue
			}


			let condition = where[table][field]
			if ('lt' in condition){
				getQField(_q ,table, field ,'lt')['$gt'] = condition.lt
			}
			if ('gt' in condition){
				getQField(_q ,table, field ,'gt')['$lt'] = condition.gt
			}
			if ('eq' in condition){
				getQField(_q ,table, field ,'eq')['$in'] = condition.eq
				getQField(_q ,table, field ,'ne')['$nin'] = condition.eq

				let lt = getQField(_q ,table, field ,'lt')
					,gt = getQField(_q,table , field ,'gt')
				condition.eq.forEach( el => {
					lt['$gt'] = ('$gt' in lt) ? Math.min(lt['$gt'],el) : el
					gt['$lt'] = ('$lt' in gt) ? Math.max(gt['$lt'],el) : el
				})
			}
			if ('ne' in condition){

			}
			if ('lk' in condition){

			}
			if ('nl' in condition){

			}
			if ('null' in condition){
				_q[table + '.' +  field + '.null'] = condition['null']
			}
			_q[table + '.' +  field + '.all'] = true

		}

	}
	///console.log('_q' ,_q)
	let _any = []
		,_and = []
	for(let k in _q){
		let _el = {}
		_el[k] = _q[k]
		_any.push(_el)
	}
	for(let k in _table_db_map){
		let _el = {}
		_el[k + '.db'] = _table_db_map[k]
		_and.push(_el)
	}

	console.log('_any' ,_any,'and',_and)
	//db.find({ $or: _any ,$and : _and} /*,{_id: 1,fd:1}*/, (err , rows) => {
	db.find({ $or: _any ,$and : _and} ,{_id: 1,fd:1}, (err , rows) => {
		if (err) return
		rows.forEach( row => {
			console.log(row)

			del({id : row._id},row)
		})
	})

}

exports.readBody = readBody
exports.clean = findAndClean
exports.del = del
exports.get = get
exports.set  = set
