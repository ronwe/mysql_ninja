let Lib = require('../lib.js')
	,LruCache = require('../lru.js')
	,Conf = require('../../conf.json')
	,FileStore = require('./file.js')
	,Nedb = require('nedb')
	,Path = require('path')

//TODO 索引从内存获得 body放入存储中
let CacheCan  = new LruCache(100)
	,MemCan = {}

let db_path = Path.resolve(__dirname,'../data', Conf.query_db || 'query.db')
const db = new Nedb({
	filename: db_path ,
	autoload: true
})

function set(query, buff ,cbk){
	return new Promise((resolve,reject) => {
		let id = query.id
		//MemCan[id] = buff
		
		FileStore.put(id, buff,function(err , pieces){
			if (err) return reject(err)

			let _saved = {
				id : query.id ,
				//default_db : query.default_db ,
				sql : query.sql,
				condition: query.influence
			}
			db.insert(_saved , (err, ret) => {
				CacheCan.set(id ,{ 
					fd : pieces 
				})
				resolve()
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
		FileStore.get(cache_pieces, function(err,body){
			if (err) return reject(err)
			resolve( body)
		})
	})
}

function del(query){
	if (!query.id) return false
	CacheCan.del(query.id)
	
}

exports.readBody = readBody
exports.del = del 
exports.get = get 
exports.set  = set 
