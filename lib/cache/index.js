let Lib = require('../lib.js')
	,LruCache = require('../lru.js')
	,Conf = require('../../conf.json')
	,FileStore = require('./file.js')

//TODO 索引从内存获得 body放入存储中
let CacheCan  = new LruCache(100)
	,MemCan = {}


function set(query, buff ,cbk){
	let id = query.id

	MemCan[id] = buff
	CacheCan.set(id ,{ 
		fd : id 
	})
	cbk()
	
	/*
	FileStore.put(id, buff,function(err , pieces){
		if (!err){
			CacheCan.set(id ,{ 
				fd : pieces 
			})
		}
		cbk(err)
	})	
	*/
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
		resolve(MemCan[cache_pieces])
	/*
		FileStore.get(cache_pieces , function(err,body){
			if (err) return reject(err)
			resolve( body)
		})
	*/
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
