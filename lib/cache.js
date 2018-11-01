let Lib = require('./lib.js')
	,LruCache = require('./lru.js')

let CacheCan  = new LruCache()
//TODO 索引从内存获得 body放入存储中

function set(query, buff){
	CacheCan.set(query.id ,{ 
		body:buff
	})
}


function get(query){
	if (!query.id) return false
	let _cache = CacheCan.get(query.id)
	if (_cache){
		return _cache.body
	}
	return false
}
exports.get = get 
exports.set  = set 
