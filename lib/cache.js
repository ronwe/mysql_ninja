let Lib = require('./lib.js')
let CacheCan  = {}
//TODO 索引从内存获得 body放入存储中

function set(query, buff){
	CacheCan[query.id] = {
		body:buff
	}
}


function get(query){
	if (!query.id) return false
	let _cache = CacheCan[query.id]
	if (_cache){
		return _cache.body
	}
	return false
}
exports.get = get 
exports.set  = set 
