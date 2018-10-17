let Lib = require('./lib.js')
let CacheCan  = {}

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
