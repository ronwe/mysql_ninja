let Lib = require('./lib.js')
	,LruCache = require('./lru.js')
	,Conf = require('../conf.json')
	,fs = require('fs')
	,path = require('path')

//TODO 索引从内存获得 body放入存储中
let CacheCan  = new LruCache(100)

let cache_path = path.resolve(__dirname , Conf.cache_path)

Lib.checkDirectory(cache_path)
function set(query, buff){
	let id = query.id
		,cnt = buff.length
		,pieces = new Array(cnt) 
		,_stop = false

	buff.forEach( (piece,i) => {
		let cache_filename = path.resolve(cache_path , id + '_' + i)
		fs.writeFile(cache_filename , piece , function(err){
			onWrite(err , cache_filename , i)	
		})
	})

	function onWrite(err , piece_filename,i){
		cnt--

		//console.log(i,cnt , err ,_stop)
		if (!err && !_stop){
			pieces[i] = path.relative(cache_path ,piece_filename)	
		}else{
			_stop = true
			return
		}
		if (cnt === 0){
			if (_stop){
			}else{
				CacheCan.set(id ,{ 
					fd : pieces 
				})
				console.log('>>> set cache ' ,id, pieces)
			}
		}
	} 
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
		let cnt = cache_pieces.length
			,body = new Array(cnt) 
			,_fail = false
		cache_pieces.forEach((piece_filename,i) => {
			piece_filename = path.resolve(cache_path , piece_filename)
			fs.readFile(piece_filename , function(err, data){
				onRead(err , data ,i)
			})
		})

		function onRead(err , piece_data ,i){
			cnt--
			if (err){
				_fail = true
			}else{
				body[i] = piece_data 
			}
			if (0 === cnt){
				if (_fail){
					reject('cache read fail')
					//TODO remove cache
				}else{
					resolve(body)	
				}
			}
		}

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
