let Lib = require('../lib.js')
	,Conf = require('../../conf.json')
	,fs = require('fs')
	,path = require('path')

let cache_path = path.resolve(__dirname , Conf.cache_path)
Lib.checkDirectory(cache_path)

function  pubIn(id, buff ,cbk){
	let cnt = buff.length
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
			cbk && cbk(_stop ? 'put fail' : null , pieces)
			console.log('>>> set cache ' ,_stop , id, pieces)
		}
	} 
}


function readOut(cache_pieces ,cbk){
	if (!cbk) return
	if (!cache_pieces.length) return cbk('param is not array')
	
	let cnt = cache_pieces.length
		,body = new Array(cnt) 
		,_fail = false
	cache_pieces.forEach((piece_filename,i) => {
		fs.readFile(path.resolve(cache_path ,piece_filename ), function(err, data){
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
				cbk('cache read fail')
				//TODO remove cache
			}else{
				cbk(null , body)	
			}
		}
	}

}


exports.get = readOut
exports.put = pubIn
