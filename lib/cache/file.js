let Lib = require('../lib.js')
	,Conf = require('../../conf.json')
	,fs = require('fs')
	,Path = require('path')

let cache_path = Path.resolve(__dirname , Conf.cache_path)
Lib.checkDirectory(cache_path)

function  pubIn(id, buff ,cbk){
	let cnt = buff.length

	let onWrite = thunk(cnt, function(err ,body){
		if (err) return cbk(err)	
		body = body.map(piece_filename => Path.relative(cache_path ,piece_filename) ) 
		cbk(null , body)
	}, 'cache put fail')

	buff.forEach( (piece,i) => {
		let cache_filename = Path.resolve(cache_path , id + '_' + i)
		fs.writeFile(cache_filename , piece , function(err){
			onWrite(err , cache_filename , i)	
		})
	})

}


function readOut(cache_pieces ,cbk){
	if (!cbk) return
	if (!cache_pieces.length) return cbk('param is not array')
	

	let onRead = thunk(cache_pieces.length , cbk ,'cache read fail')
	cache_pieces.forEach((piece_filename,i) => {
		fs.readFile(Path.resolve(cache_path ,piece_filename ), function(err, data){
			onRead(err , data ,i)
		})
	})

}

function remove(cache_pieces ,cbk){
	if (!cache_pieces || !cache_pieces.length) return cbk && cbk('param is not array')
	
	let onDel = thunk(cache_pieces.length , cbk , 'cache del fail')

	cache_pieces.forEach((piece_filename,i) => {
		fs.unlink(Path.resolve(cache_path ,piece_filename ), function(err){
			onDel(err , i)
		})
	})

}

function thunk(cnt ,cbk ,err_msg){
	let _fail = false
		,body = new Array(cnt) 
	return function(err,ret,i){
		cnt--
		if (err){
			_fail = true
		}else{
			body[i] = ret 
		}

		if (0 === cnt){
			if (cbk) cbk(_fail? err_msg || 'cache fail' : null , body)
		}
		
	}
}

exports.remove = remove 
exports.get = readOut
exports.put = pubIn
