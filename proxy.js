let Net = require('net')
	,Events = require('events')
	,Stream = require('stream').Stream
	,Util = require('util')

let Analytic = require('./lib/select_analytic.js')
let Cache = require('./lib/select_cache.js') 

let HOST= '127.0.0.1' 

let program = {
	proxyport : 3360,
	hostname : '172.24.0.161',
	port : 3360
}

let PORT = program.proxyport



Net.createServer(function(sock) {
	let _sequence = [] 

    console.log('CONNECTED: ' + sock.remoteAddress +':'+ sock.remotePort);
    let client = new Net.Socket()
	let _reponse_stack  = []  
    console.log("hostname is "+program.hostname + ':' +program.port);
    client.connect(program.port||3306, program.hostname, function() {
        console.log('Connected')
    })


    client.on('close', function() {
        console.log('Connection closed')
        sock.end()
    })

    client.on('error',function(){
        console.log("error")
    })

    client.on('data', function(data) {
		/*
		*第一个字节值    后续字节数  长度值说明
		*0-250            0   第一个字节值即为数据的真实长度
		*  251            0   空数据，数据的真实长度为零
		*  252            2   后续额外2个字节标识了数据的真实长度
		*  253            3   后续额外3个字节标识了数据的真实长度
		*  254            8   后续额外8个字节标识了数据的真实长度
		* ??? 现在是看最后一位是0x00 则结束
		*/
		//console.log('response: ',  data.readUInt8(0),data.length, data.slice(-5));
		//http://mysql.taobao.org/monthly/2018/04/05/	
		//Packet.prototype.isEOF = function() {
		//	  return this.buffer[this.offset] == 0xfe && this.length() < 13;
		//};
        sock.write(data)
		//https://jan.kneschke.de/projects/mysql/mysql-protocol/
		//会有多次response ,得合并data？
		_reponse_stack.push(data)
		let last = data.readUInt8(Buffer.byteLength(data) -1)
		//console.log('response: ',  data.readUInt8(0),data.length, data.slice(10),'|||',data.slice(-5),last)
		if (last === 0x00){
			let _query = _sequence.shift()
			if (_query ){
				Analytic.setCache(_query , _reponse_stack)
			}
			_reponse_stack = []
		}else{
		}
    })
    sock.client = client

    sock.on('data', function(data) {
		//https://jin-yang.github.io/post/mysql-protocol.html
		//https://dev.mysql.com/doc/dev/mysql-server/8.0.0/page_protocol_basic_packets.html
		//_reponse_stack = []
		let _detect = data.readUInt8(4)
		//console.log('on data' ,data.toString())
		if (_detect === 0x03){
			let _sql = data.slice(5).toString() 
			//console.log('sql' ,_sql)

			let _type = _sql.split(' ')[0].toLowerCase()
			// sql中包含注释会导致分析错误
			switch(_type){
				case 'select':
					let _query = Analytic.wrap(_sql)
					let _cache = Cache.get(_query)
					if (_cache){
						console.log('from cache\n', _query,_cache.length  )
						//sock.write( _cache  )
						_cache.forEach(c => {
							sock.write( c  )
						})
						return
					
					}else if (Analytic.isCacheAble(_query)) {
						console.log('waiting cache')
						_sequence.push(_query) 
					}
					break
				case 'update':
				case 'delete':
				case 'insert':
					break
				default:	
					_sequence.length = 0
			}
		}else if ( _detect === 0x02){
			///TODO COM_INIT_DB

		}
        sock.client.write(data)
    })

    sock.on('close', function(data) {
        sock.client.end()
    })

    sock.on('error',function(){
        console.log("error sock")
    })

}).listen(PORT)

console.log('Server listening on ' + HOST +':'+ PORT)
//node proxy.js -h 172.24.0.161 -P 3360 -x 13306
