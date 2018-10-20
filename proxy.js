let Net = require('net')
	,Events = require('events')
	,Stream = require('stream').Stream
	,Util = require('util')

let Analytic = require('./lib/select_analytic.js')

let HOST= '127.0.0.1' 

let program = {
	proxyport : 13306,
	hostname : '172.24.0.161',
	port : 3360
}

let PORT = program.proxyport

/*
Util.inherits(Connection, Events.EventEmitter);
var PacketParser = require('../node_modules/mysql2/lib/packet_parser.js');
packetParser = new PacketParser(function(p) {
	//handlePacket(p);
	//console.log('parsed',p.sequenceId ,p.buffer)
	console.log('parsed',p)
});
packetParser2 = new PacketParser(function(p) {
	//handlePacket(p);
	//console.log('parsed',p.sequenceId ,p.buffer)
	console.log('parsed2',p)
});
*/

let Cache = require('./lib/select_cache.js') 

Net.createServer(function(sock) {
	let _sequence = [] 

    console.log('CONNECTED: ' + sock.remoteAddress +':'+ sock.remotePort);
    let client = new Net.Socket()
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
		//console.log('response: ',  data);
		///packetParser2.execute(data)
		
        sock.write(data)
		let _query = _sequence.shift()
		if (_query ){
			Analytic.setCache(_query , data)
		}
    })
    sock.client = client

    sock.on('data', function(data) {
		//https://jin-yang.github.io/post/mysql-protocol.html
		//https://dev.mysql.com/doc/dev/mysql-server/8.0.0/page_protocol_basic_packets.html

		if (data.readUInt8(4) === 0x03){
			let _sql = data.slice(5).toString() 
			//console.log('sql' ,_sql)

			let _type = _sql.split(' ')[0].toLowerCase()
			// sql中包含注释会导致分析错误
			switch(_type){
				case 'select':
					let _query = Analytic.wrap(_sql)
					let _cache = Cache.get(_query)
					if (_cache){
						console.log('from cache\n')
						sock.write( _cache  )
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
		}
		///packetParser.execute(data)
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
