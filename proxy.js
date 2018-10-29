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
const _Debug = true

function Print(...msg){
	if (true !== _Debug) return
	console.log.apply(console,msg)
}
function SysPrint(msg){
	console.log(msg)
}


Net.createServer(function(sock) {
	let _sequence = [] 
		,_default_db
		,_handshaked = false
    	,client = new Net.Socket()
		,_reponse_stack  = []  

    SysPrint('CONNECTED: ' + sock.remoteAddress +':'+ sock.remotePort);
	
    SysPrint("hostname is "+program.hostname + ':' +program.port);
    client.connect(program.port||3306, program.hostname, function() {
        SysPrint('Connected')
    })


	function parseHandShake(buff){

		let _check_pos = buff.indexOf(Buffer.alloc(23,0x00))
			,protocol41 = false
		if (_check_pos > 0){
			//protocol41
			buff =  buff.slice(_check_pos + 23)
			protocol41 = true
		}
		//find user field end
		_check_pos = buff.indexOf(0x00)
		buff =  buff.slice(_check_pos + 1)
		if (protocol41){
			buff =  buff.slice(21)	//20 + 1
		}else{
			buff =  buff.slice(10) // 8 + 1 + 1	
		}
		_check_pos = buff.indexOf(0x00)
		

		let default_db = buff.slice(0, _check_pos).toString()
		return {
			default_db : default_db
		}
	}

	function handShakeInited(){
		let _query = _sequence.shift()
		//init _default_db

		if (!_query){
			Print('connnet not init')
			client.emit('close')
		}
		let _parsed_handshake = parseHandShake(_query)
		if (_parsed_handshake){
			_default_db  = _parsed_handshake.default_db
			console.log('_default_db' ,_default_db)
		}

	}
	function processResponse(to_process){
		let _query = _sequence.shift()
		if (true === to_process && _reponse_stack.length){
			Print('_query' , _query , _reponse_stack.length)
			if (_query && _query.should_cache ){
				Analytic.setCache(_query , _reponse_stack)
			}
		}
		_reponse_stack = []
	}

    client.on('close', function() {
        SysPrint('Connection closed')
        sock.end()
    })

    client.on('error',function(){
        SysPrint("error")
    })

    client.on('data', function(data) {
		/*
		*OK Packet                    00
		*EOF Packet                   fe
		*Error Packet                 ff
		*Result Set Packet            1-250 (first byte of Length-Coded Binary)
		*Field Packet                 1-250 ("")
		*Row Data Packet              1-250 ("")
		* or  现在是看最后一位是0x00 则结束
		* 造个错误的sql
		*/
		//https://zhuanlan.zhihu.com/p/24661533
		//https://jan.kneschke.de/projects/mysql/mysql-protocol/
		//http://mysql.taobao.org/monthly/2018/04/05/	

        sock.write(data)
		let last = data.readUInt8(Buffer.byteLength(data) -1)
			,first = data.readUInt8(4)

		Print('response',first , last ,data.length,data)
		/*
		if ((first >= 1 && first <= 250) || first === 0xfe){
		}else 
		*/
		if (first === 0xff){
			//error packet 可能是错误的sql ，可能是字段还没添加 所以不缓存 
			processResponse(false)
		}else if (first === 0x00){
			if (!_handshaked){
				_handshaked = true
				handShakeInited()
			}else{
				processResponse(false)
			}
		}else {
			_reponse_stack.push(data)
			if (last === 0x00 ){
				processResponse(true)
			}
		}
    })
    sock.client = client

    sock.on('data', function(data) {
		//握手协议
		//https://dev.mysql.com/doc/dev/mysql-server/8.0.11/page_protocol_connection_phase_packets_protocol_handshake_v10.html
		//https://jin-yang.github.io/post/mysql-protocol.html
		//https://dev.mysql.com/doc/dev/mysql-server/8.0.0/page_protocol_basic_packets.html
		//_reponse_stack = []
		let _detect = data.readUInt8(4)
			,_query
		Print('on data' ,data.toString())
		if (!_handshaked){
			_query = data
		}
		//com标识 这个比较重要
		//https://dev.mysql.com/doc/internals/en/text-protocol.html
		if (_detect === 0x03){
			let _sql = data.slice(5).toString() 
			//Print('sql' ,_sql)

			let _type = _sql.split(' ')[0].toLowerCase()
			// sql中包含注释会导致分析错误
			_query = Analytic.wrap(_sql,_default_db)
			switch(_type){
				case 'select':
					let _cache = Cache.get(_query)
					if (_cache){
						Print('from cache\n', _query,_cache.length  )
						//Print(_cache)
						//sock.write( _cache  )
						_cache.forEach(c => {
							sock.write( c  )
						})
						return
					
					}else if (Analytic.isCacheAble(_query)) {
						Print('waiting cache')
						_query.should_cache = true
					}
					break
				case 'update':
					break
				case 'delete':
				case 'insert':
					break
				case 'use':
					_default_db = Analytic.getUseDB(_sql)
					console.log('_default_db' ,_default_db)
					break
				default:	
					_sequence.length = 0
			}
		}else if ( _detect === 0x02){
			///TODO COM_INIT_DB
		}

		_sequence.push(_query || {}) 

        sock.client.write(data)
    })

    sock.on('close', function(data) {
        sock.client.end()
    })

    sock.on('error',function(){
        Print("error sock")
    })

}).listen(PORT)
/*
0x00   COM_SLEEP           (none, this is an internal thread state)
0x01   COM_QUIT            mysql_close
0x02   COM_INIT_DB         mysql_select_db 
0x03   COM_QUERY           mysql_real_query
0x04   COM_FIELD_LIST      mysql_list_fields
0x05   COM_CREATE_DB       mysql_create_db (deprecated)
0x06   COM_DROP_DB         mysql_drop_db (deprecated)
0x07   COM_REFRESH         mysql_refresh
0x08   COM_SHUTDOWN        mysql_shutdown
0x09   COM_STATISTICS      mysql_stat
0x0a   COM_PROCESS_INFO    mysql_list_processes
0x0b   COM_CONNECT         (none, this is an internal thread state)
0x0c   COM_PROCESS_KILL    mysql_kill
0x0d   COM_DEBUG           mysql_dump_debug_info
0x0e   COM_PING            mysql_ping
0x0f   COM_TIME            (none, this is an internal thread state)
0x10   COM_DELAYED_INSERT  (none, this is an internal thread state)
0x11   COM_CHANGE_USER     mysql_change_user
0x12   COM_BINLOG_DUMP     sent by the slave IO thread to request a binlog
0x13   COM_TABLE_DUMP      LOAD TABLE ... FROM MASTER (deprecated)
0x14   COM_CONNECT_OUT     (none, this is an internal thread state)
0x15   COM_REGISTER_SLAVE  sent by the slave to register with the master (optional)
0x16   COM_STMT_PREPARE    mysql_stmt_prepare
0x17   COM_STMT_EXECUTE    mysql_stmt_execute
0x18   COM_STMT_SEND_LONG_DATA mysql_stmt_send_long_data
0x19   COM_STMT_CLOSE      mysql_stmt_close
0x1a   COM_STMT_RESET      mysql_stmt_reset
0x1b   COM_SET_OPTION      mysql_set_server_option
0x1c   COM_STMT_FETCH      mysql_stmt_fetch
*/

SysPrint('Server listening on ' + HOST +':'+ PORT)
//node proxy.js -h 172.24.0.161 -P 3360 -x 13306
