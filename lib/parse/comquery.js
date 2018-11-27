module.exports = ComQueryPacket;
function ComQueryPacket(sql) {
	this.command = 0x03
	this.sql     = sql
	this._offset = 0
}

ComQueryPacket.prototype.writeUnsignedNumber = function writeUnsignedNumber(bytes, value) {
	for (var i = 0; i < bytes; i++) {
		this._buffer[this._offset++] = (value >> (i * 8)) & 0xff
	}
}

/*
TODO 包有最大长度
var MAX_PACKET_LENGTH = Math.pow(2, 24) - 1;

for (var packet = 0; packet < packets; packet++) {
   var isLast = (packet + 1 === packets);
   var packetLength = (isLast)
   ? length % MAX_PACKET_LENGTH
   : MAX_PACKET_LENGTH;

   var packetNumber = parser.incrementPacketNumber();

   this.writeUnsignedNumber(3, packetLength);
   this.writeUnsignedNumber(1, packetNumber);

   var start = packet * MAX_PACKET_LENGTH;
   var end   = start + packetLength;

   this.writeBuffer(buffer.slice(start, end));
}
*/
ComQueryPacket.prototype.write = function write(){
	let _sql_buff = Buffer.from(this.sql)
		,_cmd_len = Buffer.byteLength(this.sql)
		,_buff = Buffer.alloc(_cmd_len + 1 + 3 + 1)	

	_sql_buff.copy(_buff , 5)

	this._buffer = _buff
	this.writeUnsignedNumber(4, _cmd_len+1)
	this.writeUnsignedNumber(1, this.command)


	
	return this._buffer 
}
