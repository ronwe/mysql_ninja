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
ComQueryPacket.prototype.write = function write(){
	let _sql_buff = Buffer.from(this.sql)
		,_cmd_len = Buffer.byteLength(this.sql)
		,_buff = Buffer.alloc(_cmd_len + 1 + 3)	
	_sql_buff.copy(_buff , 5)

	this._buffer = _buff
	this.writeUnsignedNumber(4, _cmd_len+1)
	this.writeUnsignedNumber(1, this.command)
	return this._buffer 
}
