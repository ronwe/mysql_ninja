var MAX_PACKET_LENGTH = Math.pow(2, 24) - 1;
var MUL_32BIT         = Math.pow(2, 32);

/*
  (Result Set Header Packet)  the number of columns
  (Field Packets)             column descriptors
  (EOF Packet)                marker: end of Field Packets
  (Row Data Packets)          row contents
  (EOF Packet)                marker: end of Data Packets
*/

module.exports = Parser

function Parser(options) {
	options = options || {}
	this.reset(options)
}

Parser.prototype.reset = function(options){
	this._header = [] 
	this._body = []
	this.header_len = 4
	this._head_set = false 
	this._body_set = false
	this._reset()
}
Parser.prototype._reset = function(){
	this._buffer = Buffer.alloc(0)
	this._offset = 0
}

Parser.prototype._put = function(chunk){
	if (0 === chunk.length) return
	if (this._head_set){
		this._body.push(chunk)
	}else{
		this._header.push(chunk)
	}
}
Parser.prototype.write = function(chunk){
	if (this._body_set) return false
	this._buffer = Buffer.concat([this._buffer,chunk]) 
	this._offset = 0
	this._process()
}
	
Parser.prototype._process = function(){
	let _header_len = this.header_len
	while(true){
		if (this._buffer.length === 0 ){
			break
		}
		let _len = this.parseUnsignedNumber(3)
			,_number = this.parseUnsignedNumber(1)
		if ((_len + _header_len) > this._buffer.length){
			break
		}
		let _piece = this._buffer.slice(0,_len + _header_len) 
			,_first = _piece.readUInt8(4)

		this._put(_piece)
		if (0xfe === _first){
			if (!this._head_set){
				this._head_set = true	
			}else{
				this._body_set = true
			}
		}
		this._buffer = this._buffer.slice(_piece.length )
		
		this._offset = 0
	}
}

Parser.prototype.read = function(){
	return {
		head : this._header
		,body : this._body
		,headed : this._head_set
		,bodyed : this._body_set
	}
}

/*
Parser.prototype.fin = function(){
	this._put(this._buffer)
	this._reset()
	return this.read()
}
*/

Parser.prototype.parseUnsignedNumber = function parseUnsignedNumber(bytes) {
	if (bytes === 1) {
		return this._buffer[this._offset++];
	}

	var buffer = this._buffer;
	var offset = this._offset + bytes - 1;
	var value  = 0;

	if (bytes > 4) {
		var err    = new Error('parseUnsignedNumber: Supports only up to 4 bytes');
		err.offset = (this._offset - this._packetOffset - 1);
		err.code   = 'PARSER_UNSIGNED_TOO_LONG';
		throw err;
	}

	while (offset >= this._offset) {
		value = ((value << 8) | buffer[offset]) >>> 0;
		offset--;
	}

	this._offset += bytes;

	return value;
};


Parser.prototype._combineNextBuffers = function _combineNextBuffers(bytes) {
	var length = this._buffer.length - this._offset;

	if (length >= bytes) {
		return true;
	}

	if ((length + this._nextBuffers.size) < bytes) {
		return false;
	}

	var buffers     = [];
	var bytesNeeded = bytes - length;

	while (bytesNeeded > 0) {
		var buffer = this._nextBuffers.shift();
		buffers.push(buffer);
		bytesNeeded -= buffer.length;
	}

	this.append(buffers);
	return true;
};
