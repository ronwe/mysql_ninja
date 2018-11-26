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

/*
Field Packet https://blog.csdn.net/caisini_vc/article/details/5356136	
n practice, since identifiers are almost always 250 bytes or shorter, the Length Coded Strings look like: (1 byte for length of data) (data)
--------------------- 
VERSION 4.1
				    Hexadecimal                ASCII
                    -----------                -----
catalog             03 73 74 64                .std
db                  03 64 62 31                .db1
table               02 54 37                   .T7
org_table           02 74 37                   .t7
name                02 53 31                   .S1
org_name            02 73 31                   .s1
(filler)            0c                         .
charsetnr           08 00                      ..
length              01 00 00 00                ....
type                fe                         .
flags               00 00                      ..
decimals            00                         .
(filler)            00 00                      ..
In the example, we see what the server returns for "SELECT s1 AS S1 FROM t7 AS T7" where column s1 is defined as CHAR(1).

VERSION 4.0
 Bytes                      Name
 -----                      ----
 n (Length Coded String)    table
 n (Length Coded String)    name
 4 (Length Coded Binary)    length
 2 (Length Coded Binary)    type
 2 (Length Coded Binary)    flags
 1                          decimals
 n (Length Coded Binary)    default
*/
function parseColDef(buff){
	let _stack = _parseLine(buff)
	if (!_stack) return false
	if (_stack[0] === 'def'){
		//protocol41
		return {
			'db' : _stack[1],
			'table' : _stack[2],
			'table_full' : _stack[3],
			'field' : _stack[4],
			'field_full' : _stack[5]
		}	
	}else{
		return {
			'table' : _stack[0],
			'field' : _stack[1],
		}	
	}
}
/*
Bytes                   Name
 -----                   ----
 n (Length Coded String) (column value)
*/
function parseColVal(buff){
	let _stack = _parseLine(buff)
	return _stack
}

function _parseLine(buff){
	let _stack = []
	for(let i=0,j=buff.length ; i < j ; i++){
		let _len = buff[i].toString() * 1
		let _content = buff.slice(i+1, i+ 1 + _len  )
		_stack.push(_content.toString())
		i += _len  
	}
	if (_stack.length<2) return false
	return _stack
}


function Parser(options) {
	options = options || {}
	this.reset(options)
}

Parser.prototype.reset = function(options){
	this._header = [] 
	this._body = []
	this._parsed_columns = []
	this._parsed_col_vals = []
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
Parser.prototype.write = function(chunk ,to_parse){
	if (this._body_set) return false
	this._buffer = Buffer.concat([this._buffer,chunk]) 
	this._offset = 0
	this._process(to_parse)
}

Parser.prototype._process = function(to_parse){
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
				this._parsed_col_vals = []
				for (let _m = 0,_n = this._parsed_columns.length; _m < _n ; _m++){
					this._parsed_col_vals.push([])
				}
			
			}else{
				this._body_set = true
				//console.log('this._parsed_columns' ,this._parsed_columns)
				//console.log('val' , this._parsed_col_vals)
			}
		}else if(to_parse){
			if (!this._head_set){
				let _column = parseColDef(_piece.slice(_header_len))
				if (_column){
					this._parsed_columns.push(_column.field)
				}
			}else{
				let _col_val = parseColVal(_piece.slice(_header_len))
				for(let _m=0,_n=_col_val.length; _m < _n;_m++){
					this._parsed_col_vals[_m].push(_col_val[_m])	
				} 
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
		,columns : this._parsed_columns
		,columns_vals : this._parsed_col_vals
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
