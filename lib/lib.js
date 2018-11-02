let crypto = require('crypto')
	,fs = require('fs')

function checkDirectory(directory, callback) {  
	fs.stat(directory, function(err, stats) {
		//Check if error defined and the error code is "not exists"
		if (err && err.errno === -2) {
			fs.mkdir(directory, { recursive: true } ,callback || function(){})
		} else {
			callback && callback(err)
		}
	})
}

function md5(str) {
	var md5sum = crypto.createHash('md5')
	md5sum.update(str)
	str = md5sum.digest('hex')
	return str
}

exports.checkDirectory = checkDirectory
exports.md5 = md5
