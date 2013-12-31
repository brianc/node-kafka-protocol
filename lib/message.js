var crc32 = require('buffer-crc32')
var writeBytes = require('./util').writeBytes

var Message = module.exports = function(key, value) {
  this.magicByte = 0
  this.attributes = 0
  this.key = key
  this.value = value
  this._length = null
}

Message.prototype.toBuffer = function() {
  var buffer = Buffer(this.getLength())
  this.writeBuffer(buffer, 0)
  return buffer
}

Message.prototype.writeBuffer = function(buffer, offset) {
  var start = offset
  offset += 4
  buffer[offset++] = 0 //leave magic byte to 0 (offset 4)
  buffer[offset++] = 0 //leave attributes to 0 (offset 5)

  offset = writeBytes(buffer, offset, this.key)
  writeBytes(buffer, offset, this.value)

  var crc = crc32(buffer.slice(start + 4))
  buffer[start++] = crc[0]
  buffer[start++] = crc[1]
  buffer[start++] = crc[2]
  buffer[start++] = crc[3]
  return offset
}

Message.prototype.getLength = function() {
  //header byte length
  var byteLength = 4 + 1 + 1;

  //append 4 bytes for int32 key and value lengths
  byteLength += 4 + 4

  //append byte length of key and value
  if(this.key !== null) {
    byteLength += this.key.length
  }
  if(this.value !== null) {
    byteLength += this.value.length
  }
  return byteLength
}

