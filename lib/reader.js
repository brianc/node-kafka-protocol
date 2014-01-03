var Reader = require('packet-reader')

var Packet = function(buffer) {
  this.offset = 0
  this.buffer = buffer
  this.correlationId = this.readInt32BE()
}

Packet.prototype.readInt32BE = function() {
  var result = this.buffer.readInt32BE(this.offset)
  this.offset += 4
  return result
}

Packet.prototype.readInt16BE = function() {
  var result = this.buffer.readInt16BE(this.offset)
  this.offset += 2
  return result
}

Packet.prototype.readString = function() {
  var len = this.readInt16BE()
  return this.buffer.toString('utf8', this.offset, this.offset += len)
}

Packet.prototype.readInt32BEArray = function() {
  return this.mapArray(function(packet) {
    return packet.readInt32BE()
  })
}

Packet.prototype.mapArray = function(mapper) {
  var len = this.readInt32BE()
  var result = []
  for(var i = 0; i < len; i++) {
    result.push(mapper(this))
  }
  return result
}

var R = module.exports = function() {
  this._reader = new Reader();
}

R.prototype.addChunk = function(buff) {
  this._reader.addChunk(buff)
}

R.prototype.read = function() {
  var res = this._reader.read()
  if(!res) return res;
  return new Packet(res)
}
