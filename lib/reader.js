var Reader = require('packet-reader')

var ResponseMessage = function(correlationId, buffer) {
  this.correlationId = correlationId
  this.buffer = buffer
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
  return new ResponseMessage(res.readUInt32BE(0), res.slice(4))
}
