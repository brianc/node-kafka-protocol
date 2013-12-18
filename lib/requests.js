var Request = function(clientId, apiKey) {
  this.apiKey = apiKey
  this.apiVersion = 0
  this.correlationId = Request._count++;
  this.clientId = clientId
}

Request._count = 0

var strBuff = function(str) {
  //null string is just - 1
  if(!str) {
    var buff = Buffer(2)
    buff.writeInt16BE(-1)
    return buff
  }
  var len = Buffer.byteLength(str)
  var buff = Buffer(len + 2)
  buff.writeInt16BE(len, 0)
  buff.write(str, 2)
  return buff
}

Request.prototype.headerBuffer = function() {
  var buff = Buffer(8)
  buff.writeInt16BE(this.apiKey, 0)
  buff.writeInt16BE(this.apiVersion, 2)
  buff.writeInt32BE(this.correlationId, 4)
  return Buffer.concat([buff, strBuff(this.clientId)])
}

var util = require('util')
var MetadataRequest = function(clientId, topicNames) {
  Request.call(this, clientId, 3)
  this.topicNames = topicNames
}

util.inherits(MetadataRequest, Request)

MetadataRequest.prototype.toBuffer = function() {
  var buffs = []
  for(var i = 0; i < this.topicNames.length; i++) {
    var topicName = this.topicNames[i]
    var stf = strBuff(topicName)
    console.log('strbuff', stf)
    buffs.push(stf)
  }
  var toConcat = [Buffer([0,0, 0, 0])].concat(buffs)
  console.log(toConcat)
  var resultBuff = Buffer.concat(toConcat)
  resultBuff.writeInt32BE(buffs.length, 0)
  console.log('resultBuff', resultBuff)
  var headerBuff = this.headerBuffer()

  var lenBuff = Buffer(4)
  var totalLen = headerBuff.length + resultBuff.length;
  console.log('length', totalLen)
  lenBuff.writeInt32BE(totalLen, 0)
  return Buffer.concat([lenBuff, headerBuff, resultBuff])
}

module.exports = {
  Request: Request,
  MetadataRequest: MetadataRequest,
}

