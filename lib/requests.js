var Request = function(clientId, apiKey) {
  this.apiKey = apiKey
  this.apiVersion = 0
  this.correlationId = Request._count++;
  this.clientId = clientId
}

Request.produce = 0
Request.fetch = 1
Request.offset = 2
Request.metadata = 3

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
  Request.call(this, clientId, Request.metadata)
  this.topicNames = topicNames
}

util.inherits(MetadataRequest, Request)

MetadataRequest.prototype.toBuffer = function() {
  var buffs = []
  for(var i = 0; i < this.topicNames.length; i++) {
    var topicName = this.topicNames[i]
    var stf = strBuff(topicName)
    buffs.push(stf)
  }
  var toConcat = [Buffer([0,0, 0, 0])].concat(buffs)
  var resultBuff = Buffer.concat(toConcat)
  resultBuff.writeInt32BE(buffs.length, 0)
  var headerBuff = this.headerBuffer()

  var lenBuff = Buffer(4)
  var totalLen = headerBuff.length + resultBuff.length;
  lenBuff.writeInt32BE(totalLen, 0)
  return Buffer.concat([lenBuff, headerBuff, resultBuff])
}

var ProduceMessage = function(topic, partition, key, value) {
  this.topic = topic
  this.partition = partition
  this.key = key
  this.value = value
}

var ProduceRequest = function(clientId) {
  Request.call(this, clientId, Request.produce)
  this.requiredAcks = 0 //int16
  this.timeout = 0 //int32
  this.messages = []
}

util.inherits(ProduceRequest, Request)

ProduceRequest.prototype.addMessage = function(topic, partition, key, value) {
  this.messages.push(new ProduceMessage(topic, partition, key, value))
}

var writeArray = function(array, mapper) {
  var lenBuff = Buffer(2)
  var buffs = [lenBuff]
  lenBuff.writeInt16BE(array.length, 0)
  for(var i = 0; i < array.length; i++) {
    buffs.push(mapper(array[i]))
  }
  return Buffer.concat(buffs)
}

var writeMessage = function(parcel) {
  return Buffer([0, 1, 2, 3])
}

ProduceRequest.prototype.toBuffer = function() {
  var headerBuff = this.headerBuffer()
  var lenBuff = Buffer(4)
  var resultBuff = Buffer(6)
  resultBuff.writeInt16BE(this.requiredAcks, 0)
  resultBuff.writeInt32BE(this.timeout, 2)
  var messageBuff = writeArray(this.messages, writeMessage)
  lenBuff.writeInt32BE(4 + headerBuff.length + resultBuff.length + messageBuff.length, 0)
  return Buffer.concat([lenBuff, headerBuff, resultBuff, messageBuff])
}

module.exports = {
  Request: Request,
  MetadataRequest: MetadataRequest,
  ProduceRequest: ProduceRequest,
  Message: require('./message'),
}

