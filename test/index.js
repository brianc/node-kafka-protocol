describe('reading', function() {
})

var Request = function(clientId, apiKey) {
  this.apiKey = apiKey
  this.apiVersion = 0
  this.correlationId = Request._count++;
  this.clientId = clientId
}

Request._count = 0

Request.prototype.headerBuffer = function() {
  var buff = Buffer(8 + Buffer.byteLength(this.clientId, 'utf8'))
  buff.writeUInt16BE(this.apiKey, 0)
  buff.writeUInt16BE(this.apiVersion, 2)
  buff.writeUInt32BE(this.correlationId, 4)
  buff.write(this.clientId, 8)
  console.log(buff)
  return buff
}

var util = require('util')
var MetadataRequest = function(clientId, topicName) {
  Request.call(this, clientId, 3)
  this.topicName = topicName
}

util.inherits(MetadataRequest, Request)

MetadataRequest.prototype.toBuffer = function() {
  var resultBuff = Buffer(this.topicName, 'utf8')
  var headerBuff = this.headerBuffer()

  var lenBuff = Buffer(4)
  lenBuff.writeUInt32BE(headerBuff.length + resultBuff.length)



  return Buffer.concat([lenBuff, headerBuff, resultBuff])
}

var assert = require('assert')
var bufferEqual = require('buffer-equal')
assert.bufferEqual = function(actual, expected) {
  if(!bufferEqual(actual, expected)) {
    console.log('')
    console.log('actual:   ', actual)
    console.log('expected: ', expected)
  }
  assert(bufferEqual(actual, expected))
}

describe('writing', function() {
  describe('metadata-request', function() {
    it('writes correctly', function() {
      var req = new MetadataRequest('!', '1')
      var expected = [
        0, 0, 0, 10, //length
        0, 3, //api key - 3 for meatdata request
        0, 0, //api version - 0 for v0.8
        0, 0, 0, 0, //correlationId
        33, //! - clientId
        0x31, //'1' - the topic name
      ]
      assert.bufferEqual(req.toBuffer(), Buffer(expected))
    })
  })
})
