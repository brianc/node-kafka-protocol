var ProduceRequest = require('../lib/requests').ProduceRequest

describe('ProduceRequest', function() {
  beforeEach(function() {
    require('../lib/requests').Request._count = 7
  })

  it('writes no messages', function() {
    var req = new ProduceRequest('!')
    req.requiredAcks = 4
    req.timeout = 5
    var actual = req.toBuffer()
    var expected = Buffer([
      0, 0, 0, 23, //length
      0, 0, //api key - 0 for produce request
      0, 0, //api version - 0 for v0.8
      0, 0, 0, 7, //correlation id
      0, 1, //clientid string length
      33, //! - clientId
      0, 4, //requiredAcks
      0, 0, 0, 5, //timeout
      0, 0 //empty topics array
    ])
    assert.bufferEqual(actual, expected)
  })

  it('writes for single topic, single message', function() {
    var req = new ProduceRequest('!')
    var topic = '!!'
    var partition = 1
    var key = '!!!'
    var value = '!!!!'
    req.addMessage(topic, partition, key, value)
    req.requiredAcks = 4
    req.timeout = 5
    var actual = req.toBuffer()
    var expected = Buffer([
      0, 0, 0, 23, //length
      0, 0, //api key - 0 for produce request
      0, 0, //api version - 0 for v0.8
      0, 0, 0, 7, //correlation id
      0, 1, //clientid string length
      33, //! - clientId
      0, 4, //requiredAcks
      0, 0, 0, 5, //timeout
      0, 1, //topics array length
      0, 2, //topicName string length
      33, 33, //topicName - "!!"
      0, 1, //partition array length
      0, 0, 0, 1, //partitionId
    ])
    assert.bufferEqual(actual, expected)
  })
})
