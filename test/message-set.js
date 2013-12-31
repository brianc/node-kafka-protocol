var Message = require('../lib/message')
var MessageSet = require('../lib/message-set')

describe('message-set-encoding', function() {
  it('works with single empty message', function() {
    var msg1 = new Message(null, null)
    var set = new MessageSet([msg1])
    var actual = set.toBuffer()
    var expected = Buffer([
      0, 0, 0, 1,
      0, 0, 0, 0, 0, 0, 0, 0, //offset, int64, does not matter for outgoing messages
      0, 0, 0, 14, //size of message
      0xa7, 0xec, 0x68, 0x03, // message crc
      0, //message magic byte - always 0
      0, //message attributes - 0 because uncompressed
      0xff, 0xff, 0xff, 0xff, //negative 1 because null key
      0xff, 0xff, 0xff, 0xff, //negative 1 because null value
    ])
    assert.bufferEqual(actual, expected)
  })

  it('works with single message with contents', function() {
    var msg1 = new Message(Buffer([1]), Buffer([2, 3]))
    var set = new MessageSet([msg1])
    var actual = set.toBuffer()
    var expected = Buffer([
      0, 0, 0, 1,
      0, 0, 0, 0, 0, 0, 0, 0, //offset, int64, does not matter for outgoing messages
      0, 0, 0, 17, //size of message
      0xcd, 0x12, 0xaa, 0xfe, // message crc
      0, //message magic byte - always 0
      0, //message attributes - 0 because uncompressed
      0, 0, 0, 1, //negative 1 because null key
      1,
      0, 0, 0, 2, //negative 1 because null value
      2, 3,
    ])
    assert.bufferEqual(actual, expected)
  })
})
