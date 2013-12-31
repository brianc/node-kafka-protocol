var requests = require('../lib/requests')
var Message = requests.Message

describe('message encoding', function() {
  it('works on empty message', function() {
    var req = new Message(null, null)
    var expected = [
      0xa7, 0xec, 0x68, 0x03, //TODO: what is crc?
      0, //magic byte - always 0
      0, //attributes - 0 because uncompressed
      0xff, 0xff, 0xff, 0xff, //negative 1 because null key
      0xff, 0xff, 0xff, 0xff, //negative 1 because null value
    ]
    assert.bufferEqual(req.toBuffer(), Buffer(expected))
  })

  it('empty message has correct length', function() {
    assert.equal(new Message(null, null).getLength(), 14)
  })

  it('works on 0 length key and value', function() {
    var req = new Message(Buffer(0), Buffer(0))
    var expected = [
      0xe3, 0x8a, 0x68, 0x76, //TODO: what is crc?
      0, //magic byte - always 0
      0, //attributes - 0 because uncompressed
      0, 0, 0, 0, //0 because key has 0 length
      0, 0, 0, 0, //0 because value has 0 length
    ]
    assert.bufferEqual(req.toBuffer(), Buffer(expected))
  })

  it('works on message with buffer contents', function() {
    var key = Buffer([1,2])
    var value = Buffer([3,4,5])
    var req = new Message(key, value)
    var expected = [
      0x82, 0x8d, 0xe0, 0xd8, //TODO: what is crc?
      0, //magic byte - always 0
      0, //attributes - 0 because uncompressed
      0, 0, 0, 2, //2 bytes of key
      1, 2,
      0, 0, 0, 3, //3 bytes of value
      3, 4, 5,
    ]
    assert.bufferEqual(req.toBuffer(), Buffer(expected))
  })

  it('has correct length with buffer with contents', function() {
    var key = Buffer([1,2])
    var value = Buffer([3,4,5])
    var req = new Message(key, value)
    assert.equal(req.getLength(), 19)
  })
})
