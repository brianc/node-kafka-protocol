var Reader = require('../lib/reader')

describe('metdata response', function() {
  before(function() {
    this.reader = new Reader()
  })

  describe('empty response', function() {
    it('works', function() {

      this.reader.addChunk(Buffer([
        '00', '00', '00', 0x0c, //length
        '00', '00', '00', '09', //correlationId
        '00', '00', '00', '00', //broker array length?
        '00', '00', '00', '00', //TopicMetadata array length?
      ]))

      var packet = this.reader.read()
      assert(packet, 'Should have returned a packet')
      assert.equal(packet.correlationId, 9)
      assert.equal(packet.buffer.length, 8)
    })
  })

  describe('simple, error code response', function() {
    var res = Buffer([
                     0x00, 0x00, 0x00, 0x1c, //length
                     0x00, 0x00, 0x00, 0x09, //correlationId
                     0x00, 0x00, 0x00, 0x00, //broker array length
                     0x00, 0x00, 0x00, 0x01, //TopicMetadata array length
                     0xff, 0xff, //TopicErrorCode
                     0x00, 0x08, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x21, //topicName
                     0x00, 0x00, 0x00, 0x00]) //partitionMetadata array (0 length)

    assert.equal(res.readInt16BE(16), -1, 'should have -1 for invalid request error code')
    console.log(res.readInt16BE(16))

  })
})
