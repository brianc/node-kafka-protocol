var requests = require('../lib/requests')
var MetadataRequest = requests.MetadataRequest

describe('writing', function() {
  describe('metadata-request', function() {
    it('writes correctly', function() {
      var req = new MetadataRequest('!', ['11'])
      var expected = [
        0, 0, 0, 19, //length of the rest of the message, not including these bytes
        0, 3, //api key - 3 for meatdata request
        0, 0, //api version - 0 for v0.8
        0, 0, 0, 0, //correlationId
        0, 1, //lenght of clientId string
        33, //! - clientId
        0, 0, 0, 1, //length of topic array
        0, 2, //length of topic name string
        0x31, 0x31, //'1' - the topic name
      ]
      assert.bufferEqual(req.toBuffer(), Buffer(expected))
    })
  })
})
