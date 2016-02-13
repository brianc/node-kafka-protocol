import expect from 'expect.js'
import ProduceRequest, { group } from '../lib/request/produce-request'
import { dump } from '../example/send'

describe('writing', function() {
  describe('produce-request', function() {
    it('writes an empty message set', () => {
      var req = new ProduceRequest()
      req.requiredAcks = 0
      req.timeout = 0

      const expected = [
        0x00, 0x00, 0x00, 20, //length of the rest of the message, not including these bytes
        0x00, 0x00, //api key - 0 for produce request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //lenght of clientId string (-1 = null)
        0x00, 0x00, //requiredAcks
        0x00, 0x00, 0x00, 0x00, //timeout
        0x00, 0x00, 0x00, 0x00, //topics array length
      ]

      expect(req.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes a single message set', () => {
      var req = new ProduceRequest()
      req.requiredAcks = 0
      req.timeout = 0

      req.add('!!', 3, new Buffer([8]), new Buffer([9]))

      const expected = [
        0x00, 0x00, 0x00, 0x40, //length of the rest of the message, not including these bytes
        0x00, 0x00, //api key - 0 for produce request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //lenght of clientId string (-1 = null)
        0x00, 0x00, //requiredAcks
        0x00, 0x00, 0x00, 0x00, //timeout
        0x00, 0x00, 0x00, 0x01, //topics array length
        0x00, 0x02, //topic name string length
        33, 33, //topic name (!!)
        0x00, 0x00, 0x00, 0x01, //partition array length
        0x00, 0x00, 0x00, 0x03, //partition number
        0x00, 0x00, 0x00, 28, //message set size

        //start of message set
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 16, //size

        //start of message
        81, 54, 198, 83, //crc32
        0, //MagicByte
        0, //Attributes
        0, 0, 0, 1, //key length
        8, //'!' - key
        0, 0, 0, 1, //value length
        9, //value (!!)
      ]
      expect(req.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes a multi-message set', () => {
      var req = new ProduceRequest()
      req.requiredAcks = 0
      req.timeout = 0

      req.add('!!', 3, new Buffer([8]), new Buffer([9]))
      req.add('!!', 3, new Buffer([8]), new Buffer([9]))

      const expected = [
        0x00, 0x00, 0x00, 92, //length of the rest of the message, not including these bytes (64)
        0x00, 0x00, //api key - 0 for produce request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //lenght of clientId string (-1 = null)
        0x00, 0x00, //requiredAcks
        0x00, 0x00, 0x00, 0x00, //timeout
        0x00, 0x00, 0x00, 0x01, //topics array length
        0x00, 0x02, //topic name string length
        33, 33, //topic name (!!)
        0x00, 0x00, 0x00, 0x01, //partition array length
        0x00, 0x00, 0x00, 0x03, //partition number
        0x00, 0x00, 0x00, 56, //message set size

        //start of message set
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 16, //size
        81, 54, 198, 83, //crc32
        0, //MagicByte
        0, //Attributes
        0, 0, 0, 1, //key length
        8, //'!' - key
        0, 0, 0, 1, //value length
        9, //value (!!)

        //second message
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 16, //size
        81, 54, 198, 83, //crc32
        0, //MagicByte
        0, //Attributes
        0, 0, 0, 1, //key length
        8, //'!' - key
        0, 0, 0, 1, //value length
        9, //value (!!)
      ]
      expect(req.toBuffer()).to.eql(Buffer(expected))
    })
  })
})
