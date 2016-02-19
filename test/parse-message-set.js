import expect from 'expect.js'
import MessageSet from '../lib/message-set'
import Reader from '../lib/reader'

describe('parse', function() {
  describe('message-set', () => {

    it('parses totally empty message set', async () => {
      const buff = Buffer([0, 0, 0, 0])
      const set = new MessageSet(0).read(new Reader(buff))
      expect(set.messages).to.have.length(0)
    })

    it('parses single message', () => {
      const buff = Buffer([
        0, 0, 0, 0, 0, 0, 0, 8, //offset int64 - (8)
        0, 0, 0, 20, //message size int32 - (20)
        0, 0, 0, 1, //crc32 int32 - (1)
        0, //magicbyte
        0, //attributes
        0, 0, 0, 2, //keyLength int32 - (2)
        0x02, 0x03, //key bytes
        0, 0, 0, 4, //bodyLength int32 - (4)
        0x05, 0x06, 0x07, 0x08
      ])
      const set = new MessageSet(32).read(new Reader(buff))
      expect(set).to.eql({
        size: 32,
        messages: [{
          offset: 8,
          size: 20,
          crc: 1,
          magicByte: 0,
          attributes: 0,
          key: Buffer([2, 3]),
          value: Buffer([5, 6, 7, 8])
        }]
      })
    })
  })
})
