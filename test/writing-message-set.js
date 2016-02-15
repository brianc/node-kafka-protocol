import expect from 'expect.js'
import MessageSet from '../lib/message-set'

describe('message-set', () => {

  describe('writing', () => {
    it('writes empty', () => {
      const set = new MessageSet()
      const expected = [];
      expect(set.toBuffer()).to.eql(Buffer(expected));
    })

    it('writes single message', () => {
      const set = new MessageSet()
      set.add(Buffer('!', 'utf8'), Buffer('!!', 'utf8'))
      const expected = [
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 17, //size
        105, 102, 165, 231, //crc32
        0, //MagicByte
        0, //Attributes
        0, 0, 0, 1, //key length
        33, //'!' - key
        0, 0, 0, 2, //value length
        33, 33, //value (!!)
      ];
      expect(set.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes a message with a null key and value', () => {
      const set = new MessageSet()
      set.add()
      const expected = [
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 14, //size
        167, 236, 104, 3, //crc32
        0, //MagicByte
        0, //Attributes
        255, 255, 255, 255, //key length (null)
        255, 255, 255, 255, //value length (null)
      ];
      expect(set.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes compound message set', () => {
      const set = new MessageSet()
      set.add()
      set.add(Buffer('!', 'utf8'), Buffer('!!', 'utf8'))
      set.add(null, Buffer('!!', 'utf8'))

      const expected = [
        //first, empty message
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 14, //size
        167, 236, 104, 3, //crc32
        0, //MagicByte
        0, //Attributes
        255, 255, 255, 255, //key length (null)
        255, 255, 255, 255, //value length (null)

        //second, hydrated message
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 17, //size
        105, 102, 165, 231, //crc32
        0, //MagicByte
        0, //Attributes
        0, 0, 0, 1, //key length
        33, //'!' - key
        0, 0, 0, 2, //value length
        33, 33, //value (!!)

        //third, keyless message
        0, 0, 0, 0, 0, 0, 0, 0, //offset
        0, 0, 0, 16, //size
        164, 210, 128, 53, //crc32
        0, //MagicByte
        0, //Attributes
        255, 255, 255, 255, //key length (null)
        0, 0, 0, 2, //value length
        33, 33, //value (!!)
      ];
      expect(set.toBuffer()).to.eql(Buffer(expected))
    })
  })
})
