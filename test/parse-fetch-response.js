import expect from 'expect.js'
import parse from './parse'
import { dump } from '../example/send'

describe('parse', function() {
  describe('fetch-response', () => {
    it('parses empty response', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 0x08, //length (32)
        0x00, 0x00, 0x00, 0x03, //correlationId (0)
        0x00, 0x00, 0x00, 0x00, //length of topic array (1)
      ])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(3)

      const res = msg.readFetchResponse()
      expect(res.topics).to.have.length(0)
    })

    it('parses topics with empty partitions', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 0x17, //length (32)
        0x00, 0x00, 0x00, 0x07, //correlationId (7)
        0x00, 0x00, 0x00, 0x02, //length of topic array (2)

        //first topic name length (1)
        0x00, 0x01,
        0x21, //topic name (!)
        0x00, 0x00, 0x00, 0x00, //length of partition array

        //second topic name length (1)
        0x00, 0x02,
        0x21, 0x21, //topic name (!!)
        0x00, 0x00, 0x00, 0x00, //length of partition array
      ])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(7)

      const res = msg.readFetchResponse()
      expect(res.topics).to.have.length(2)
    })

    it('parses topics with partitions', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 51, //length (53)
        0x00, 0x00, 0x00, 0x09, //correlationId (7)
        0x00, 0x00, 0x00, 0x01, //length of topic array (1)

        //first topic name length (1)
        0x00, 0x01,
        0x21, //topic name (!)
        0x00, 0x00, 0x00, 0x02, //length of partition array

          //partition 0
          0x00, 0x00, 0x00, 0x00, //partition id (0)
          0x00, 0x01, //errorCode (1)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, //highwatermark offset (1) - string
          0x00, 0x00, 0x00, 0x00, //messageset size (0 bytes)

          //partition 1
          0x00, 0x00, 0x00, 0x01, //partition id (1)
          0x00, 0x00, //errorCode (0)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, //highwatermark offset (9) - string
          0x00, 0x00, 0x00, 0x00, //messageset size (0 bytes)
      ])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(9)

      const res = msg.readFetchResponse()
      expect(res.topics).to.have.length(1)

      const [topic] = res.topics
      expect(topic.partitions).to.have.length(2)

      const [part1, part2] = topic.partitions
      expect(part1).to.eql({
        id: 0,
        errorCode: 1,
        highwaterMarkOffset: '2',
        messageSet: {
          size: 0,
          messages: []
        }
      })

      expect(part2).to.eql({
        id: 1,
        errorCode: 0,
        highwaterMarkOffset: '9',
        messageSet: {
          size: 0,
          messages: []
        }
      })
    })

    it('parses message-set', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 65, //length (65)
        0x00, 0x00, 0x00, 0x09, //correlationId (7)
        0x00, 0x00, 0x00, 0x01, //length of topic array (1)

        //first topic name length (1)
        0x00, 0x01,
        0x21, //topic name (!)
        0x00, 0x00, 0x00, 0x01, //length of partition array

          //partition 0
          0x00, 0x00, 0x00, 0x00, //partition id (0)
          0x00, 0x01, //errorCode (1)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, //highwatermark offset (1) - string
          0x00, 0x00, 0x00, 32, //messageset size (32 bytes)

          //message set
          //message 0
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, //offset (1)
          0x00, 0x00, 0x00, 0x10, //message size (20)
          0x00, 0x00, 0x00, 0x00, //crc32 (0)
          0x00, //magic byte
          0x00, //attributes,
          0xFF, 0xFF, 0xFF, 0xFF, //-1 key length - null key
          0x00, 0x00, 0x00, 0x06, //message value length (6)
          0x21, 0x21, 0x22, 0x22, 0x23, 0x23 //message body

      ])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(9)

      const res = msg.readFetchResponse()
      expect(res.topics).to.have.length(1)

      const [topic] = res.topics
      expect(topic.partitions).to.have.length(1)

      const [part1] = topic.partitions
      expect(part1.messageSet.messages).to.have.length(1)
      expect(part1).to.eql({
        id: 0,
        errorCode: 1,
        highwaterMarkOffset: '2',
        messageSet: {
          size: 32,
          messages: [{
            offset: '1',
            size: 16,
            crc: 0,
            magicByte: 0,
            attributes: 0,
            key: null,
            value: Buffer([0x21, 0x21, 0x22, 0x22, 0x23, 0x23])
          }]
        }
      })
    })

    it('parses mutli-message response', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 104, //length (192)
        0x00, 0x00, 0x00, 0x00, //correlationId (0)
        0x00, 0x00, 0x00, 0x01, //length of topic array (1)

        0x00, 0x04, //topic name length (4)
        0x74, 0x65, 0x73, 0x74, //topic name ('test')
        0x00, 0x00, 0x00, 0x01, //partition array length

        0x00, 0x00, 0x00, 0x01, //partition id (1)
        0x00, 0x00, //errorCode (0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x87, 0xfd, //highwatermark offset ('100349')

        0x00, 0x00, 0x00, 0x44, //messageset size (68)

        //message 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x87, 0xfb, //offset ('100347')
        0x00, 0x00, 0x00, 0x16, //messagesize (22)
        0x78, 0x32, 0xdb, 0xa0, //crc32
        0x00, //magic byte
        0x00, //attributes
        0x00, 0x00, 0x00, 0x04, //key length (4)  //58
        0x70, 0x6f, 0x6f, 0x70, //key
        0x00, 0x00, 0x00, 0x04, //value length (4)
        0x66, 0x61, 0x72, 0x74, //value //70

        //message 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x87, 0xfc, //offset ('100348')
        0x00, 0x00, 0x00, 0x16, //messagesize (22)
        0x78, 0x32, 0xdb, 0xa0, //crc32
        0x00, //magic byte
        0x00, //attributes
        0x00, 0x00, 0x00, 0x04, //key length (4)
        0x70, 0x6f, 0x6f, 0x70, //key
        0x00, 0x00, 0x00, 0x04, //value length (4)
        0x66, 0x61, 0x72, 0x74, //value
      ])

      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(0)

      const res = msg.readFetchResponse()
      expect(res.topics).to.have.length(1)

      const [topic] = res.topics
      expect(topic.partitions).to.have.length(1)

      const [part1] = topic.partitions
      expect(part1.messageSet.messages).to.have.length(2)

      expect(part1).to.eql({
        id: 1,
        errorCode: 0,
        highwaterMarkOffset: '100349',
        messageSet: {
          size: 68,
          messages: [{
            offset: '100347',
            size: 22,
            crc: 2016598944,
            magicByte: 0,
            attributes: 0,
            key: Buffer([0x70, 0x6f, 0x6f, 0x70]),
            value: Buffer([0x66, 0x61, 0x72, 0x74])
          }, {
            offset: '100348',
            size: 22,
            crc: 2016598944,
            magicByte: 0,
            attributes: 0,
            key: Buffer([0x70, 0x6f, 0x6f, 0x70]),
            value: Buffer([0x66, 0x61, 0x72, 0x74])
          }]
        }
      })
    })
  })
})
