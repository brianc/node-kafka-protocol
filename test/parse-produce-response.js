import expect from 'expect.js'
import parse from './parse'

describe('parse', function() {
  describe('produce-response', () => {

    it('parses simple response', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 0x20, //length (32)
        0x00, 0x00, 0x00, 0x00, //correlationId (0)
        0x00, 0x00, 0x00, 0x01, //length of topic array (1)
        0x00, 0x04, //length of topic name (4)
        0x74, 0x65, 0x73, 0x74, //topic name (test)
        0x00, 0x00, 0x00, 0x01, //partition array length (1)
        0x00, 0x00, 0x00, 0x00, //partition (0)
        0x00, 0x00, //errorCode (0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x87, 0xfa//offset
      ])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(0)

      const res = msg.readProduceResponse()
      expect(res.correlationId).to.be(0)
      expect(res.topics).to.have.length(1)
      const [topic] = res.topics;
      expect(topic.name).to.be('test')
      expect(topic.partitions).to.have.length(1)
      const [partition] = topic.partitions
      expect(partition.errorCode).to.be(0)
      expect(partition.offset).to.be('100346')
    })
  })
})
