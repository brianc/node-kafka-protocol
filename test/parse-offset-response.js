import expect from 'expect.js'
import parse from './parse'

describe('parse', function() {
  describe('offset-response', () => {

    it('parses simple response', async () => {
      const buff = Buffer([
        0x00, 0x00, 0x00, 0x24, //length (32)
        0x00, 0x00, 0x00, 0x00, //correlationId (0)
        0x00, 0x00, 0x00, 0x01, //length of topic array (1)
        0x00, 0x04, //length of topic name (4)
        0x74, 0x65, 0x73, 0x74, //topic name (test)
        0x00, 0x00, 0x00, 0x01, //partition array length (1)
        0x00, 0x00, 0x00, 0x00, //partition (0)
        0x00, 0x00, //errorCode (0)
        0x00, 0x00, 0x00, 0x01, //offset array length (1)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07//offset
      ])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(0)

      const res = msg.readOffsetResponse()
      expect(res.correlationId).to.be(0)
      expect(res.topics).to.have.length(1)
      const [topic] = res.topics
      expect(topic.name).to.be('test')
      expect(topic.partitions).to.have.length(1)
      const [partition] = topic.partitions
      expect(partition.errorCode).to.be(0)
      expect(partition.offsets).to.have.length(1)
      expect(partition.offsets[0]).to.be('7')
    })
  })
})
