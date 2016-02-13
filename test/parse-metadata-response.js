import expect from 'expect.js'
import parse from './parse'

describe('parse', function() {
  describe('metadata-response', () => {

    it('parses totally empty message', async () => {
      const buff = Buffer([0, 0, 0, 0x0c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(0)

      const res = msg.readMetadataResponse()
      expect(res.correlationId).to.be(0)
      expect(res.brokers).to.have.length(0)
      expect(res.topics).to.have.length(0)
    })

    it('parses simple message', async function() {
      const buff = Buffer([
        0x00, 0x00, 0x00, 0x63, //message length - int32 (99)
        0x00, 0x00, 0x00, 0x06, //correlation id - int32 (6)
        0x00, 0x00, 0x00, 0x01, //broker array length (1)
        //first broker array item
        0x00, 0x00, 0x00, 0x00, //nodeId - int32 (0)
        0x00, 0x0d, //host string length - int16 (13)
        0x31, 0x39, 0x32, 0x2e, 0x31, 0x36, 0x38, 0x2e, 0x31, 0x2e, 0x31, 0x31, 0x31, //host string contents
        0x00, 0x00, 0x23, 0x84, //port - int32
        0x00, 0x00, 0x00, 0x01, //topic metadata array length - int32 (1)
        0x00, 0x00, //topic error code - int16 (0)
        0x00, 0x04, //topic name string length - int16 (4)
        0x74, 0x65, 0x73, 0x74, //topic name string contents - 'test'

        0x00, 0x00, 0x00, 0x02, //partition metadata array length - int32 (4)
        //first partition metadata array item
        0x00, 0x00, //partition error code - int16 (0)
        0x00, 0x00, 0x00, 0x01, //partitionId - int32 (1)
        0x00, 0x00, 0x00, 0x00, //leader - int32(0)
        0x00, 0x00, 0x00, 0x01, //replicas array length - int32 (1)
        0x00, 0x00, 0x00, 0x00, //replicas array item - int32 (0)
        0x00, 0x00, 0x00, 0x01, //lsr array length - int32 (1)
        0x00, 0x00, 0x00, 0x00, //lsr array item - int32 (0)
        //second partition metadata array item
        0x00, 0x00, //partition error code - int16 (0)
        0x00, 0x00, 0x00, 0x00, //partitionId - int32 (0)
        0x00, 0x00, 0x00, 0x00, //leader - int32 (0)
        0x00, 0x00, 0x00, 0x01, //replicas array length - int32 (1)
        0x00, 0x00, 0x00, 0x00, //replicas array item - int32 (0)
        0x00, 0x00, 0x00, 0x01, //lsr array length - int32 (1)
        0x00, 0x00, 0x00, 0x00 //lsr array item - int32 (0)
      ])

      const messages = await parse(buff)
      expect(messages).to.have.length(1)
      const [msg] = messages
      expect(msg.correlationId).to.be(6)

      const res = msg.readMetadataResponse()
      expect(res.correlationId).to.be(6)
      expect(res.brokers).to.have.length(1)
      const [broker] = res.brokers
      expect(broker).to.eql({
        nodeId: 0,
        host: '192.168.1.111',
        port: 9092,
      })
      expect(res.topics).to.have.length(1)
      const [topic] = res.topics;
      expect(topic).to.eql({
        errorCode: 0,
        name: 'test',
        partitions: [{
          errorCode: 0,
          id: 1,
          leader: 0,
          replicas: [0],
          isr: [0],
        },{
          errorCode: 0,
          id: 0,
          leader: 0,
          replicas: [0],
          isr: [0],
        }]
      })
    })
  })
})
