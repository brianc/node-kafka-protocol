import expect from 'expect.js'
import OffsetRequest from '../lib/request/offset-request'
import { dump } from '../example/send'

describe('writing', function() {
  describe('offset-request', function() {
    it('writes a request', () => {
      const req = new OffsetRequest()
      req.replicaId = 1
      const topic = '!'
      req.add(topic, 0, '1', 1)
      req.add(topic, 1, '2', 2)
      const expected = [
        0x00, 0x00, 0x00, 57, //length of the rest of the message, not including these bytes
        0x00, 0x02, //api key - 2 for offset request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //length of clientId string (-1 = null)
        0x00, 0x00, 0x00, 0x01, //replicaId
        0x00, 0x00, 0x00, 0x01, //topic array length
        0x00, 0x01, //topic name length
        33, //topic name (!)
        0x00, 0x00, 0x00, 0x02, //partition array length
        0x00, 0x00, 0x00, 0x00, //partitionId
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, //time - int64 (-1)
        0x00, 0x00, 0x00, 0x01, //max number of offsets (1)
        0x00, 0x00, 0x00, 0x01, //partitionId
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, //time - int64 (-2)
        0x00, 0x00, 0x00, 0x02, //max number of offsets (1)
      ]
      expect(req.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes a request with negative bignums', false, () => {
      const req = new OffsetRequest()
      req.replicaId = 1
      const topic = '!'
      req.add(topic, 0, '-1', 1)
      req.add(topic, 1, '-2', 2)
      const expected = [
        0x00, 0x00, 0x00, 57, //length of the rest of the message, not including these bytes
        0x00, 0x02, //api key - 2 for offset request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //length of clientId string (-1 = null)
        0x00, 0x00, 0x00, 0x01, //replicaId
        0x00, 0x00, 0x00, 0x01, //topic array length
        0x00, 0x01, //topic name length
        33, //topic name (!)
        0x00, 0x00, 0x00, 0x02, //partition array length
        0x00, 0x00, 0x00, 0x00, //partitionId
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, //time - int64 (-1)
        0x00, 0x00, 0x00, 0x01, //max number of offsets (1)
        0x00, 0x00, 0x00, 0x01, //partitionId
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, //time - int64 (-2)
        0x00, 0x00, 0x00, 0x02, //max number of offsets (1)
      ]
      expect(req.toBuffer()).to.eql(Buffer(expected))
    })
  })
})
