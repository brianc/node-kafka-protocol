import expect from 'expect.js'
import FetchRequest from '../lib/request/fetch-request'
import { dump } from '../example/send'

describe('writing', function() {
  describe('fetch-request', function() {
    it('writes an empty fetch request', () => {
      const fetch = new FetchRequest()
      const expected = [
        0x00, 0x00, 0x00, 26,  //length of the rest of themessage, not including these 4 bytes
        0x00, 0x01, //api key - 1 for fetch request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //length of clientId string (-1 = null)
        255, 255, 255, 255, //replica id (-1)
        0x00, 0x00, 0x00, 0x00, //maxWaitTime (0)
        0x00, 0x00, 0x00, 0x00, //minBytes (0)
        0x00, 0x00, 0x00, 0x00, //topic array length (0)
      ]

      expect(fetch.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes simple fetch request', () => {
      const fetch = new FetchRequest()
      fetch.add('test', 1, '64', 7)
      const expected = [
        0x00, 0x00, 0x00, 52,  //length of the rest of themessage, not including these 4 bytes
        0x00, 0x01, //api key - 1 for fetch request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        255, 255, //length of clientId string (-1 = null)
        255, 255, 255, 255, //replica id (-1)
        0x00, 0x00, 0x00, 0x00, //maxWaitTime (0)
        0x00, 0x00, 0x00, 0x00, //minBytes (0)
        0x00, 0x00, 0x00, 0x01, //topic array length (1)

        //first topic (test)
        0x00, 0x04, //topic name length (4)
        116, 101, 115, 116, //topic name string (test)
        0x00, 0x00, 0x00, 0x01, //partition array length

          //first partition
          0x00, 0x00, 0x00, 0x01, //partitionId (1)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, //offset (64)
          0x00, 0x00, 0x00, 0x07, //maxBytes (7)
      ]
      expect(fetch.toBuffer()).to.eql(Buffer(expected))
    })

    it('writes an complex fetch request', () => {
      const fetch = new FetchRequest('!!')
      fetch.replicaId = 2
      fetch.maxWaitTime = 1
      fetch.minBytes = 1024
      fetch.add('test', 1, '64', 7)
      fetch.add('test', 2, '64', 8)
      fetch.add('foo', 3, '2', 9)
      const expected = [
        0x00, 0x00, 0x00, 95,  //length of the rest of themessage, not including these 4 bytes
        0x00, 0x01, //api key - 1 for fetch request
        0x00, 0x00, //api version - 0 for v0.8
        0x00, 0x00, 0x00, 0x00, //correlationId
        0x00, 0x02, //length of clientId string (2)
        0x21, 0x21, //clientId (!!)
        0x00, 0x00, 0x00, 0x02, //replica id (-1)
        0x00, 0x00, 0x00, 0x01, //maxWaitTime (0)
        0x00, 0x00, 0x04, 0x00, //minBytes (0)
        0x00, 0x00, 0x00, 0x02, //topic array length (2)

        //first topic (test)
        0x00, 0x04, //topic name length (4)
        116, 101, 115, 116, //topic name string (test)
        0x00, 0x00, 0x00, 0x02, //partition array length

          //first partition
          0x00, 0x00, 0x00, 0x01, //partitionId (1)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, //offset (64)
          0x00, 0x00, 0x00, 0x07, //maxBytes (7)

          //second partition
          0x00, 0x00, 0x00, 0x02, //partitionId (1)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, //offset (64)
          0x00, 0x00, 0x00, 0x08, //maxBytes (8)

        //second topic (foo)
        0x00, 0x03, //topic name length (3)
        102, 111, 111, //topic name string (foo)
        0x00, 0x00, 0x00, 0x01, //partition array length (1)

          //first partition
          0x00, 0x00, 0x00, 0x03, //partitionId (3)
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, //offset (2)
          0x00, 0x00, 0x00, 0x09, //maxBytes (9)

      ]

      expect(fetch.toBuffer()).to.eql(Buffer(expected))
    })
  })
})
