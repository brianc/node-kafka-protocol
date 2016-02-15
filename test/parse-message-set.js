import expect from 'expect.js'
import MessageSet from '../lib/message-set'
import Reader from '../lib/reader'

describe('parse', function() {
  describe('message-set', () => {

    it('parses totally empty message set', async () => {
      const buff = Buffer([0, 0, 0, 0])
      const set = new MessageSet().read(new Reader(buff))
      expect(set).to.have.length(0)
    })
  })
})
