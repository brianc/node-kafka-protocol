import expect from 'expect.js'
import Client from '../../lib/client'
import { MetadataRequest } from '../../lib/request'
import { MetadataResponse, ProduceResponse } from '../../lib/response'

const HOST = 'localhost'
const PORT = 9092

describe('client', () => {
  before(function() {
    this.client = new Client()
    return this.client.connect(HOST, PORT)
  })

  after(function() {
    return this.client.end()
  })

  it('can send blank metadata message', async function() {
    const req = new MetadataRequest()
    const res = await this.client.send(req)
    expect(res).to.be.ok()
    expect(res).to.be.a(MetadataResponse)
  })

  it('can send metadata request with helper method', async function() {

    const res = await this.client.metadata(['test'])
    expect(res).to.be.ok()
    expect(res).to.be.a(MetadataResponse)
  })

  it('can send produce request without ack', async function() {
    const opts = { requiredAcks: 0 }
    const value = Buffer('hello!', 'utf8')
    const res = await this.client.produce('test', 0, null, value, opts)
    expect(res).to.be(true)
  })

  it('can send produce request with acks', async function() {
    const key = Buffer(Date.now().toString(), 'utf8')
    const value = Buffer('hello!', 'utf8')
    const res = await this.client.produce('test', 0, key, value)
    expect(res).to.be.a(ProduceResponse)
  })
})
