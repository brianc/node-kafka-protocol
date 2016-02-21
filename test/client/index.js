import expect from 'expect.js'
import Client from '../../lib/client'
import { MetadataRequest } from '../../lib/request'
import {
  MetadataResponse,
  ProduceResponse,
  OffsetResponse,
  FetchResponse,
} from '../../lib/response'

const HOST = 'localhost'
const PORT = 9092
const TOPIC = 'test'
const PARTITION = 0

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

    const res = await this.client.metadata([TOPIC])
    expect(res).to.be.ok()
    expect(res).to.be.a(MetadataResponse)
  })

  it('can send produce request without ack', async function() {
    const opts = { requiredAcks: 0 }
    const value = Buffer('hello!', 'utf8')
    const res = await this.client.produce(TOPIC, PARTITION, null, value, opts)
    expect(res).to.be(true)
  })

  it('can send produce request with acks', async function() {
    const key = Buffer(Date.now().toString(), 'utf8')
    const value = Buffer('hello!', 'utf8')
    const res = await this.client.produce(TOPIC, PARTITION, key, value)
    expect(res).to.be.a(ProduceResponse)
  })

  it('can send offset request', async function() {
    const res = await this.client.offset(TOPIC, PARTITION, -1)
    expect(res).to.be.an(OffsetResponse)
  })

  it('can fetch', async function() {
    const res = await this.client.fetch(TOPIC, PARTITION, 0)
    expect(res).to.be.a(FetchResponse)
  })

  it('can roundtrip', async function() {
    const key = Buffer('testing', 'utf8')
    const value = Buffer('this is the song that never ends', 'utf8')

    //produce the message
    const res = await this.client.produce(TOPIC, PARTITION, key, value)
    expect(res).to.be.a(ProduceResponse)
    expect(res.topics).to.have.length(1)
    const [top] = res.topics
    expect(top.partitions).to.have.length(1)
    const offset = top.partitions[0].offset

    //fetch the last produced message
    const fetch = await this.client.fetch(TOPIC, PARTITION, offset)
    expect(fetch).to.be.a(FetchResponse)
    expect(fetch.topics).to.have.length(1)
    const [topic] = fetch.topics
    expect(topic.partitions).to.have.length(1)
    const [part] = topic.partitions
    expect(part.messageSet.messages).to.have.length(1)
    const [msg] = part.messageSet.messages;

    expect(msg.offset).to.be(offset)
    expect(msg.key.toString('utf8')).to.be('testing')
    expect(msg.value.toString('utf8')).to.be('this is the song that never ends')
  })
})
