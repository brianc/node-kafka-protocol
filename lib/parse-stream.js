import { Transform } from 'stream'
import PacketReader from 'packet-reader'
import BufferReader from './reader'
import MetadataResponse from './response/metadata-response'
import OffsetResponse from './response/offset-response'
import ProduceResponse from './response/produce-response'
import FetchResponse from './response/fetch-response'

class Message {
  constructor(buffer) {
    this._buffer = buffer
    this.reader = new BufferReader(buffer)
    this.correlationId = this.reader.int32()
  }

  readMetadataResponse() {
    return new MetadataResponse(this.correlationId).read(this.reader)
  }

  readOffsetResponse() {
     return new OffsetResponse(this.correlationId).read(this.reader)
  }

  readProduceResponse() {
    return new ProduceResponse(this.correlationId).read(this.reader)
  }

  readFetchResponse() {
    return new FetchResponse(this.correlationId).read(this.reader)
  }

}

export default class ParseStream extends Transform {
  constructor(options) {
    super({ objectMode: true })
    this._reader = new PacketReader(0)
  }

  _transform(chunk, encoding, done) {
    this._reader.addChunk(chunk)
    let packet = this._reader.read()
    while(packet) {
      this.push(new Message(packet))
      packet = this._reader.read()
    }
    done()
  }
}
