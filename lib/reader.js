import bignum from 'bignum'
import { dump } from '../example/send'

export default class Reader {
  constructor(buffer) {
    this._buffer = buffer
    this.offset = 0
  }

  byte() {
    return this._buffer[this.offset++]
  }

  bytes() {
    const size = this.int32()
    if (size === -1) {
      return null
    }
    const offset = this.offset
    this.offset = offset + size
    return this._buffer.slice(offset, this.offset)
  }

  int32() {
    const offset = this.offset
    this.offset += 4
    return this._buffer.readInt32BE(offset)
  }

  int16() {
    const offset = this.offset
    this.offset += 2
    return this._buffer.readInt16BE(offset)
  }

  string() {
    const strLen = this.int16()
    const offset = this.offset
    this.offset = this.offset + strLen
    return this._buffer.toString('utf8', offset, this.offset)
  }

  int64() {
    const offset = this.offset
    this.offset += 8
    const slice = this._buffer.slice(offset, this.offset)
    return bignum.fromBuffer(slice, { size: 4 }).toString()
  }

  int32Array() {
    const len = this.int32()
    const result = []
    for (let i = 0; i < len; i++) {
      result.push(this.int32())
    }
    return result
  }

  array(cb) {
    const len = this.int32()
    const result = []
    for (let i = 0; i < len; i++) {
      result.push(cb(this))
    }
    return result
  }

  remaining() {
    return this._buffer.length - this.offset;
  }

  slice(size) {
    const offset = this.offset
    this.offset += size
    if (this.offset > this._buffer.length) {
      console.log(this._buffer.slice(offset))
      throw new Error('TODO: Cannot slice past end of buffer')
    }
    return new Reader(this._buffer.slice(offset, this.offset))
  }
}
