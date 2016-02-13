import bignum from 'bignum'

export default class Reader {
  constructor(buffer) {
    this._buffer = buffer
    this.offset = 0
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
}
