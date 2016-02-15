import bignum from 'bignum'

export default class Writer {
  constructor() {
    this.offset = 0
    this._buffer = new Buffer([0, 0, 0, 0])
  }

  int16(num) {
    const newBuffer = new Buffer(2)
    newBuffer.writeInt16BE(num, 0)
    this.buffer(newBuffer)
  }

  int32(num, fieldName) {
    const newBuffer = new Buffer(4)
    newBuffer.writeInt32BE(num, 0)
    this.buffer(newBuffer)
  }

  int64(num) {
    if (num == '-1') {
      this.buffer(new Buffer([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]))
    }
    else if (num == '-2') {
      this.buffer(new Buffer([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]))
    } else {
      const newBuffer = new bignum(num).toBuffer({ size: 8 })
      this.buffer(newBuffer)
    }
  }

  string(string, encoding = 'utf8') {
    if (typeof string == 'undefined' || string === null) {
      const buff = new Buffer(2)
      buff.writeInt16BE(-1)
      this._buffer = Buffer.concat([this._buffer, buff])
      return
    }
    const len = Buffer.byteLength(string, encoding)
    const buff = new Buffer(2 + len)
    buff.writeInt16BE(len, 0)
    buff.write(string, 2, len, encoding)
    this.buffer(buff)
  }

  array(items, cb) {
    this.int32(items.length)
    items.forEach(item => cb(item))
  }

  buffer(buffer) {
    this._buffer = Buffer.concat([this._buffer, buffer], this._buffer.length + buffer.length)
  }

  toBuffer() {
    this._buffer.writeInt32BE(this._buffer.length - 4, 0)
    return this._buffer
  }
}

