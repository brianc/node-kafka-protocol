import Writer from './writer'
import bignum from 'bignum'
import { buf as crc32 } from 'crc-32'

const emptyBignum = Buffer([0, 0, 0, 0, 0, 0, 0, 0])
const NULL_BUFFER = Buffer(0)

const lenBuffer = other => {
  const buff = new Buffer(4)
  buff.writeInt32BE(other ? other.length : -1, 0)
  return buff
}

class Message {
  constructor(key, value) {
    this.key = key
    this.value = value
    this.size = (key ? key.length : 0) + (value ? value.length : 0) + 14
  }

  toBuffer() {
    const header = new Buffer([0, 0, 0, 0, 0, 0])
    const buffers = [header, lenBuffer(this.key)]
    if (this.key) {
      buffers.push(this.key)
    }
    buffers.push(lenBuffer(this.value))
    if (this.value) {
      buffers.push(this.value)
    }
    const result = Buffer.concat(buffers, this.size)
    const crc = crc32(result.slice(4))
    result.writeInt32BE(crc, 0)
    return result
  }
}

export default class MessageSet {
  constructor(size = 0) {
    this.size = size
    this.messages = []
  }

  add(key, value) {
    const msg = new Message(key, value)
    this.size += (msg.size + 12)
    this.messages.push(msg)
  }

  toBuffer(writer = new Writer()) {
    const buffers = this.messages.map(message => {
      const messageBuffer = message.toBuffer()
      const lenBuffer = new Buffer(4)
      lenBuffer.writeInt32BE(messageBuffer.length)
      return Buffer.concat([emptyBignum, lenBuffer, messageBuffer])
    })
    return Buffer.concat(buffers)
  }

  read(reader) {
    if (!this.size) {
      console.warn('missing message set size:', this.size)
      return this;
    }
    const buff = reader.slice(this.size)
    this.messages.push({
      offset: buff.int64(),
      size: buff.int32(),
      crc: buff.int32(),
      magicByte: buff.byte(),
      attributes: buff.byte(),
      key: buff.bytes(),
      value: buff.bytes()
    })
    return this
  }
}
