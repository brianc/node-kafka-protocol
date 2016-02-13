import _ from 'lodash'

import Writer from '../writer'
import Request from './request'
import MessageSet from '../message-set'

class ProduceMessage {
  constructor(topic, partition, key, value) {
    this.topic = topic
    this.partition = partition
    this.key = key
    this.value = value
  }
}

export default class ProduceRequest extends Request {
  constructor(clientId) {
    super(0, clientId)
    this.requiredAcks = 1
    this.timeout = 1000
    this._messages = []
  }

  add(topic, partition, key, value) {
    let item = _.find(this._messages, { topic })
    if (!item) {
      item = {
        topic,
        partitions: [],
      }
      this._messages.push(item)
    }
    let part = _.find(item.partitions, { id: partition })
    if (!part) {
      part = {
        id: partition,
        messageSet: new MessageSet(),
      }
      item.partitions.push(part)
    }
    part.messageSet.add(key, value)
  }

  toBuffer(writer = new Writer()) {
    super.toBuffer(writer)
    writer.int16(this.requiredAcks)
    writer.int32(this.timeout)
    writer.array(this._messages, msg => {
      writer.string(msg.topic)
      writer.array(msg.partitions, part => {
        writer.int32(part.id)
        writer.int32(part.messageSet.size)
        writer.buffer(part.messageSet.toBuffer())
      })
    })
    return writer.toBuffer()
  }
}
