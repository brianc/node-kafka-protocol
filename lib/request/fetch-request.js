import _ from 'lodash'

import Writer from '../writer'
import Request from './request'

class FetchTopic {
  constructor(name) {
    this.name = name
    this.partitions = []
  }
}

export default class FetchRequest extends Request {
  constructor(clientId) {
    super(1, clientId)
    this.replicaId = -1
    this.maxWaitTime = 0
    this.minBytes = 0
    this._topics = []
  }

  add(topicName, partition, offset, maxBytes) {
    let topic = _.find(this._topics, { name: topicName })
    if (!topic) {
      topic = new FetchTopic(topicName)
      this._topics.push(topic)
    }
    topic.partitions.push({ id: partition, offset, maxBytes })
  }

  toBuffer(writer = new Writer()) {
    super.toBuffer(writer)
    writer.int32(this.replicaId)
    writer.int32(this.maxWaitTime)
    writer.int32(this.minBytes)
    writer.array(this._topics, topic => {
      writer.string(topic.name)
      writer.array(topic.partitions, part => {
        writer.int32(part.id)
        writer.int64(part.offset)
        writer.int32(part.maxBytes)
      })
    })
    return writer.toBuffer()
  }
}
