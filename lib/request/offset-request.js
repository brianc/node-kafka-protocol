import _ from 'lodash'

import Writer from '../writer'
import Request from './request'

class OffsetPartitionMessage {
  constructor(partition, time, maxNumOffsets) {
    this.partition = partition
    this.time = time
    this.maxNumOffsets = maxNumOffsets
  }
}

class OffsetRequestMessage {
  constructor(topic) {
    this.topic = topic
    this.partitions = []
  }
}

export default class OffsetRequest extends Request {
  constructor(clientId) {
    super(2, clientId)
    this.replicaId = null
    this._topics = []
  }

  add(topic, partition, time, maxNumOffsets) {
    let existing = _.find(this._topics, { topic })
    if (!existing) {
      existing = new OffsetRequestMessage(topic)
      this._topics.push(existing)
    }
    const msg = new OffsetPartitionMessage(partition, time, maxNumOffsets)
    existing.partitions.push(msg)
  }

  toBuffer(writer = new Writer()) {
    super.toBuffer(writer)
    writer.int32(this.replicaId, 'replicaId')
    writer.array(this._topics, topic => {
      writer.string(topic.topic)
      writer.array(topic.partitions, part => {
        writer.int32(part.partition)
        writer.int64(part.time)
        writer.int32(part.maxNumOffsets)
      })
    })
    return writer.toBuffer()
  }
}
