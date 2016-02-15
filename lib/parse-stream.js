import { Transform } from 'stream'
import PacketReader from 'packet-reader'
import BufferReader from './reader'

class MetadataResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.brokers = []
    this.topics = []
  }
}

class Broker {
  constructor(nodeId, host, port) {
    this.nodeId = nodeId
    this.host = host
    this.port = port
  }
}

class TopicMetadata {
  constructor(errorCode, name) {
    this.errorCode = errorCode
    this.name = name
    this.partitions = []
  }
}

class PartitionMetadata {
  constructor(errorCode, id, leader, replicas, isr) {
    this.errorCode = errorCode
    this.id = id
    this.leader = leader
    this.replicas = replicas
    this.isr = isr
  }
}


class ProducePartition {
  constructor(id, errorCode, offset) {
    this.id = id
    this.errorCode = errorCode
    this.offset = offset
  }
}

class ProduceTopic {
  constructor(name) {
    this.name = name
    this.partitions = []
  }
}

class ProduceResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.topics = []
  }
}


class OffsetPartitionOffsets {
  constructor(partition, errorCode) {
    this.partition = partition
    this.errorCode = errorCode
    this.offsets = []
  }
}

class OffsetTopicPartitions {
  constructor(topic) {
    this.name = topic
    this.partitions = []
  }
}

class OffsetResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.topics = []
  }
}

class Message {
  constructor(buffer) {
    this._buffer = buffer
    this.reader = new BufferReader(buffer)
    this.correlationId = this.reader.int32()
  }

  readProduceResponse() {
    const msg = new ProduceResponse(this.correlationId)
    msg.topics = this.reader.array(reader => {
      const topic = new ProduceTopic(reader.string())
      topic.partitions = reader.array(reader => {
        const id = reader.int32()
        const errorCode = reader.int16()
        const offset = reader.int64()
        return new ProducePartition(id, errorCode, offset)
      })
      return topic
    })
    return msg
  }

  readMetadataResponse() {
    const msg = new MetadataResponse(this.correlationId)
    msg.brokers = this.reader.array(reader => {
      const nodeId = this.reader.int32()
      const host = this.reader.string()
      const port = this.reader.int32()
      return new Broker(nodeId, host, port)
    })

    msg.topics = this.reader.array(reader => {
      const errorCode = this.reader.int16()
      const topicName = this.reader.string()
      const topic = new TopicMetadata(errorCode, topicName)
      topic.partitions = reader.array(reader => {
        const partitionErrorCode = this.reader.int16()
        const id = this.reader.int32()
        const leader = this.reader.int32()
        const replicas = this.reader.int32Array()
        const isr = this.reader.int32Array()
        return new PartitionMetadata(partitionErrorCode, id, leader, replicas, isr)
      })
      return topic
    })
    return msg
  }

  readOffsetResponse() {
    const msg = new MetadataResponse(this.correlationId)
    msg.topics = this.reader.array(reader => {
      const topicName = reader.string()
      const topic = new OffsetTopicPartitions(topicName)
      topic.partitions = reader.array(reader => {
        const partitionId = reader.int32()
        const errorCode = reader.int16()
        const offset = new OffsetPartitionOffsets(partitionId, errorCode)
        offset.offsets = reader.array(reader => {
          return reader.int64()
        })
        return offset
      })
      return topic
    })
    return msg
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
