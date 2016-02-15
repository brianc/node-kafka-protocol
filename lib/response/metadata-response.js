export default class MetadataResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.brokers = []
    this.topics = []
  }

  read(reader) {
    this.brokers = reader.array(reader => {
      const nodeId = reader.int32()
      const host = reader.string()
      const port = reader.int32()
      return new Broker(nodeId, host, port)
    })

    this.topics = reader.array(reader => {
      const errorCode = reader.int16()
      const topicName = reader.string()
      const topic = new TopicMetadata(errorCode, topicName)
      topic.partitions = reader.array(reader => {
        const partitionErrorCode = reader.int16()
        const id = reader.int32()
        const leader = reader.int32()
        const replicas = reader.int32Array()
        const isr = reader.int32Array()
        return new PartitionMetadata(partitionErrorCode, id, leader, replicas, isr)
      })
      return topic
    })
    return this
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
