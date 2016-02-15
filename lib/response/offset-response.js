export default class OffsetResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.topics = []
  }

  read(reader) {
    this.topics = reader.array(reader => {
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
    return this

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

