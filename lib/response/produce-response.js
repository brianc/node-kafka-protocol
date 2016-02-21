export default class ProduceResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.topics = null
  }

  read(reader) {
    this.topics = reader.array(reader => {
      const topic = new ProduceTopic(reader.string())
      topic.partitions = reader.array(reader => {
        const id = reader.int32()
        const errorCode = reader.int16()
        const offset = reader.int64()
        return new ProducePartition(id, errorCode, offset)
      })
      return topic
    })
    return this
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
