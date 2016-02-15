import MessageSet from '../message-set'

class Partition {
  constructor(id, errorCode, highwaterMarkOffset, messageSetSize) {
    this.id = id
    this.errorCode = errorCode
    this.highwaterMarkOffset = highwaterMarkOffset
    this.messageSet = new MessageSet(messageSetSize)
  }
}

class Topic {
  constructor(name, partitions) {
    this.name = name
    this.partitions = partitions
  }
}

export default class FetchResponse {
  constructor(correlationId) {
    this.correlationId = correlationId
    this.topics = []
  }

  read(reader) {
    this.topics = reader.array(reader => {
      const topicName = reader.string()
      return new Topic(topicName, reader.array(part => {
        const id = reader.int32()
        const errorCode = reader.int16()
        const highwaterMarkOffset = reader.int64()
        const messageSetSize = reader.int32()
        return new Partition(id, errorCode, highwaterMarkOffset, messageSetSize)
      }))
    })
    return this
  }
}