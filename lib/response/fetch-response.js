import MessageSet from '../message-set'

class Partition {
  constructor(id, errorCode, highwaterMarkOffset, messageSet) {
    this.id = id
    this.errorCode = errorCode
    this.highwaterMarkOffset = highwaterMarkOffset
    this.messageSet = messageSet
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
    this.topics = null
  }

  read(reader) {
    this.topics = reader.array(reader => {
      const topicName = reader.string()
      return new Topic(topicName, reader.array(part => {
        const id = reader.int32()
        const errorCode = reader.int16()
        const highwaterMarkOffset = reader.int64()
        const messageSetSize = reader.int32()

        const messageSet = new MessageSet(messageSetSize)
        messageSet.read(reader)

        return new Partition(id, errorCode, highwaterMarkOffset, messageSet)
      }))
    })
    return this
  }
}
