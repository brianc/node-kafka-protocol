import Writer from '../writer'
import Request from './request'

export default class MetadataRequest extends Request {
  constructor(clientId, topics = []) {
    super(3, clientId)
    this.topics = topics
  }

  toBuffer(writer = new Writer()) {
    super.toBuffer(writer)
    writer.array(this.topics, topic => {
      writer.string(topic)
    })
    return writer.toBuffer()
  }
}
