export default class Request {
  constructor(apiKey, clientId) {
    this.apiKey = apiKey
    this.apiVersion = 0
    this.correlationId = 0
    this.clientId = clientId
  }

  toBuffer(writer = new Writer()) {
    writer.int16(this.apiKey)
    writer.int16(this.apiVersion)
    writer.int32(this.correlationId)
    writer.string(this.clientId)
  }
}
