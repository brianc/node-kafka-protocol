var Broker = function() {
  this.nodeId = null
  this.host = null
  this.port = null
}

var Topic = function() {
  this.errorCode = null
  this.name = null
  this.partitions = null
}

var Partition = function() {
  this.errorCode = null
  this.id = null
  this.leader = null
  this.replicas = null
  this.isr = null
}

var MetadataResponseMessage = module.exports = function(correlationId) {
  this.correlationId = correlationId
  this.brokers = null
  this.topics = null
}

var parsePartition = function(packet) {
  var partition = new Partition()
  partition.errorCode = packet.readInt16BE()
  partition.id = packet.readInt32BE()
  partition.leader = packet.readInt32BE()
  partition.replicas = packet.readInt32BEArray()
  partition.isr = packet.readInt32BEArray()
  return partition
}

var parseTopic = function(packet) {
  var topic = new Topic()
  topic.errorCode = packet.readInt16BE()
  topic.name = packet.readString()
  topic.partitions = packet.mapArray(parsePartition)
  return topic
}

var parseBroker = function(packet) {
  var broker = new Broker()
  broker.nodeId = packet.readInt32BE()
  broker.host = packet.readString()
  broker.port = packet.readInt32BE()
  return broker
}

//Given a ResponseMessage instance
//read it and return the results as a metadata response
MetadataResponseMessage.parse = function(packet) {
  var result = new MetadataResponseMessage(packet.correlationId)
  result.brokers = packet.mapArray(parseBroker)
  result.topics = packet.mapArray(parseTopic)
  return result
}
