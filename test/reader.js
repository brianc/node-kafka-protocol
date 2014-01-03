var Reader = require('../lib/reader')
var MetadataResponseMessage = require('../lib/metadata-response-message')

describe('metdata response', function() {
  before(function() {
    this.reader = new Reader()
  })

  describe('empty response', function() {
    it('works', function() {

      this.reader.addChunk(Buffer([
        '00', '00', '00', 0x0c, //length
        '00', '00', '00', '09', //correlationId
        '00', '00', '00', '00', //broker array length 0
        '00', '00', '00', '00', //TopicMetadata array length 0
      ]))

      var packet = this.reader.read()
      assert(packet, 'Should have returned a packet')
      assert.equal(packet.correlationId, 9)
      var msg = MetadataResponseMessage.parse(packet)
      assert(msg, 'Should return a message')
      assert.equal(msg.correlationId, 9)
      assert.equal(msg.brokers.length, 0)
      assert.equal(msg.topics.length, 0)
    })
  })

  describe('simple, error code response', function() {
    var res = Buffer([
                     0x00, 0x00, 0x00, 0x1c, //length
                     0x00, 0x00, 0x00, 0x09, //correlationId
                     0x00, 0x00, 0x00, 0x00, //broker array length (0)
                     0x00, 0x00, 0x00, 0x01, //TopicMetadata array length (1)
                     0xff, 0xff, //TopicErrorCode
                     0x00, 0x08, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x21, //topicName
                     0x00, 0x00, 0x00, 0x00]) //partitionMetadata array (0 length)

    assert.equal(res.readInt16BE(16), -1, 'should have -1 for invalid request error code')
    console.log(res.readInt16BE(16))
  })

  describe('simple single topic response', function() {
    it('reads & parses', function() {
      var chunk = Buffer([
                         /*00*/ 0x00, 0x00, 0x00, 0x45, //length
                         /*04*/ 0x00, 0x00, 0x00, 0x09, //correlationId
                         /*08*/ 0x00, 0x00, 0x00, 0x01, //broker array length (1)
                         /*12*/ 0x00, 0x00, 0x00, 0x00, //broker - nodeId (0)
                         /*16*/ 0x00, 0x09, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // int16 of length (9) + "localhost"
                         /*27*/ 0x00, 0x00, 0x23, 0x84, //port - 9092
                         0x00, 0x00, 0x00, 0x01, //TopicMetadata array length (1)
                         0x00, 0x00, //TopicErorrCode
                         //QUESTION - TopicName MISSING in protocol spec - https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataResponse
                         0x00, 0x04, //TopicName string length (4)
                         0x74, 0x65, 0x73, 0x74, //TopicName string ("test" - (utf8))
                         0x00, 0x00, 0x00, 0x01, //PartitionMetadata Array Length (1)
                         0x00, 0x00, //PartitionErrorCode - (0) - no error
                         0x00, 0x00, 0x00, 0x00, //PartitionId - (0)
                         0x00, 0x00, 0x00, 0x00, //Leader (0)
                         0x00, 0x00, 0x00, 0x01, //Replicas Array Length (1)
                         0x00, 0x00, 0x00, 0x00, //Replica[0] id? (0)
                         0x00, 0x00, 0x00, 0x01, //Isr length (1)
                         0x00, 0x00, 0x00, 0x00 //The set subset of the replicas that are "caught up" to the leader (0)
      ]);
      this.reader.addChunk(chunk)
      var packet = this.reader.read()
      assert(packet, 'reader should have returned a full packet')
      var msg = MetadataResponseMessage.parse(packet)

      assert.equal(msg.brokers.length, 1)
      var broker = msg.brokers.pop()
      assert(broker)
      assert.strictEqual(broker.nodeId, 0)
      assert.equal(broker.host, 'localhost')
      assert.strictEqual(broker.port, 9092)


      assert.equal(msg.topics.length, 1)
      var topic = msg.topics.pop()
      assert(topic)
      assert.strictEqual(topic.errorCode, 0)
      assert.equal(topic.name, 'test')

      assert.equal(topic.partitions.length, 1)
      var partition = topic.partitions.pop()
      assert.strictEqual(partition.id, 0)
      assert.strictEqual(partition.errorCode, 0)
      assert.strictEqual(partition.leader, 0)
      assert.equal(partition.replicas.length, 1)
      assert.strictEqual(partition.replicas[0], 0)
      assert.equal(partition.isr.length, 1)
      assert.strictEqual(partition.isr[0], 0)
    })
  })
})
