var requests = require('../lib/requests')
var Message = requests.Message

var MessageSet = module.exports = function(messages) {
  this.messages = messages
}

MessageSet.prototype.toBuffer = function() {
  var len = 0
  for(var i = 0; i < this.messages.length; i++) {
    len += 8 //int64 offset - does not matter for outgoing messages
    len += 4 //int32 messageSize
    len += this.messages[i].getLength()
  }
  var buff = Buffer(len + 4)
  buff.fill(0)
  var offset = 4
  buff.writeInt32BE(this.messages.length, 0)
  for(var i = 0; i < this.messages.length; i++) {
    var msg = this.messages[i]


    //set the outgoing offset int64 to 0
    //to make testing easier
    for(var j = 0; j < 8; j++) {
      buff[j + offset] = 0
    }
    offset += 8
    //write the length of the message
    buff.writeInt32BE(msg.getLength(), offset)
    offset += 4
    msg.writeBuffer(buff, offset)
    return buff
  }
}

