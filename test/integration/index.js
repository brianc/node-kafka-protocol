var ok = require('okay')

var Client = function(port, host) {
  this.port = port || 9092
  this.host = host || 'localhost'
  this.socket = null
}

Client.prototype.connect = function(cb) {
  this.socket = require('net').connect(this.port, this.host, cb)
}

Client.prototype.send = function(req) {
  console.log('wrote')
  this.socket.write(req.toBuffer())
}

function hex(d) {
  var hex = Number(d).toString(16);
  padding = 2

  while (hex.length < padding) {
    hex = "0" + hex;
  }

  return hex;
}

describe('integration', function() {
  it('connects', function(done) {
    var client = new Client()
    client.connect(done)
  })

  it('gets metadata', function(done) {
    var Metadata = require('../../lib/requests').MetadataRequest
    var client = new Client()
    client.connect(ok(done, function() {
      console.log('connected')
      client.socket.on('data', function(data) {
        console.log('socket data')
        for(var i = 0; i < data.length; i++) {
          if(i % 16 == 0) {
            process.stdout.write('\n')
          }
          process.stdout.write('0x')
          process.stdout.write(hex(data[i]))
          process.stdout.write(', ')
        }
      })
      var req = new Metadata('br0ker', ['testing!'])
      req.correlationId = 9
      client.send(req)
    }))
  })
})
