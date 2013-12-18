var net = require('net')

var requests = require('./lib/requests')
var MetadataRequest = requests.MetadataRequest

var con = net.connect(9092, function() {
  console.log('connected to server')
  var req = new MetadataRequest('node client', ['test'])
  con.write(req.toBuffer())
})

con.on('data', function(data) {
  console.log('server data')
  console.log(data)
})

con.on('error', function(e) {
  console.log('error')
  console.log(e)
})

con.on('end', function() {
  console.log('ended')
})
