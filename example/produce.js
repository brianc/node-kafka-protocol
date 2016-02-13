import ProduceRequest from '../lib/produce-request'
import send from './send'

const request = new ProduceRequest()
request.timeout = 1000
request.requiredAcks = 1
const keyString = process.argv[3] || 'key'
const key = Buffer(keyString, 'utf8')

const valString = process.argv[4] || 'value'
const value = Buffer(valString, 'utf8')

const times = parseInt(process.argv[5]) || 1

console.log('produce', keyString, valString)
for(var i = 0; i < times; i++) {
  request.add('test', 1, key, value)
}

send(request, (err, res) => {
  const prod = res.readProduceResponse()
  console.log(JSON.stringify(prod, null, 2))
})
