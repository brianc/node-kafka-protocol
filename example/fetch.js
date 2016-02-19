import FetchRequest from '../lib/request/fetch-request'
import send, { dump } from './send'

const fetch = async (topic, offset = 0) => {
  const req = new FetchRequest()
  const maxBytes = 1024
  req.minBytes = 1
  req.maxWaitTime = 100
  req.add(topic, 1, offset, maxBytes)

  const response = await send(req)
  console.log(req._topics[0])
  const msg = response.readFetchResponse()
  console.log()
  console.log('--response--')
  console.log()
  console.log(msg.topics[0])
  console.log(msg.topics[0].partitions[0].messageSet)

  dump(response._buffer)
}


const topic = process.argv[3]
const offset = process.argv[4]
fetch(topic, offset).then(x => console.log('done?'))
  .catch(e => setImmediate(() => { throw e; }))
