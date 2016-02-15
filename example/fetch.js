import FetchRequest from '../lib/request/fetch-request'
import send from './send'

const fetch = async (topic, offset = 0) => {
  const req = new FetchRequest()
  const maxBytes = 1024
  req.minBytes = 1
  req.maxWaitTime = 100
  req.add(topic, 1, offset, maxBytes)

  const response = await send(req)
  console.log('response', response)
}

fetch('test').then(x => console.log('done?'))
  .catch(e => setImmediate(() => { throw e; }))
