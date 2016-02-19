import OffsetRequest from '../lib/request/offset-request'
import send from './send'

const offset = async (topic, partition, time) => {
  const req = new OffsetRequest()
  partition = partition || 1
  time = time || -2
  const max = 100
  req.add(topic, partition, time, max)
  const res = (await send(req)).readOffsetResponse()
  console.log(res.topics[0].partitions)
}

offset('test').then(x => console.log('done'))
  .catch(e => setImmediate(() => { throw e; }))
