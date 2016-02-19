import MetadataRequest from '../lib/request/metadata-request'
import send from './send'

send(new MetadataRequest(), (err, res) => {
  console.log(res.readMetadataResponse())
})
