import MetadataRequest from '../lib/metadata-request'
import send from './send'

send(new MetadataRequest(), (err, res) => {
  console.log(res.readMetadataResponse())
})
