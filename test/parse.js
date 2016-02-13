import Promise from 'bluebird'
import ParseStream from '../lib/parse-stream'
export default buffer => new Promise((resolve, reject) => {
  const stream = new ParseStream()
  const messages = []
  stream.on('data', msg => {
    messages.push(msg)
  })
  stream.on('end', () => setImmediate(() => resolve(messages)))
  stream.write(buffer)
  stream.end()
})

