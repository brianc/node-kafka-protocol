import { connect } from 'net'
import Parser from '../lib/parse-stream'
import Promise from 'bluebird'

export const dump = buffer => {
  const arr = []
  for(let i = 0; i < buffer.length; i++) {
    const num = buffer[i].toString(16)
    const str = `0x${num.length < 2 ? '0' : ''}${num}`
    arr.push(str)
  }
  arr.forEach(function(byte, i) {
    if ((i % 8) == 0) {
      console.log()
    }
    process.stdout.write(byte)
    process.stdout.write(', ')
  })
  console.log()
}

export default (message, cb) => {
  const opts = { host: 'localhost', port: 9092 }
  return new Promise((resolve, reject) => {
    const socket = connect(opts, () => {
      console.log('connected to', opts)

      socket.on('data', buff => {
        console.log('socket data')
        if (process.env.DEBUG) {
          dump(buff)
        }
      })

      if (process.env.DEBUG) {
        console.log('sending message: CID', message.correlationId)
        dump(message.toBuffer())
      }
      socket.write(message.toBuffer())

      const parser = new Parser()

      socket.pipe(parser).on('data', data => {
        if (cb) {
          cb(null, data)
        }
        resolve(data)
        socket.end()
      })
    })
  })
}
