import Promise from 'bluebird'
import { connect } from 'net'

import {
  MetadataRequest,
  ProduceRequest
} from '../request'

import Parser from '../parse-stream'

class Signal {
  constructor(request) {
    this._request = request
    this._resolve = null
    this.reject = null
    this.promise = new Promise((resolve, reject) => {
      this._resolve = resolve
      this._reject = reject
    })
  }

  _read(response) {
    switch(this._request.apiKey) {
      case 3:
        return response.readMetadataResponse()
      case 0:
        return response.readProduceResponse()
      default:
        throw new Error('Unknown api response key ' + this._request.apiKey)
    }
  }

  read(response) {
    this._resolve(this._read(response))
  }
}

export default class Client {
  constructor(id) {
    this.id = id
    this.socket = null
    this._cid = 1
    this._pending = []
  }

  connect(host, port) {
    return new Promise((resolve, reject) => {
      const opts = { host, port }
      this.socket = connect(opts, resolve)
      const parser = new Parser()
      this.socket.pipe(parser).on('data', this._onData.bind(this))
    })
  }

  end() {
    return new Promise((resolve, reject) => {
      this.socket.end(resolve)
    })
  }

  send(request) {
    this._cid = this._cid + 1
    request.correlationId = this._cid
    const signal = new Signal(request)
    this.socket.write(request.toBuffer())
    this._pending.push(signal)
    return signal.promise
  }

  metadata(topics) {
    const request = new MetadataRequest(this.id, topics)
    return this.send(request)
  }

  produce(topic, partition, key, value, options) {
    const request = new ProduceRequest(this.id)
    if (options) {
      request.requiredAcks = options.requiredAcks
      request.timeout = options.timeout
    }
    request.add(topic, partition, key, value)

    //we do not wait for a callback if requiredAcks is 0
    if (!request.requiredAcks) {
      this.socket.write(request.toBuffer())
      return Promise.resolve(true)
    }
    return this.send(request)
  }

  _onData(buffer) {
    this._pending.pop().read(buffer)
  }
}
