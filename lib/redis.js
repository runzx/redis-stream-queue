
const Ioredis = require('ioredis')
const { objToArr } = require('./util')
const debug = require('debug')('redis')

class RedisBase {
  client    // redis
  opt       // setting
  constructor(opt = {}) {
    this.opt = opt
    this._init(opt)
  }
  _init(opt) {
    const { host = '127.0.0.1', port = 6379, password = '', db = 3 } = opt
    const client = new Ioredis({ host, port, password, db })

    client.on('connect', () => console.log('redis was connected!'))
    client.on('disconnect', () => console.log('redis was disconnected!'))
    client.on('error', (err) => console.log('redis err: ', err))
    this.client = client
    Ioredis.Command.setArgumentTransformer('xadd', (args) => {
      if (args.length === 3) {
        const [sKey, id, msg] = args
        if (typeof msg === 'object' && id !== null)
          return [sKey, id, ...objToArr(msg)]
      } else if (args.length === 5) {
        const [sKey, maxlen, len, id, msg] = args
        if (maxlen.toLocaleLowerCase() === 'maxlen' && typeof +len === 'number' && typeof msg === 'object' && id !== null)
          return [sKey, maxlen, len, id, ...objToArr(msg)]
      }
      return args
    })
  }
  async start() {
    const info = await this.client.info()
    if (this.client.serverInfo.redis_version < '5.0') {
      throw new Error('redis version need V5+')
    }
  }
}


module.exports = { RedisBase }