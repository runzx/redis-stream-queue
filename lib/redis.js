
const Ioredis = require('ioredis')
const { objToArr } = require('./util')
const debug = require('debug')('redis:stream')

const toHump = (str, sign = '_') => {
  const re = new RegExp(`\\${sign}(\\w)`, 'g')
  return str.replace(re, (match, letter) => letter.toUpperCase())
}

const arr2obj = (arr) => {
  // if (!arr) return ''
  const info = {}
  for (let idx = 0; idx < arr.length; idx += 2) {
    arr[idx] = toHump(arr[idx], '-')
    info[arr[idx]] = arr[idx + 1]
  }
  return info
}

const arr2item = ([id, item]) => {
  if (!id || !item) {
    console.log('*****  id,item err:', id, item)
    return id ? { id, item: null } : null
  }

  return { id, item: arr2obj(item) }
}

class RedisBase {
  client    // redis
  opt       // setting
  sKey      // stream
  gKey      // group
  cKey      // consumer
  constructor(opt = {}) {
    this.opt = opt
    this._init(opt)
  }
  _init(opt) {
    let { client, host = '127.0.0.1', port = 6379, password = '', db = 3,
      sKey, gKey, cKey } = opt
    if (!client) client = new Ioredis({ host, port, password, db })
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
    sKey && (this.sKey = sKey)
    gKey && (this.gKey = gKey)
    cKey && (this.cKey = cKey)
  }
  async start() {
    const info = await this.client.info()
    if (this.client.serverInfo.redis_version < '5.0') {
      throw new Error('redis version need V5+')
    }
  }

  async creatGroup(gKey = this.gKey, sKey = this.sKey,) {
    try {
      // const key = await this.client.exists(sKey)
      // if (key === 0)
      await this.client.xgroup('create', sKey, gKey, 0, 'MKSTREAM')  // 'MKSTREAM'没有流则自动创建0长度流
    } catch (err) {
      console.log('creatGroup err:', err)
    }
  }

  /**
   * 组内消费 取消息
   * @param {string} cKey 
   * @param {number} count 1
   * @returns {[{id,item}]}
   */
  async getMsgByConsumer(cKey = this.cKey, count = 1) {
    let res = await this.client.xreadgroup('GROUP', this.gKey, cKey, 'COUNT', count, 'STREAMS', this.sKey, '>')
    debug('getMsgByConsumer: %j', res)
    if (!res) return res
    let [sKey, tmp] = res[0]
    return tmp.map(i => arr2item(i))
  }

  async getStreamInfo(sKey = this.sKey) {
    let res = await this.client.xinfo('STREAM', sKey)
    res = arr2obj(res)
    res.firstEntry = arr2item(res.firstEntry)
    res.lastEntry = arr2item(res.lastEntry)
    return res
  }

  async getGroupInfo(sKey = this.sKey) {
    let res = await this.client.xinfo('GROUPS', sKey)
    console.log('info:', res)
    // res = arr2obj(res)
    // res.firstEntry = arr2item(res.firstEntry)
    // res.lastEntry = arr2item(res.lastEntry)
    return res.map(i => arr2obj(i))
  }

  async getConsumersInfo(gKey = this.gKey, sKey = this.sKey,) {
    let res = await this.client.xinfo('CONSUMERS', sKey, gKey)
    debug('info:', res)

    return res.map(i => arr2obj(i))
  }
}


module.exports = { RedisBase }