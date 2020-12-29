const EventEmitter = require('events')
const IORedis = require('ioredis')
const { RedisStream } = require('./redis-stream')

const debug = require('debug')('pd123:sKey')


class StreamQueue {
  sKey  // stream key
  gKey // group name
  cKey // consumer name
  events
  stream  // redis-stream-api
  streamBlock // block redis client
  subWating = false // beging 
  constructor({ events = new EventEmitter(), ...config }) {
    this.events = events
    this._initialize(config)
  }

  async subcribe(cb) {
    if (!this.subWating) {
      this.waitTaskByGroupBlock()
      this.subWating = true
    }

    this.events.on(this.sKey, async (res) => {
      console.log('events res:', res)
      // debug('events res:', res)
      const [{ id, item }] = res
      id && cb && await cb({ id, item })
      this.ack(id)
    })
    // this.checkedPending(this.sKey)
  }

  async readGroupPendingList(id = 0, count = 100, cKey) {
    let res = await this.stream.xreadGroup({ id, count, cKey })
    return res
  }

  async getPendingList({ start = '-', end = '+', count = 100, cKey }) {
    //  [{ id, consumer, deliveredTime, deliveredNum }]
    let res = await this.stream.getPending({ start, end, count, cKey })
    return res
  }
  // 添加消息到队列
  addTask(info, sKey = this.sKey) {
    return this.stream.add(info, sKey)
  }

  getMsgById(id) {
    return this.stream.getInfoById(id)
  }

  ack(id, gKey = this.gKey, sKey = this.sKey) {
    return this.stream.ack(id, gKey, sKey)
  }
  // 阻塞式等待 消息
  async waitTaskByGroupBlock() {
    while (true) {
      const res = await this.blockReadGroup({ block: 0, id: '>', count: 1 })
        .catch(err => {
          console.log('block task err:', err)
          return null
        })
      console.log('block readGroup: ', res)
      res && this.emitMsg(res)
    }
  }

  emitMsg(data) {
    this.events.emit(this.sKey, data)
  }

  blockReadGroup({ id = '>', block = 0, count = 1 }) {
    // id:'>' 未读过(delivered)的新消息； block:0 一直阻塞等待
    return this.streamBlock.xreadGroup({ id, count, block, })
  }

  _initialize({ stream, streamBlock, sKey, gKey, cKey, client, }) {
    Object.assign(this, {
      stream, streamBlock, sKey, gKey, cKey, client,
    })
  }

  static async init({ sKey = STREAM_KEY, gKey = GROUP_NAME,
    cKey = CONSUMER_NAME, client, ...opt }) {
    if (!client || !(client instanceof IORedis)) throw new Error('no Ioredis client!')

    opt.stream = new RedisStream({ client, sKey, gKey, cKey })
    let res = await opt.stream.initStreamAndGroup(sKey, gKey)
      .catch(err => {
        if (err.message.includes('BUSYGROUP')) return 'OK'
        console.log('create group err:%s\n', err.message, err.command)
      })
    if (res !== 'OK') throw new Error('init stream err')
    opt.streamBlock = new RedisStream({ client: client.duplicate(), sKey, gKey, cKey })
    return new StreamQueue({ ...opt, sKey, gKey, cKey, client })
  }
}

module.exports = { StreamQueue }