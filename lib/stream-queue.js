const EventEmitter = require('events')
const IORedis = require('ioredis')
const { RedisStream } = require('./redis-stream')

const debug = require('debug')('mq:queue')

const STREAM_KEY = 'ZxQueueDelay'
const GROUP_NAME = 'ZxQueueGroup'
const CONSUMER_NAME = 'ZxQueueGroupConsumer'
const STREAM_DELAY_KEY = 'mmQueueDelay'
const PREFIX_DELAY_KEY = 'ZX_PUB_EX'

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
      // console.log('events res:', res)
      debug('stream events res:', res)
      const [{ id, item }] = res
      id && cb && await cb({ id, item })
      let ack = await this.ack(id)
      debug('stream events ack: ', ack)
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
      // debug('block readGroup: ', res)
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

  delInfoById(id, sKey) {
    return this.stream.delInfoById(id, sKey)
  }

  _initialize({ stream, streamBlock, sKey, gKey, cKey, client, }) {
    Object.assign(this, {
      stream, streamBlock, sKey, gKey, cKey, client,
    })
  }

  static async init({ sKey = STREAM_KEY, gKey = GROUP_NAME,
    cKey = CONSUMER_NAME, client, ...opt }) {
    if (!client || !(client instanceof IORedis)) throw new Error('no IORedis client!')

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

const defaultOptDelayStream = {
  // blockPrivy : null,  // true:启用redisReadBlock
  maxlen: 100000, // stream maxlen
  sKey: 'mmDelayStream', // stream key
  gKey: 'mmDelayStreamGroup',// group name
  cKey: 'mmDelayStreamGroupConsumer', // consumer name
}
// 只 延时 ,转入相应队列
class DelayQueue {
  client  // write
  clientSub // publish/subscribe
  sKey  // 流key， 延迟队列 skey
  delayStream // 延迟队列 专用流队列， 共用
  intervalTime = 60 * 1000 // 5m    5 * 60 * 1000 
  intervalId  // 定时id
  constructor(opt) {
    this._initialize(opt)
  }
  addTask(streamKey, info, ttl) {
    debug('task :', streamKey, info)
    const stored = +new Date()
    const expire = stored + ttl * 1000
    return this.delayStream.addTask({ streamKey, ttl, stored, expire, ...this.preData(info) })
  }

  findExpireed(list) {
    const now = +new Date()
    let res = list.filter(i => !i.item || !i.item.expire || i.item.expire < now)
      .map(({ id, item }) => {
        if (!item) return { id, }
        const { streamKey, ttl, stored, expire, ...info } = item
        return { id, streamKey, expire, info }
      })
    return res
  }
  // 定时 检查 pending表
  service() {
    this.intervalId = setInterval(() => {
      this.checkedExMsg()
    }, this.intervalTime)
  }
  async checkedExMsg() {
    let ID = 0
    const count = 100
    while (true) {
      let list = await this.delayStream.readGroupPendingList(ID, count)
      const len = list.length
      if (!list || len === 0) break
      ID = list[len - 1].id
      console.log('checked expire list ,list len:%d, next id:%s', ID, len)
      list = this.findExpireed(list)  // 重试次数<deliveredNumMax 
      list.forEach((res) => {
        this.postDelayMsg(res)
      })
    }
  }

  preData(data) {
    if (typeof data !== 'object') data = { data }
    if (Array.isArray(data)) data = data.reduce((acc, cur, idx) => {
      acc[`arrIdx:${idx}`] = cur
      return acc
    }, {})
    return data
  }
  getKey(streamKey, id) {
    return `${PREFIX_DELAY_KEY}:${streamKey}:${id}`
  }

  async getInfoById(id) {
    let res = await this.delayStream.getMsgById(id)
    if (!res || !res.item) {
      console.log('delayQueue getTaskById err, id:', id, res)
      return
    }
    const { item } = res
    const { streamKey, ttl, stored, expire, ...info } = item
    return { streamKey, info, expire, id }
  }

  pubExEvent(client, db) {
    this.clientSub = client.duplicate()
    this.clientSub.psubscribe(`__keyevent@${db}__:expired`)
  }
  // 到期事件 订阅
  exEventSub(cb) {
    this.clientSub.on('pmessage', (pattern, channel, message) => {
      debug('channel:', channel)
      cb && cb(message)
    })
  }
  streamDelaySubInit() {
    const stream = this.delayStream
    if (!stream.subWating) {
      stream.waitTaskByGroupBlock()
      stream.subWating = true
    }

    stream.events.on(this.sKey, async (res) => {
      const [{ id, item }] = res
      if (!id) {
        console.log('delay task events err:', res)
        return
      }
      const { streamKey, ttl, stored, expire, ...info } = item
      const key = this.getKey(streamKey, id)
      let res1 = await this.client.set(key, 1, "EX", ttl)
      if (res1 !== 'OK')
        debug('setEx res:', res1, res,)
    })

    this.checkedExMsg()
  }
  // 到期消息 预处理
  async preExMsg(message) {
    debug('ex message:', message)
    const [prefix, sKey, id] = message.split(':')
    const { streamKey, info } = await this.getInfoById(id)
    if (sKey !== streamKey || !id) {
      console.log('delayQueue pub err: ', id, sKey)
      return null
    }
    await this.postDelayMsg({ id, streamKey, info })
  }
  // 取出延时队列里的消息 -> 正常 队列
  async postDelayMsg({ id, streamKey, info }) {
    if (!id || !streamKey || !info) {
      id && this.delayStream.ack(id)
      id && this.delayStream.delInfoById(id)
      return console.log('err info:', id, streamKey, info)
    }

    let res = await this.delayStream.addTask(info, streamKey)
    if (!res) return console.log('addTask err:', id, info, streamKey)
    else debug('delay ok id:%s -> %s', id, res)
    res = await this.delayStream.ack(id)
    if (res !== 1) console.log('ack err:', res, id)
  }
  start() {
    this.pubExEvent(this.client, this.db)
    this.streamDelaySubInit()

    this.exEventSub(res => {
      this.preExMsg(res)  // 把this传入preExMsg
    })

    // this.service()
    this.checkedExMsg()
  }

  _initialize({ client, events, sKey, delayStream, db,
    intervalTime, gKey, cKey }) {
    Object.assign(this, defaultOptDelayStream, {
      client, events, sKey, delayStream, db,
      intervalTime, gKey, cKey
    })

  }

  static async init({ client, events = new EventEmitter(),
    db = 5, sKey = STREAM_DELAY_KEY }) {

    if (!client || !(client instanceof IORedis)) throw new Error('no IORedis client!')
    const c = client.duplicate()
    await c.select(db)

    // sKey = `${PREFIX_DELAY_KEY}:${sKey}`
    const delayStream = await StreamQueue.init({
      sKey, client, events
    })
    // delayStream.waitTaskByGroupBlock()
    return new DelayQueue({ client: c, db, delayStream, events, sKey })
  }
}

class RedisQueue {
  client
  subscriptions = {}
  delayQueue
  intervalTime = 2 * 60 * 1000 // 5m    5 * 60 * 1000 
  intervalId  // 定时id
  deliveredNumMax = 5
  constructor(opt) {
    this.events = new EventEmitter()
    this._initialize(opt)
  }

  async subcribe(streamKey, cb) {
    if (!this.subscriptions[streamKey]) {
      this.subscriptions[streamKey] = await StreamQueue.init({
        stream: streamKey, client: this.client,
        events: this.events
      })
      // this.subscriptions[streamKey].waitTaskByGroupBlock(this.events)
    }

    this.subscriptions[streamKey].subcribe(cb)

    this.checkedPending(streamKey)
  }

  addTask(streamKey, info, ttl) {
    if (ttl && +ttl > 0) return this.addDelayTask(streamKey, info, ttl)
    if (!this.subscriptions[streamKey]) return console.log('no Init stream & subcribe! ', streamKey)
    return this.subscriptions[streamKey].addMsg(this.preData(info))
  }

  async addDelayTask(streamKey, info, ttl = 3) {
    if (!ttl && +ttl <= 0) return console.log('delay ttl must > 0s: ', ttl)
    if (!this.delayQueue) await this.preDelay()

    this.delayQueue.addTask(streamKey, info, ttl,)
  }

  async preDelay() {
    this.delayQueue = await DelayQueue.init({
      client: this.client, db: 5, sKey: STREAM_DELAY_KEY,
    })
  }
  _initialize({ intervalTime, client }) {
    this.client = client
    intervalTime && (this.intervalTime = intervalTime)
    // this.service()
    this.checkedPending()
  }
  service() {
    this.intervalId = setInterval(() => {
      Object.keys(this.subscriptions).forEach(stream => {
        this.checkedPending(stream)
      })

    }, this.intervalTime)
  }
  async checkedPending(stream) {
    const streamModel = this.subscriptions[stream]
    let start = '-', end = '+', ID = 0
    const count = 100
    while (true) {
      let pendingList = await streamModel.getPendingList({ start, end, count })
      let list = await streamModel.readGroupPendingList(ID, count)
      const len = list.length
      if (!list || len === 0) break
      ID = list[len - 1].id
      start = pendingList[len - 1].id
      console.log('next id:%s ,list len:%d', ID, len)
      let res = this.retryTask(list, pendingList, streamModel)  // 过期表 [{streamKey,info}]

    }
  }
  retryTask(list, pendingList, streamModel) {
    let res = pendingList.filter(({ deliveredNum }) => deliveredNum > this.deliveredNumMax)
    console.log('deliveredNum > %d: \n', this.deliveredNumMax, res)
    res.forEach(({ id }) => {
      streamModel.xack(id)  // xack 移出pending表，不再重试
    })
    res = pendingList.filter(({ deliveredNum }) => deliveredNum <= this.deliveredNumMax)
    res.forEach(({ id }) => {
      const msg = list.find(i => i.id === id)
      msg && streamModel.emitMsg([msg])
    })
    return res
  }

  static init(opt) {
    const { client, } = opt
    if (!client || !(client instanceof Ioredis)) throw new Error('no Ioredis client!')
    return new RedisQueue(opt)
  }
}


module.exports = { StreamQueue, DelayQueue }