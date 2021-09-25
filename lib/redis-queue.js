const EventEmitter = require('events')
const { RedisBase } = require("./redis")
const debug = require('debug')('redis:queue')

const GROUP_NAME = 'mmQueueGroup'
const CONSUMER_NAME = 'mmQueueConsumer'

// const STREAM_DELAY_KEY = 'mmQueueDelay'

class RedisQueue extends RedisBase {
  events  // 
  subWating = null  // true 已在阻塞式等待
  constructor(opt) {
    super(opt)

    this._initRedisQueue(opt)
  }

  _initRedisQueue(opt) {
    const { sKey, gKey = GROUP_NAME, cKey = CONSUMER_NAME, events = new EventEmitter() } = opt
    if (!sKey) throw new Error('没有队列名称:sKey')
    this.gKey = gKey
    this.cKey = cKey
    this.events = events

  }

  async start() {
    await super.start()
  }

  /**
   * 创建 队列 的 任务
   * @param {function} cb
   */
  async subcribe(cb) {
    if (!this.subWating) {
      this.waitTaskByGroupBlock()
      this.subWating = true
    }

    this.events.on(this.sKey, async (msg) => {
      // console.log('events res:', res)
      debug('stream events res: %O', msg)
      const { id, item } = msg
      try {
        id && cb && await cb(msg)
        let ack = await this.xack(id)
        debug('stream events ack: ', ack)
      } catch (error) {
        console.log('MQ err: %s, msg:%O', error.message, msg)
      }

    })
    // this.checkedPending(this.sKey)
  }

  // 添加消息到队列
  addTask(msg) {
    return this.sendMsg(msg)
  }

  /**
   * 阻塞式等待 消息, 一次只取一条消息
   * @param {string} cKey
   */
  async waitTaskByGroupBlock(cKey) {
    while (true) {
      const res = await this.getMsgByConsumerBlock(cKey, 1)
        .catch(err => {
          console.log('block task err:', err)
          return null
        })
      debug('block readGroup: %O', res)
      res && res.length === 1 && this.emitMsg(res[0])
    }
  }

  emitMsg(data) {
    this.events.emit(this.sKey, data)
  }
}


const PREFIX_DELAY_KEY = 'MM_PUB_EX'


/**
 * 延时队列
 * ttl 秒
 * 2队列+到期事件 保证不丢失数据，延时
 * 延时队列 sKey = `${sKey}:DELAY_STREAM`
 * 延时队列item:{...data, expiresZxAt}  expiresZxAt: 期满时间 ms,  data: key=val键值对
 * 到期事件 key= `${PREFIX_DELAY_KEY}:${sKey}:${id}`
 */
class DelayQueue extends RedisQueue {
  clientSub   // 定时专用
  clientSubWrite  // 定时专用
  delayStream // 延迟队列 专用
  intervalTime = 1000 * 60 * 60 //  
  intervalId  // 定时id
  exEventDb = 5

  constructor(opt) {
    super(opt)
    this._init(opt)
  }

  _init(opt) {
    const { exEventDb, intervalTime } = opt
    exEventDb && (this.exEventDb = exEventDb)
    intervalTime && (this.intervalTime = intervalTime)
    this.opt = opt
    // this.start()
  }

  /**
   * 新加任务(消息)
   * ttl,expiresZxAt不为空，则 延时处理.
   * expiresZxAt 优先于 ttl
   * @param {object} msg 消息(key,val对集合)
   * @param {number} ttl 延时 s
   * @param {number} expiresZxAt 预定时间 ms,
   * @returns
   */
  addTask(msg, ttl, expiresZxAt) {
    if (ttl || expiresZxAt) {
      if (!expiresZxAt) expiresZxAt = +new Date() + ttl * 1000
      msg.expiresZxAt = expiresZxAt
      return this.delayStream.addTask(msg)
    }
    return super.addTask(msg)
  }

  async pubExEvent(client = this.client, db = this.exEventDb) {
    this.clientSub = client.duplicate()
    await this.clientSub.select(db)
    this.clientSubWrite = this.clientSub.duplicate()
    await this.clientSubWrite.select(db)
    this.clientSub.psubscribe(`__keyevent@${db}__:expired`)
  }

  // 到期事件 订阅
  exEventSub(cb) {
    this.clientSub.on('pmessage', (pattern, channel, message) => {
      debug('channel:', channel)
      cb && cb(message)
    })
  }

  async start() {
    await super.start()
    this.pubExEvent()
    await this.delaySubSubcribe()

    this.exEventSub(res => {
      this.preExMsg(res)  // 把this传入preExMsg
    })
    // this.checkedExMsg()
    // this.service()
  }

  /**
   * 延时队列 特殊 订阅
   * xack留到
   */
  async delaySubSubcribe() {
    let stream = this.delayStream
    if (!stream) {
      this.delayStream = stream = new RedisQueue({ ...this.opt, sKey: this.sKey + ':DELAY_STREAM', maxlen: 8 })
      await stream.start()
    }
    if (!stream.subWating) {
      stream.waitTaskByGroupBlock()
      stream.subWating = true
    }

    stream.events.on(stream.sKey, async (msg) => {
      const { id, item } = msg
      if (!id) return console.log('delay task events err:', msg)

      let res = await this.setDataTTL(id, item.expiresZxAt)
      //TODO 已过期处理， 直接送sKey队列？？
      // if (res && res.includes('TTL:')) { }
    })

    // this.checkedExMsg()
  }

  /**
   *
   * @param {string} id 延时队列 的 消息id
   * @param {number} expiresZxAt 预定时间戳(ms)
   * @returns
   */
  async setDataTTL(id, expiresZxAt) {
    const key = this.genKey(id)
    const ttl = ((+expiresZxAt - new Date()) / 1000).toFixed(0)
    if (ttl <= 0) {
      debug('setEx ttl err: %d, key: %s', ttl, key)
      return 'TTL:' + ttl
    }
    let res = await this.clientSubWrite.set(key, 1, "EX", ttl)
    if (res !== 'OK')
      debug('setEx err: %s', res)
    return res
  }

  // 到期消息 预处理
  async preExMsg(message) {
    debug('ex message: %O', message)
    const [prefix, sKey, id] = message.split(':')
    if (sKey !== this.sKey) return  // 其它 队列的

    const item = await this.delayStream.getMsgById(id)
    await this.postDelayMsg(id, item)
  }

  // 延时队列里的消息 -> 正常 队列
  async postDelayMsg(id, msg) {
    let res = await this.addTask(msg)
    if (!res) return console.log('postDelayMsg err:', id, msg)

    let ack = await this.delayStream.xack(id)
    if (ack !== 1) console.log('ack err : ', id, this.sKey)
    else this.delayStream.delMsgById(id)
  }

  genKey(id, sKey = this.sKey,) {
    return `${PREFIX_DELAY_KEY}:${sKey}:${id}`
  }
}

module.exports = { RedisQueue, DelayQueue }