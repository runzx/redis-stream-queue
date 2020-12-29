const EventEmitter = require('events')

const debug = require('debug')('pd123:stream')


class StreamQueue {
  stream  // redis-stream-api
  sKey  // stream key
  gKey // group name
  cKey // consumer name
  streamBlock // block redis client
  constructor({ events = new EventEmitter(

  ), ...config }) {
    this.events = events
    this.initConfig(config)
  }

  async readGroupPendingList(id = 0, count = 100) {
    let res = await this.streamModel.readGroup({ id, count })
    return res
  }

  async getPendingList({ start = '-', end = '+', count = 100 }) {
    //  [{ id, consumer, deliveredTime, deliveredNum }]
    let res = await this.streamModel.readPending({ start, end, count })
    return res
  }
  // 添加消息到队列
  addMsg(info, stream = this.stream) {
    return this.streamModel.addTask(info, stream)
  }

  getMsgById(id) {
    return this.streamModel.getInfoById(id)
  }

  xack(id, group = this.group) {
    return this.streamModel.xack(id, group, this.stream)
  }
  // 阻塞式等待 消息
  async waitTaskByGroupBlock() {
    while (true) {
      const res = await this.blockReadGroup({ block: 0, id: '>', count: 1 })
        .catch(err => {
          console.log('block task err:', err)
          return null
        })
      // if (!res) continue
      // console.log('block task res:', res)
      res && this.emitMsg(res)
    }
  }

  emitMsg(data) {
    this.events.emit(this.stream, data)
  }

  blockReadGroup({ id = '>', block = 0, count = 1 }) {
    // id:'>' 未读过(delivered)的新消息； block:0 一直阻塞等待
    return this.streamBlockModel.readByGroup({ id, count, block, })
  }

  initConfig(opt) {
    const { stream, sKey, gKey, cKey, client,
    } = opt
    Object.assign(this, {
      stream, sKey, gKey, cKey, client,
    })
  }

  static async init({ stream = STREAM_KEY, group = GROUP_NAME,
    consumer = CONSUMER_NAME, client, ...opt }) {
    if (!client || !(client instanceof Ioredis)) throw new Error('no Ioredis client!')

    opt.streamModel = new RedisStream({ client, stream, group, consumer })
    let res = await opt.streamModel.initStreamAndGroup(stream, group)
      .catch(err => {
        if (err.message.includes('BUSYGROUP')) return 'OK'
        console.log('create group err:%s\n', err.message, err.command)
      })
    if (res !== 'OK') throw new Error('init stream err')
    opt.streamBlockModel = new RedisStream({ client: client.duplicate(), stream, group, consumer })
    return new StreamQueue({ ...opt, stream, group, consumer, client })
  }
}

module.exports = { StreamQueue }