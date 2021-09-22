const debug = require('debug')('mq:stream')


const defaultOptReidsStream = {
  // blockPrivy : null,  // true:启用redisReadBlock
  maxlen: 100000, // stream maxlen
  sKey: 'mmQueueStream', // stream key
  gKey: 'mmQueueStreamGroup',// group name
  cKey: 'mmQueueStreamGroupConsumer', // consumer name
}

class RedisStream {
  redisRead // redis client
  redisVersion  // '6.0.9'
  maxlen  // stream maxlen
  sKey  // stream key
  gKey // group name
  cKey // consumer name
  constructor(opt) {
    this._initialize(opt)
  }

  _initialize({ client, ...opt }) {
    Object.assign(this, defaultOptReidsStream, opt)

    if (!client) throw new Error('no IORedis client!')
    this.redisRead = client
    if (this.redisRead.serverInfo) this.redisVersion = this.redisRead.serverInfo.redis_version
    else {
      this.redisRead.info(res => {
        this.redisVersion = this.redisRead.serverInfo.redis_version
      })
    }
  }

  // 创建0长度的stream,group; 已有则 throw
  async initStreamAndGroup(stream, group) {
    return await this.createGroup(group, stream)
      .catch(err => {
        if (err.message.includes('BUSYGROUP')) return 'OK'
        console.log('create group err:%s\n', err.message, err.command)
      })
  }

  /**
   * 默认自动创建sKey,0长度, success: 'OK'
   * 已有gKey,throw 'BUSYGROUP Consumer Group name already exists'
   * @param {gKey} param0 组名，
   */
  createGroup(gKey, sKey = this.sKey, id = '$') {
    return this.updateGroup({ type: 'CREATE', gKey, sKey, id, mkstream: true })
  }

  updateGroup({ id = '$', cKey, type = 'CREATE', gKey = this.gKey, sKey = this.sKey, mkstream }) {
    const query = [type, sKey, gKey]
    if (type === 'CREATE' || type === 'SETID') query.push(id)
    else if (type === 'DELCONSUMER' && cKey) query.push(cKey)
    mkstream && query.push('MKSTREAM')
    return this.redisRead.xgroup(query)
  }

  /**
   * 取groups状态 [{}]
   * @param {string} stream channel-name
   */
  async getGroupsInfo(sKey = this.sKey) {
    let res = await this.redisRead.xinfo(['GROUPS', sKey])
    return res ? res.map(i => this.arr2obj(i)) : null
  }

  // 返回 success: 1
  deleteGroup(gKey, sKey = this.sKey) {
    return this.updateGroup({ type: 'DESTROY', gKey, sKey })
  }

  async getConsumersInfo(gKey = this.gKey, sKey = this.sKey) {
    let res = await this.redisRead.xinfo(['CONSUMERS', sKey, gKey])
    return res ? res.map(i => this.arr2obj(i)) : null
  }

  async getStreamInfo(sKey = this.sKey, full, count) {
    let res = await this.redisRead.exists(sKey)
    if (res !== 1) return null

    const query = ['STREAM', sKey]
    if (this.redisVersion >= '6.0.0') {
      full && query.push('FULL')
      count && query.push('COUNT', count)
    }
    res = await this.redisRead.xinfo(query)
    res = this.arr2obj(res)
    if (full && this.redisVersion >= '6.0.0') {
      res.entries = res.entries.map(i => this.arr2item(i))
      res.groups = res.groups.map(i => {
        let g = this.arr2obj(i)
        g.consumers = g.consumers.map(j => this.arr2obj(j))
        // g.pending = g.pending.map(j => this.arr2obj(j))
        return g
      })
    } else {
      res.firstEntry = this.arr2item(res.firstEntry)
      res.lastEntry = this.arr2item(res.lastEntry)
    }
    return res
  }

  //  id: 1609213179820,'1609213179820-0'都可以
  async getInfoById(id, sKey = this.sKey) {
    let res = await this.redisRead.xrange(sKey, id, id)
    // 没有 res =[]
    return res && res.length > 0 ? this.arr2item(res[0]) : null
  }

  delInfoById(id, sKey = this.sKey) {
    return this.redisRead.xdel([sKey, id])
  }

  getPending({ sKey, gKey, cKey, start = '-', end = '+', count = 20 } = {}) {
    return this.xpending({ start, end, count, sKey, gKey, cKey })
  }

  async xpending({ start, end, count, cKey, gKey = this.gKey, sKey = this.sKey, query = [] } = {}) {
    query.push(sKey, gKey)
    start && query.push(start, end, count)
    cKey && query.push(cKey)

    const res = await this.redisRead.xpending(query)
    if (start) return res.map(i => {
      const [id, consumer, deliveredTime, deliveredNum] = i
      return { id, consumer, deliveredTime, deliveredNum }
    })

    let [xPendingLen, startId, endId, consumers] = res
    consumers = consumers.map(i => ({ consumer: i[0], xPendingLen: i[1] }))
    return { xPendingLen, startId, endId, consumers }
  }

  async xrange({ sKey = this.sKey, start = '-', end = '+', count = 20 } = {}) {
    const query = [sKey, start, end]
    count && query.push('COUNT', count)
    const res = await this.redisRead.xrange(query)
    return res.map(i => this.arr2item(i))
  }

  async xrevrange({ sKey = this.sKey, start = '+', end = '-', count = 20 } = {}) {
    const query = [sKey, start, end]
    count && query.push('COUNT', count)
    const res = await this.redisRead.xrevrange(query)
    return res.map(i => this.arr2item(i))
  }

  // 可以传数组, '1609204444880'
  ack(id, gKey = this.gKey, sKey = this.sKey) {
    return this.xack({ id, gKey, sKey })
  }

  /**
   * 返回 确认的 数量 0,1...
   * @param {string|array} id /ids:[]
   * @param {string} group
   * @param {string} stream
   */
  xack({ id, ids, gKey = this.gKey, sKey = this.sKey }) {
    const query = [sKey, gKey]
    if (ids && Array.isArray(ids)) query.push(...id)
    else query.push(id)
    return this.redisRead.xack(query)
  }

  add(item, sKey = this.sKey,) {
    return this.xadd({ item, sKey, maxlen: this.maxlen })
  }

  /**
   * 追加新消息，返回 id:'1609213179820-0'
   * throw 'ERR Invalid stream id specified as stream command argument'
   * @param {object} param0
   */
  async xadd({ item, id = '*', maxlen, sKey = this.sKey, query = [] } = {}) {
    if (!item) return null
    query.push(sKey)
    maxlen && query.push('MAXLEN', '~', typeof +maxlen === 'number' ? maxlen : this.maxlen)
    query.push(id)
    Object.keys(item).forEach(i => query.push(i, item[i]))
    const res = await this.redisRead.xadd(query)
    return res
  }

  /**
   * 返回[{id,item}]
   * @param {object} param0
   * noAck: true, 自动 XACK
   * id '>' 接收从未传递给任何其他使用者的消息: 新消息！
   *    0|time  发送比此ID大的ID消息
   */
  async xreadGroup({ id = '>', count = 20, block, cKey = this.cKey, gKey = this.gKey, sKey = this.sKey, noAck, query = [] } = {}) {
    query.push('GROUP', gKey, cKey)
    count && query.push('COUNT', count)
    if (block !== undefined && block !== null && block !== '') {
      query.push('BLOCK', block)
    }
    noAck && query.push('NOACK')
    query.push('STREAMS', sKey, id)
    const res = await this.redisRead.xreadgroup(query)
    // [[id, arr]]
    return !res ? null : res[0][1].map((i) => this.arr2item(i))
  }

  async xread({ id = 0, count = 20, block, sKey = this.sKey, query = [] } = {}) {
    count && query.push('COUNT', count)
    if (block !== undefined && block !== null && block !== '') {
      query.push('BLOCK', block)
    }
    query.push('STREAMS', sKey, id)
    const res = await this.redisRead.xread(query)
    // [[id, arr]]
    return !res ? null : res[0][1].map((i) => this.arr2item(i))
  }

  /**
   * 返回流的长度 不存在的流返回 0
   * @param {string} stream channel-name
   */
  getStreamLen(sKey = this.sKey) {
    return this.redisRead.xlen(sKey)
  }

  toHump(str, sign = '_') {
    const re = new RegExp(`\\${sign}(\\w)`, 'g')
    return str.replace(re, (match, letter) => letter.toUpperCase())
  }

  arr2obj(arr) {
    // if (!arr) return ''
    const info = {}
    for (let idx = 0; idx < arr.length; idx += 2) {
      arr[idx] = this.toHump(arr[idx], '-')
      info[arr[idx]] = arr[idx + 1]
    }
    return info
  }

  arr2item([id, item]) {
    if (!id || !item) {
      console.log('*****  id,item err:', id, item)
      return id ? { id, item: null } : null
    }

    return { id, item: this.arr2obj(item) }
  }

  static init(opt) {
    return new RedisStream(opt)
  }
}

module.exports = { RedisStream }