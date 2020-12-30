const IORedis = require("ioredis")

class RedisBase {
  client  // master connect
  constructor(opt = {}) {
    this.config = {
      host: '127.0.0.1',
      port: 6379,
      db: 0,
      password: '',
      ...opt
    }
    this.client = opt.client || new IORedis(this.config)
  }
  async serverInfo() {
    const res = await this.client.info()
    const { redis_version, connected_clients, blocked_clients, used_memory,
      pubsub_channels, pubsub_patterns } = this.client.serverInfo
    const dbs = {}
    for (let index = 0; index < 16; index++) {
      const db = this.client.serverInfo['db' + index]
      if (db) {
        dbs['db' + index] = db.split(',').reduce((acc, cur) => {
          const [key, value] = cur.split('=')
          acc[key] = +value
          return acc
        }, {})
      }
    }
    return [this.client.serverInfo, dbs, res]
  }
  // 不能联接直接 throw,不再重试20+次
  restart(opt) {
    this.client.disconnect()

    return new Promise((resolve, reject) => {
      this.client = new IORedis(opt)
      this.client.on('error', err => reject(err))
      this.client.on('connect', res => resolve(res))
    })
  }
  async getConnectInfo(obj) {
    const msg = await this.client('list')
    let res = msg.split('\n')
      .filter(i => i)
      .map(i => i.split(' ')
        .reduce((acc, j) => {
          let [key, value] = j.split('=')
          acc[key] = value
          return acc
        }, {}))
      .sort((a, b) => (a.id - b.id))
    if (obj) return res
    // return { id, age, idle, psub, sub, cmd }
    return res.map(k => ['id', 'age', 'idle', 'psub', 'sub', 'cmd']
      .reduce((acc, i) => (acc += [i, k[i]].join(':') + ' ', acc), ''))
  }
}

module.exports = { RedisBase }