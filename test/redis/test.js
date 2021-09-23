// const { RedisBase } = require("../../lib/redis-base")
const { RedisBase } = require("../../lib/redis")
const { objToArr } = require("../../lib/util")


let redis = new RedisBase({ password: 'zx2962' })
redis.client.xadd('abc', 'maxlen', 5, '*', { zx: 'zhaix', b: 58 })
// redis.client.xadd('abc', '*', { a: 1, b: 2 })
// objToArr({ b: 'abc', c: { a: 'a' }, a: 1, })

console.log('res:',)