// const { RedisBase } = require("../../lib/redis-base")
const { RedisBase } = require("../../lib/redis")
const { objToArr, arr2obj, arr2item } = require("../../lib/util")


let res
let redis = new RedisBase({ password: 'zx2962', sKey: 'abc', gKey: 'bosstg', cKey: 'zx' })
// redis.client.xadd('abc', 'maxlen', 5, '*', { zx: 'zhaix', b: 58 })
redis.client.xadd('abc', '*', { a: 4, b: 2 })
// objToArr({ b: 'abc', c: { a: 'a' }, a: 1, })
// redis.creatGroup('abc', 'bosstg').then()
// redis.getStreamInfo('abc').then(res => {
//   console.log('res:', res)
// })

// redis.getGroupInfo('abc').then(res => {
//   console.log('res:', res)
// })

// redis.getConsumersInfo('abc', 'bosstg').then(res => {
//   console.log('res:', res)
// })

redis.getMsgByConsumer('zx', 2).then(res => {
  console.log('res:', res)
})



/* 
res = [
  [
    "abc",
    [
      [
        "1632399001376-0",
        [
          "zx",
          "zhaix",
          "b",
          "58",
        ],
      ],
    ],
  ],
]

let [sKey, tmp] = res[0]
let list = tmp.map(i => arr2item(i)) */
console.log('res:',)