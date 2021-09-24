// const { RedisBase } = require("../../lib/redis-base")
const { RedisBase } = require("../../lib/redis")
const { objToArr, arr2obj, arr2item } = require("../../lib/util")


let res
let redis = new RedisBase({ password: 'zx2962', sKey: 'abc', gKey: 'bosstg', cKey: 'zx' })

redis.sendMsg({ i: '16323987503954', data: 158 }).then(res => {
  console.log('msgId:', res)
})

// redis.getMsgById('16323987503954').then(res => {
//   console.log('msg:', res)
// })

// redis.getMsgByConsumer('mm').then(res => {
//   console.log('res:', res)
// }, err => { console.log('err:', err) })
// redis.xack('1632398750394-0').then(res => {
//   console.log('xack:', res)
// })
// redis.getPEL().then(res => {
//   console.log('res:', res)
// })


// redis.client.xadd('abc', 'maxlen', 5, '*', { zx: 'zhaix', b: 58 })
// redis.client.xadd('abc', '*', { a: 4, b: 2 })
// objToArr({ b: 'abc', c: { a: 'a' }, a: 1, })
// redis.creatGroup('abc', 'bosstg').then()

// redis.getStreamInfo('ab', ).then(res => {
// })

// redis.getGroupInfo('abc').then(res => {
//   console.log('res:', res)
// })

// redis.getConsumersInfo().then(res => {
//   console.log('res:', res)
// }, err => {
//   console.log('err:', err)

// })

// redis.getMsgByConsumer('zx', ).then(res => {
//   console.log('res:', res)
// })



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