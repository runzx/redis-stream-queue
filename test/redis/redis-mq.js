const IORedis = require("ioredis")
const { RedisStream } = require("../../lib/redis-stream")
const { StreamQueue, DelayQueue, RedisQueue } = require("../../lib/stream-queue")
const { generateRandom } = require("../../lib/util")
const redis = new IORedis()

const debug = require('debug')('mq:test')


// const STREAM_KEY = 'streamDemoKey'
// const GROUP_NAME = 'streamDemoGroup'
// const CONSUMER_NAME = 'streamDemoConsumer'
// ZxQueue
const STREAM_KEY = 'ZxQueueDelay'
const GROUP_NAME = 'ZxQueueGroup'
const CONSUMER_NAME = 'ZxQueueGroupConsumer'
const STREAM_DELAY_KEY = 'ZxQueueDelay'
const PREFIX_DELAY_KEY = 'ZX_PUB_EX'

const f = async () => {
  const mq = await RedisQueue.init({
    client: redis,
    // sKey: STREAM_DELAY_KEY,
    // sKey: STREAM_KEY,
    // gKey: GROUP_NAME, cKey: CONSUMER_NAME
  })

  let res, res1, res2

  res = await mq.subcribe('streamDemoKey', res => {
    console.log('res:', res)
  })

  // console.log('res:', res)
  const no = generateRandom(6)
  res = await mq.addTask('streamDemoKey', { no }, 6)
  debug('start:', res)
  return
  res = await mq.getConsumersInfo()
  console.log('res:', res)
  res = await mq.add({ no: 2 })
  console.log('res:', res)
  res = await mq.getPending()
  console.log('res:', res)
  res = await mq.getInfoById('1609213179820-0')
  console.log('res:', res)
  res = await mq.xrevrange()
  console.log('res:', res)
  res = await mq.xrange()
  console.log('res:', res)
  res = await mq.ack('1609204444880')
  console.log('res:', res)
  res = await mq.xreadGroup({ ID: 0 })
  console.log('res:', res)
  res = await mq.createGroup('test1')
    .catch(err => console.log('err:', err.message, res.command))
  console.log('res:', res)
  res = await mq.xadd({ ID: 'asd', item: { no: 1 } })
  console.log('res:', res)
  res = await mq.xread({ ID: 1609204903275 })
  // mq.subcribe(STREAM_KEY, (res) => {
  //   console.log('res:', res)
  // })
  console.log('res:', res)

  // const s = await StreamQueue.init({ stream: STREAM_KEY, client: redis })

  // res1 = await mq.addTask(STREAM_KEY, { test: 'ok' }, 26)
  // await s.streamModel.readByGroup({  })
  // mq.addTask(STREAM_KEY, { test: 'ok' })
  // setInterval(() => {
  //   const label = `${STREAM_KEY}:${generateRandom(6)}`
  //   mq.addTask({ label, expire: 1000 * 60, stored: +new Date(), ttl: 60 })
  // }, 1000 * 5)
  console.log('res1:', res1)

  console.log('res2:', res2)
  // return

  // XACK streamDemoKey streamDemoGroup 1608810766785 - 0
  process.exit(0)
}
f().catch(err => console.log('err:', err))
