const IORedis = require("ioredis")
const { RedisStream } = require("../../lib/redis-stream")
const redis = new IORedis()

RedisStream
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
  const mq = await RedisStream.init({
    client: redis, sKey: STREAM_KEY,
    gKey: GROUP_NAME, cKey: CONSUMER_NAME
  })

  let res, res1, res2 = +new Date()
  // 8秒插入16万条记录
  for (let index = 0; index < 158000; index++) {
    res = await mq.add({ no: index })
    if (!res) console.log('res:', index)
  }
  console.log('time:', +new Date() - res2)
  process.exit(0)

  console.log('res:', res)

  return
  res = await mq.getStreamInfo()
  console.log('res:', res)
  res = await mq.getGroupsInfo()
  console.log('res:', res)
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
