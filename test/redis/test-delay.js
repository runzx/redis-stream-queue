
const { DelayQueue } = require("../../lib/redis-queue")

const queue = new DelayQueue({
  password: 'zx2962',
  maxlen: 6,
  sKey: 'abc',
  gKey: 'bosstg', cKey: 'zx'
})
queue.subcribe(msg => {
  console.log('task msg:', msg)
})
queue.start().then(() => {


  queue.addTask({ test: 4, data: 'hehe' }, 5)
  // queue.addTask({ test: 2, data: 'hehe' }, 20)

  // queue.getPELList().then((res) => {
  //   console.log('res:', res)
  //   res.forEach(i => {
  //     queue.xack(i.id)
  //   })
  // })
})


  // queue.delayStream.getPELList().then((res) => {
  //   console.log('res:', res)
  //   res.forEach(i => {
  //     queue.delayStream.xack(i.id)
  //   })
  // })

  // queue.getGroupInfo().then((res) => {
  //   console.log('getGroupInfo:', res)
  // })

  // queue.getStreamInfo().then((res) => {
  //   console.log('getStreamInfo:', res)
  // })