// https://github.com/redisq/redisq/blob/master/queue.js

"use strict"

const { customAlphabet } = require('nanoid')
const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
const taskID = customAlphabet(alphabet, 32)

const Redis = require('ioredis')

const NAMESPACE = 'QUEUE'
const DEDUPESET = 'DEDUPE'

const sleep = delay => new Promise(resolve => setTimeout(() => resolve(), delay))

const prettyError = (error) => {
  let keys = error ? Reflect.ownKeys(error) : []

  let result = keys.reduce((memo, key) => {
    memo[key] = error[key]

    return memo
  }, {})

  process.env.NODE_ENV === 'production' && delete result.stack
  delete result.ctx

  return result
}

const COMMANDS = {
  xadd({ redis, stream, length, payload, options, id }) {
    return redis.xadd(stream, 'MAXLEN', '~', length, '*', 'payload', JSON.stringify(payload), 'options', JSON.stringify(options), 'id', id)
  }
}

class RedisStreamsQueue {
  constructor({
    name = 'queue',
    group,
    consumer,
    redis = new Redis(process.env.REDIS),
    length = 10000,
    logger = console,
    worker = async () => void 0,
    batch_size = 10,
    claim_interval = 1000 * 60 * 60,
    loop_interval = 5000,
    onTaskComplete,
    onTaskError,
    onTaskPending,
    onClaim
  }) {
    this.name = name
    this.group = group || `${name}:group`

    this.consumer = consumer || `consumer:${taskID()}`

    this.redis = redis
    this.length = length
    this.logger = logger
    /* this.concurency = concurency; */ // CONCYRENCY CAN BE MADE BY CREATING MULTIPLY INSTANCES WITH THE SAME NAMES
    this.batch_size = batch_size

    this.worker = worker
    this.onTaskComplete = onTaskComplete
    this.onTaskError = onTaskError
    this.onTaskPending = onTaskPending
    this.onClaim = onClaim

    this.redis.xgroup('CREATE', name, this.group, 0, 'MKSTREAM').catch(() => void 0)

    this.started = false
    this.ID = 0

    this.loop_interval = loop_interval
    this.claim_interval = claim_interval

    this.claim_interval && setInterval(() => {
      this.claim()
    }, this.claim_interval)

    this.claim_interval && this.claim()
  }

  async claim() {
    let consumers = await this.redis.xinfo('CONSUMERS', this.name, this.group)

    consumers = consumers.reduce((memo, consumer) => {
      let [, name, , pending, , idle] = consumer

      memo.push({ name, pending, idle })

      return memo
    }, [])

    const expired = consumers.filter(consumer => consumer.idle > this.claim_interval)

    for (let consumer of expired) {
      if (consumer.name !== this.consumer && consumer.pending > 0) {
        let pending = await this.redis.xpending(this.name, this.group, '-', '+', this.batch_size, consumer.name)

        pending = pending.map(task => task[0])

        let tasks = {}

        for (let id of pending) {
          let range = await this.redis.xrange(this.name, id, id)

          if (range.length) {
            let [[, body]] = range
            let [, , , , , task_id] = body

            tasks[task_id] = id
          }
        }

        const claims = Object.values(tasks)

        pending = await this.redis.xclaim(this.name, this.group, this.consumer, 0, ...claims, 'JUSTID')

        pending.length && this.onClaim && this.onClaim({ name: this.name, consumer: consumer.name, tasks: pending })

        this.logger.info(`Claimed tasks from ${consumer.name} -> ${this.consumer}: ${pending}`)

        consumer.skip = claims.length > this.batch_size
      }

      if (consumer.name !== this.consumer && !consumer.skip) {
        const pending_count = await this.redis.xgroup('DELCONSUMER', this.name, this.group, consumer.name)

        if (pending_count > 0) {
          this.logger.warn(`Consumer ${consumer.name} had pending messages!`)
        }

        this.logger.info(`Consumer deleted: ${consumer.name}`)
      }
    }
  }

  async clear(task_id) {
    if (!task_id) {
      await this.redis.xtrim(this.name, 'MAXLEN', 0)

      return await this.redis.del(`${this.name}:${DEDUPESET}`)
    }
    else {
      const message_id = await this.redis.get(`${this.name}:TASK:${task_id}`)

      return await this.redis.multi()
        .xdel(this.name, message_id)
        .srem(`${this.name}:${DEDUPESET}`, task_id)
        .exec()
    }
  }

  async push({ payload = {}, id = taskID(), options = {} }) {
    options = { timeout: 5000, delay: 0, retries: 0, ...options }

    const exists = !await this.redis.sadd(`${this.name}:${DEDUPESET}`, id) // dedupe

    if (!exists) {
      this.logger.info(`Pushed task:`, this.name, id)

      const message_id = await COMMANDS.xadd({ redis: this.redis, stream: this.name, length: this.length, payload, options, id })

      await this.redis.set(`${this.name}:TASK:${id}`, message_id)

      return message_id
    }
    else {
      this.logger.info(`Task already exists:`, this.name, id)
    }

    return exists
  }

  restart() {
    if (this.started) {
      this.ID = 0
      this.started = false
      this.start()
    }
  }

  start() {
    if (!this.started) {
      this.started = true
      this._loop()
    }
  }

  stop() {
    this.started = false
  }

  async _loop() {
    while (this.started) {
      let semaphore = false

      await this.redis.xreadgroup('GROUP', this.group, this.consumer, /* 'BLOCK', this.loop_interval, */ 'COUNT', this.ID === 0 ? 0 : this.batch_size, 'STREAMS', this.name, this.ID).then(async (data) => {
        if (!semaphore) {
          semaphore = true
          this.ID = '>'

          await this._processTasks(data).finally(() => semaphore = false)
        }
      })

      await sleep(this.loop_interval)
    }
  }

  async _processTasks(data) {
    if (data) {
      this.logger.info('Consumer process tasks:', this.group, data)

      const [[stream, messages]] = data

      for (const message of messages) {
        const [id, payload] = message

        if (!payload) {
          await this.redis.multi()
            .xack(stream, this.group, id)
            .xdel(stream, id)
            .exec()

          continue
        }

        let [, value, , options, , task_id, , errors] = payload || []

        try {
          value = JSON.parse(value)
        }
        catch (err) {
          err
        }
        options = options ? JSON.parse(options) : {}
        errors = errors ? JSON.parse(errors) : 0

        const worker = ({ message_id, payload, task_id }) => {
          return this.worker.constructor.name === 'AsyncFunction' ? this.worker({ name: this.name, message_id, payload, task_id }) : new Promise((resolve, reject) => {
            try {
              resolve(this.worker({ name: this.name, message_id, payload, task_id }))
            }
            catch (err) {
              reject(err)
            }
          })
        }

        let promises = [
          !options.timeout
            ? void 0
            : new Promise((resolve, reject) => {
              const timer = setTimeout(() => {
                clearTimeout(timer)

                reject({
                  code: 500,
                  message: `Task timed out after ${options.timeout} ms.`,
                })
              }, options.timeout)
            }),
          worker({ message_id: id, payload: value, task_id })
        ]

        promises = promises.filter((promise) => !!promise)

        let retries = options.retries

        await Promise.race(promises).then(async result => {
          if (result) {
            await this.redis.multi()
              .xack(stream, this.group, id)
              .xdel(stream, id)
              .srem(`${this.name}:${DEDUPESET}`, task_id)
              .exec()

            this.onTaskComplete && this.onTaskComplete({ name: this.name, payload: value, options, task_id, result })

            this.logger.info(`Task "${task_id}" ends with "${typeof (result) === 'object' ? JSON.stringify(result) : result}"`)
          }
          else {
            if (retries < 0) {
              retries = 2
              errors = 0

              throw { code: 400, message: 'endless queue' }
            }

            this.logger.info(`Task remains as pending "${task_id}"`)

            this.onTaskPending && this.onTaskPending({ name: this.name, payload: value, options, task_id })
          }
        }).catch(async (error) => {
          error = prettyError(error)

          errors++

          const retry = !!((retries === -1) || (retries && (errors < (retries || 0))))

          if (retry) {
            if (retries && options.delay) {
              this.logger.info(`Delay task: ${task_id}`)

              await sleep(options.delay)
            }

            const [, , [, message_id]] = await this.redis
              .multi()
              .xack(stream, this.group, id)
              .xdel(stream, id)
              //.srem(`${this.name}:${DEDUPESET}`, task_id) //DO NOT REMOVE FROM DEDUPE TO AVOID APPEND NEW TASK WITH TE SAME task_id
              .xadd(stream, 'MAXLEN', '~', 10000, '*', 'payload', JSON.stringify(value), 'options', JSON.stringify(options), 'id', task_id, 'errors', errors)
              .exec()

            await this.redis.set(`${this.name}:TASK:${task_id}`, message_id)
          } else {
            await this.redis
              .multi()
              .xack(stream, this.group, id)
              .xdel(stream, id)
              .srem(`${this.name}:${DEDUPESET}`, task_id)
              .exec()

            this.logger.error(`Error on task ${stream}.${task_id}`, error)

            this.onTaskError && this.onTaskError({ name: this.name, error, payload: value, options, task_id })
          }
        })
      }
    }
  }
}

module.exports = {
  RedisStreamsQueue
}
