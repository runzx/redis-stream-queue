# redis-stream-queue

message queue on redis stream v5+.

## redis v5+ stream, ES6

- 延时队列 采用双 stream 队列保证订阅消息(`__keyevent@5__:expired`)不丢失
- 采用 ES6 + async/await + 面向对象设计
- 达到易读懂，方便修改。
- vscode 上有专为此开发的扩展插件: [redis-stream](https://marketplace.visualstudio.com/items?itemName=zhaixiang.redis-stream&ssr=false#overview)

## Install

```shell
npm install redis-stream-queue
```

## Basic Usage

```js
const { RedisQueue } = require('redis-stream-queue')
const IORedis = require('ioredis')

const client = new IORedis(opt)
const mq = RedisQueue.init({ client })

const sKey = 'streamName' // queue name
const ttl = 0 // unit:second,defalut 0; if ttl >0 --> delayQueue,

mq.subcribe(sKey, cb) // callback for task
const msgId = mq.addTask(sKey, { orderNo: '20210101001' }, ttl)
```

## 进度

1. 基本 redis stream api 包文件: redis-stream.js
2. 消息队列 redis stream queue(delay)包文件: stream-queue.js

## 目录结构

```bash
├── lib                   # 相关代码
│   ├── stream-queue.js   queue api库
│   ├── redis-stream.js   stream api库
│   └── redis-base.js     基本库文件
│
├── doc                   #  相关文档
│   ├──
│   ├──
│   └── stream.md         redis stream 要点
│
└── index.js              # 主入口

```

## 提交规范

- 请注意代码规范（vscode 默认 TypeScript 风格）。

- 提交前请先拉取代码，以免产生不必要的冲突

- 提交规范：`key: value`

- `key` 可选 ：

  ```
  feat：  新功能（feature）
  fix：   修补bug
  docs：  文档（documentation）
  style： 格式（不影响代码运行的变动）
  refactor：重构（即不是新增功能，也不是修改bug的代码变动）
  test：  增加测试
  chore： 构建过程或辅助工具的变动
  release: 发布
  ```

## 说明

-

-
