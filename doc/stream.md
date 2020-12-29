# redos stream

1. 消费者组
   XGROUP 是用来创建删除和管理消费组的  
   XREADGROUP 是用来通过消费组从 Stream 里读消息的  
   XACK 是用来确认消息已经消费的

```
XGROUP [CREATE key groupname id-or-$] [SETID key groupname id-or-$] [DESTROY key groupname] [DELCONSUMER key groupname consumername]

XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] ID [ID ...]

XACK key group ID [ID ...]
```

头部开始消费 0-0  
last_delivered_id  
尾部开始消费 $

2. xgroup
   `XGROUP CREATE mystream consumer-group-name $ MKSTREAM`  
   mkstream 表示没有此流时自动创建， 长度为 0

` XREADGROUP GROUP group-name user-name STREAMS stream-name >`
XREADGROUP 中，最后一个参数 > 代表消费者希望 Redis 只给自己没有发布过的消息。如果使用具体的 ID，例如 0，则是从那个 ID 之后的 消息。

XPENDING mystream mygroup

3. xreadgroup
   `xinfo groups person`查看组信息

// 读一条最早的， 下次就读不到此条，(每当消费者读取一条消息，last_delivered_id 变量就会前进)
`xreadgroup group g-test zx count 1 streams person >`

` xack person g-test 1606543546471-0` 消息确认

`xpending person g-test`查看 pending 表

4. read
   返回 >ID 数组 [[stream,arr],[stream2,arr2]]  
   没有数据返回 null

5. xadd  自动创建 stream 
   返回 id,
   throw
