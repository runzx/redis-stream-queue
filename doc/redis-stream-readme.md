# 名词定义

1. ID（key）
   - 队列中的每个节点
2. 消费组（Consumer Group）
   - XGROUP CREATE 命令创建，
   - 一个消费组有多个消费者(Consumer)
   - 游标 last_delivered_id
3. 消费者（Consumer）
4. 确认消费（acknowledge）
5. 未决队列（Pending_ids）
   - 消费者(Consumer)的状态变量，作用是维护消费者的未确认的 id
   - 已经被客户端读取的消息，但是还没有 ack
6. 消息使用 XREADGROUP 读取后会进入待处理条目列表（PEL）

##　 command

- XADD 添加消息到末尾
- XTRIM 对流进行修剪，限制长度
- XLEN 获取流包含的元素数量，即消息长度
- XDEL 删除消息
- XRANGE 获取消息列表，会自动过滤已经删除-的消息
- XREAD 以阻塞或非阻塞方式获取消息列表
- XGROUP CREATE 创建消费组
- XREADGROUP 按消费者的形式来读取消息
- XACK 用于向消息队列确认消息处理已完成
- XPENDING 查询每个消费组内所有消费者已读-取但尚未确认的消息

## stream

1. redis 1 条记录？
