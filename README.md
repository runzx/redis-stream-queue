# redis-stream-queue

message queue on redis stream v5+.

## ES6 

- 采用 ES6 + async/await + 面向对象重构设计
- 达到易读懂，方便修改的学习目的。

## 进度

1. 基本 redis stream api 包文件: redis-stream.js

## 目录结构

```bash
├── lib                   # 相关代码
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
