# koishi-plugin-database-jsondb

基于 JSON 文件的 Koishi 数据库服务。

## 特性

- **轻量级**: 无外部数据库依赖，非常适合小型机器人。
- **易于使用**: 配置简单，即插即用。

## 说明

数据库文件默认存放于 `data/database/jsondb/*.json`

json文件会以表的名称命名

## 使用方法

完整用法请参考`koishi`文档 ->  [koishi.chat/zh-CN/guide/database](https://koishi.chat/zh-CN/guide/database/index.html)

### 在你的插件中使用

```typescript
import { Context } from 'koishi'

// 声明你需要用到的数据表结构
declare module 'koishi' {
  interface Tables {
    demo: DemoTable
  }
}

export interface DemoTable {
  userid: string
  // ... 其他字段
}

export const inject = ['database']

export async function apply(ctx: Context) {
  // 为 "demo" 表扩展模型
  ctx.model.extend('demo', {
    userid: 'string', // 用户 ID
    // ... 其他字段
  }, {
    primary: ['userid'], // 设置主键
  })

  ctx.middleware(async (session, next) => {
    let userId = session.userId
    if (!userId) return next()

    // 尝试从数据库中获取用户记录
    let [userRecord] = await ctx.database.get('demo', { userid: userId })

    // 如果记录不存在，则创建一条新记录
    if (!userRecord) {
      userRecord = {
        userid: userId,
        // ... 初始化其他字段
      }
      await ctx.database.create('demo', userRecord)
    }

    // 现在你可以使用 userRecord 对象了
    // ...
    
    return next()
  })
}
```
