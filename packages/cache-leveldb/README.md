# 介绍

完整介绍请参考 [github.com/koishijs/cache](https://github.com/koishijs/cache/blob/main/docs/zh-CN/index.md)

## 用法

```ts
import {} from '@koishijs/cache'

// 扩展 foo 表
declare module '@koishijs/cache' {
  interface Tables {
    foo: number
  }
}

// 声明依赖
export const inject = {
  required: ['cache'],
  optional: []
};

await ctx.cache.set('foo', 'bar', 114514)
await ctx.cache.get('foo', 'bar') // 会返回 114514 
```
