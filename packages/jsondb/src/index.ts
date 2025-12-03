import { Awaitable, Context, Dict, Schema, Service } from 'koishi'
import { promises as fs } from 'node:fs'
import { resolve } from 'node:path'

declare module 'koishi' {
  interface Context {
    cache: Cache
  }
}

export interface Tables {
  default: any
}

abstract class Cache extends Service {
  static [Service.provide] = 'cache'

  constructor(ctx: Context) {
    super(ctx, 'cache')
  }

  abstract clear<K extends keyof Tables>(table: K): Promise<void>
  abstract get<K extends keyof Tables>(table: K, key: string): Promise<Tables[K]>
  abstract set<K extends keyof Tables>(table: K, key: string, value: Tables[K], maxAge?: number): Promise<void>
  abstract delete<K extends keyof Tables>(table: K, key: string): Promise<void>
  abstract keys<K extends keyof Tables>(table: K): AsyncIterable<string>
  abstract values<K extends keyof Tables>(table: K): AsyncIterable<Tables[K]>
  abstract entries<K extends keyof Tables>(table: K): AsyncIterable<[string, Tables[K]]>

  async forEach<K extends keyof Tables>(table: K, callback: (value: Tables[K], key: string) => Awaitable<void>) {
    const tasks: Awaitable<void>[] = []
    for await (const [key, value] of this.entries(table)) {
      tasks.push(callback(value, key))
    }
    await Promise.all(tasks)
  }
}

// JsonDB 缓存服务的实现
class JsonDBCache extends Cache {
  private _path: string
  private store: Dict<Dict<any>> = Object.create(null)

  constructor(protected ctx: Context, public config: JsonDBCache.Config) {
    super(ctx)
    this._path = resolve(ctx.baseDir, config.path)
    this.init()
  }

  private async init() {
    try {
      const data = await fs.readFile(this._path, 'utf8')
      this.store = JSON.parse(data)
    } catch (err) {
      if (err.code !== 'ENOENT') {
        this.ctx.logger('cache').warn('failed to read cache file: %s', err)
      }
    }
  }

  private async write() {
    try {
      await fs.writeFile(this._path, JSON.stringify(this.store))
    } catch (err) {
      this.ctx.logger('cache').warn('failed to write cache file: %s', err)
    }
  }

  private table(name: string): Dict<any> {
    return this.store[name] ??= Object.create(null)
  }

  async clear(name: string) {
    delete this.store[name]
    await this.write()
  }

  async get(name: string, key: string) {
    const table = this.table(name)
    return table[key]
  }

  async set(name: string, key: string, value: any, maxAge?: number) {
    // jsondb cache does not support maxAge
    const table = this.table(name)
    table[key] = value
    await this.write()
  }

  async delete(name: string, key: string) {
    const table = this.table(name)
    delete table[key]
    await this.write()
  }

  async* keys(table: string) {
    const entries = this.table(table)
    yield* Object.keys(entries)
  }

  async* values(table: string) {
    const entries = this.table(table)
    yield* Object.values(entries)
  }

  async* entries(table: string) {
    const entries = this.table(table)
    for (const key in entries) {
      yield [key, entries[key]] as [string, any]
    }
  }
}

namespace JsonDBCache {
  export interface Config {
    path?: string
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.string().description('缓存文件的路径').default('cache.json'),
  })
}

export default JsonDBCache