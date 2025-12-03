import { Awaitable, Context, Dict, Schema, Service } from 'koishi'
import { promises as fs } from 'node:fs'
import { dirname, resolve } from 'node:path'

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

class JsonDBCache extends Cache {
  static inject = ['logger']

  private _path: string
  private store: Dict<Dict<any>> = Object.create(null)
  private _debounce: NodeJS.Timeout | null = null

  constructor(protected ctx: Context, public config: JsonDBCache.Config) {
    super(ctx)
    this._path = resolve(ctx.baseDir, config.path)
    this.init()

    ctx.on('dispose', () => {
      if (this._debounce) {
        clearTimeout(this._debounce)
        return this.flush()
      }
    })
  }

  private async init() {
    try {
      await fs.mkdir(dirname(this._path), { recursive: true })
      const data = await fs.readFile(this._path, 'utf8')
      if (!data) return
      try {
        this.store = JSON.parse(data)
      } catch (e) {
        this.ctx.logger('cache').warn('failed to parse cache file: %s', e)
      }
    } catch (err) {
      if (err.code === 'ENOENT') {
      } else {
        this.ctx.logger('cache').warn('failed to read cache file: %s', err)
      }
    }
  }

  private async flush() {
    this._debounce = null
    try {
      await fs.writeFile(this._path, JSON.stringify(this.store, null, 2))
    } catch (err) {
      this.ctx.logger('cache').warn('failed to write cache file: %s', err)
    }
  }

  private write() {
    if (this._debounce) clearTimeout(this._debounce)
    this._debounce = setTimeout(() => this.flush(), 1000)
  }

  private table(name: string): Dict<any> {
    return this.store[name] ??= Object.create(null)
  }

  async clear(name: string) {
    delete this.store[name]
    this.write()
  }

  async get(name: string, key: string) {
    const table = this.table(name)
    return table[key]
  }

  async set(name: string, key: string, value: any, maxAge?: number) {
    // jsondb cache does not support maxAge
    const table = this.table(name)
    table[key] = value
    this.write()
  }

  async delete(name: string, key: string) {
    const table = this.table(name)
    delete table[key]
    this.write()
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
    path: Schema.path().description('缓存文件的路径').default('data/cache/cache.json'),
  })
}

export default JsonDBCache