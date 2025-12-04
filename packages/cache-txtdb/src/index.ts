import { Awaitable, Context, Dict, Schema, Service } from 'koishi';
import { promises as fs } from 'node:fs';
import { dirname, resolve } from 'node:path';

// Koishi 模块声明
declare module 'koishi' {
  interface Context
  {
    cache: Cache;
  }
}

// 缓存表接口
export interface Tables
{
  default: any;
}

// 抽象 Cache 服务
abstract class Cache extends Service
{
  static [Service.provide] = 'cache';

  constructor(ctx: Context)
  {
    super(ctx, 'cache');
  }

  abstract clear<K extends keyof Tables>(table: K): Promise<void>;
  abstract get<K extends keyof Tables>(table: K, key: string): Promise<Tables[K]>;
  abstract set<K extends keyof Tables>(table: K, key: string, value: Tables[K], maxAge?: number): Promise<void>;
  abstract delete<K extends keyof Tables>(table: K, key: string): Promise<void>;
  abstract keys<K extends keyof Tables>(table: K): AsyncIterable<string>;
  abstract values<K extends keyof Tables>(table: K): AsyncIterable<Tables[K]>;
  abstract entries<K extends keyof Tables>(table: K): AsyncIterable<[string, Tables[K]]>;

  async forEach<K extends keyof Tables>(table: K, callback: (value: Tables[K], key: string) => Awaitable<void>)
  {
    const tasks: Awaitable<void>[] = [];
    for await (const [key, value] of this.entries(table))
    {
      tasks.push(callback(value, key));
    }
    await Promise.all(tasks);
  }
}

// TXT DB Cache 实现
class TxtDBCache extends Cache
{
  static inject = ['logger'];

  private _path: string;
  private store: Dict<Dict<any>> = Object.create(null);
  private _debounce: NodeJS.Timeout | null = null;

  constructor(protected ctx: Context, public config: TxtDBCache.Config)
  {
    super(ctx);
    this._path = resolve(ctx.baseDir, config.path);
    this.init();

    ctx.on('dispose', () =>
    {
      if (this._debounce)
      {
        clearTimeout(this._debounce);
        return this.flush();
      }
    });
  }

  private async init()
  {
    try
    {
      await fs.mkdir(dirname(this._path), { recursive: true });
      const data = await fs.readFile(this._path, 'utf8');
      if (!data) return;
      for (const line of data.split(/[\r\n]+/))
      {
        if (!line) continue;
        try
        {
          const [table, key, value] = JSON.parse(line);
          this.table(table)[key] = value;
        } catch (err)
        {
          this.ctx.logger('cache').warn('failed to parse cache line: %s', err);
        }
      }
    } catch (err)
    {
      if (err.code !== 'ENOENT')
      {
        this.ctx.logger('cache').warn('failed to read cache file: %s', err);
      }
    }
  }

  private async flush()
  {
    this._debounce = null;
    let txtContent = '';
    for (const tableName in this.store)
    {
      for (const key in this.store[tableName])
      {
        const value = this.store[tableName][key];
        txtContent += `${JSON.stringify([tableName, key, value])}\n`;
      }
    }
    try
    {
      await fs.writeFile(this._path, txtContent);
    } catch (err)
    {
      this.ctx.logger('cache').warn('failed to write cache file: %s', err);
    }
  }

  private write()
  {
    if (this._debounce) clearTimeout(this._debounce);
    this._debounce = setTimeout(() => this.flush(), 1000);
  }

  private table(name: string): Dict<any>
  {
    return this.store[name] ??= Object.create(null);
  }

  async clear(name: string)
  {
    delete this.store[name];
    this.write();
  }

  async get(name: string, key: string)
  {
    return this.table(name)[key];
  }

  async set(name: string, key: string, value: any, maxAge?: number)
  {
    this.table(name)[key] = value;
    this.write();
  }

  async delete(name: string, key: string)
  {
    delete this.table(name)[key];
    this.write();
  }

  async* keys(table: string)
  {
    yield* Object.keys(this.table(table));
  }

  async* values(table: string)
  {
    yield* Object.values(this.table(table));
  }

  async* entries(table: string)
  {
    yield* Object.entries(this.table(table));
  }
}

namespace TxtDBCache
{
  export interface Config
  {
    path?: string;
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.path().description('缓存文件的路径').default('data/cache/cache.txt'),
  });
}

export default TxtDBCache;