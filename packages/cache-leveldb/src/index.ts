import { Awaitable, Context, Dict, Schema, Service } from 'koishi';
import { Level } from 'level';
import { resolve } from 'node:path';
import { AbstractLevel } from 'abstract-level';

// Koishi 模块声明，为 Context 添加 cache 属性
declare module 'koishi' {
  interface Context
  {
    cache: Cache;
  }
}

// 定义缓存表接口
export interface Tables
{
  default: any;
}

// 抽象的 Cache 服务类
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

// LevelDB Cache 实现
class LevelDBCache extends Cache
{
  static inject = ['logger'];

  private db: Level<string, any>;
  private _path: string;

  constructor(protected ctx: Context, public config: LevelDBCache.Config)
  {
    super(ctx);
    this._path = resolve(ctx.baseDir, config.path);

    // 在构造函数中初始化数据库
    this.db = new Level(this._path, { valueEncoding: 'json' });

    // 使用 'ready' 事件确保数据库在 Koishi 启动后打开
    ctx.on('ready', async () =>
    {
      try
      {
        await this.db.open();
        this.ctx.logger('cache').info('leveldb cache service started at %c', this._path);
      } catch (err)
      {
        this.ctx.logger('cache').error('failed to open leveldb cache: %s', err);
      }
    });

    // 注册 dispose 回调以安全关闭数据库
    ctx.on('dispose', async () =>
    {
      if (this.db?.status === 'open')
      {
        await this.db.close();
        this.ctx.logger('cache').info('leveldb cache service stopped');
      }
    });
  }

  private getTable(name: string): AbstractLevel<any, string, any>
  {
    // 使用 sublevel 来隔离不同表的数据
    return this.db.sublevel(name, { valueEncoding: 'json' });
  }

  async clear(name: string)
  {
    await this.getTable(name).clear();
  }

  async get(name: string, key: string)
  {
    try
    {
      return await this.getTable(name).get(key);
    } catch (error)
    {
      // @ts-ignore
      if (error.code === 'LEVEL_NOT_FOUND') return undefined;
      throw error;
    }
  }

  async set(name: string, key: string, value: any, maxAge?: number)
  {
    // leveldb cache does not support maxAge without extra packages
    await this.getTable(name).put(key, value);
  }

  async delete(name: string, key: string)
  {
    await this.getTable(name).del(key);
  }

  async* keys(table: string)
  {
    yield* this.getTable(table).keys();
  }

  async* values(table: string)
  {
    yield* this.getTable(table).values();
  }

  async* entries(table: string)
  {
    for await (const [key, value] of this.getTable(table).iterator())
    {
      yield [key, value] as [string, any];
    }
  }
}

namespace LevelDBCache
{
  export interface Config
  {
    path?: string;
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.path({
      filters: ['directory'],
      allowCreate: true,
    }).description('缓存数据库的路径').default('data/cache/leveldb'),
  });
}

export default LevelDBCache;