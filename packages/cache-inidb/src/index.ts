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

// INI 格式解析与序列化
namespace INI
{
  export function parse(source: string): Dict<Dict<string>>
  {
    const result = Object.create(null);
    let currentSection: string = null;
    for (const line of source.split(/[\r\n]+/))
    {
      const trimmed = line.trim();
      if (trimmed.startsWith('[') && trimmed.endsWith(']'))
      {
        currentSection = trimmed.slice(1, -1);
        result[currentSection] ??= Object.create(null);
      } else if (currentSection && trimmed.includes('='))
      {
        const index = trimmed.indexOf('=');
        const key = trimmed.slice(0, index).trim();
        // 尝试解析 JSON 值，如果失败则作为字符串
        try
        {
          result[currentSection][key] = JSON.parse(trimmed.slice(index + 1).trim());
        } catch
        {
          result[currentSection][key] = trimmed.slice(index + 1).trim();
        }
      }
    }
    return result;
  }

  export function stringify(data: Dict<Dict<any>>): string
  {
    let output = '';
    for (const section in data)
    {
      output += `[${section}]\n`;
      for (const key in data[section])
      {
        const value = data[section][key];
        // 将值序列化为 JSON 字符串以保留类型
        output += `${key} = ${JSON.stringify(value)}\n`;
      }
      output += '\n';
    }
    return output;
  }
}

// INI DB Cache 实现
class IniDBCache extends Cache
{
  static inject = ['logger'];

  private _path: string;
  private store: Dict<Dict<any>> = Object.create(null);
  private _debounce: NodeJS.Timeout | null = null;

  constructor(protected ctx: Context, public config: IniDBCache.Config)
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
      this.store = INI.parse(data);
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
    try
    {
      await fs.writeFile(this._path, INI.stringify(this.store));
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
    const table = this.table(name);
    return table[key];
  }

  async set(name: string, key: string, value: any, maxAge?: number)
  {
    // inidb cache does not support maxAge
    const table = this.table(name);
    table[key] = value;
    this.write();
  }

  async delete(name: string, key: string)
  {
    const table = this.table(name);
    delete table[key];
    this.write();
  }

  async* keys(table: string)
  {
    const entries = this.table(table);
    yield* Object.keys(entries);
  }

  async* values(table: string)
  {
    const entries = this.table(table);
    yield* Object.values(entries);
  }

  async* entries(table: string)
  {
    const entries = this.table(table);
    for (const key in entries)
    {
      yield [key, entries[key]] as [string, any];
    }
  }
}

namespace IniDBCache
{
  export interface Config
  {
    path?: string;
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.path().description('缓存文件的路径').default('data/cache/cache.ini'),
  });
}

export default IniDBCache;