import { Context, Schema, Driver, Model, Query, Selection, Dict, Logger, deepEqual, executeUpdate, Eval } from 'koishi';
import { promises as fs } from 'node:fs';
import { resolve, dirname } from 'node:path';

// 获取对象的嵌套属性值
function get(obj: any, path: string)
{
  if (!path) return obj;
  return path.split('.').reduce((acc, part) => acc && acc[part], obj);
}

// 内存查询求值器
function evaluateQuery(row: any, query: Query.Expr): boolean
{
  // 逻辑操作符: $and, $or, $not
  if (query.$and)
  {
    return query.$and.every(subQuery => evaluateQuery(row, subQuery));
  }
  if (query.$or)
  {
    return query.$or.some(subQuery => evaluateQuery(row, subQuery));
  }
  if (query.$not)
  {
    return !evaluateQuery(row, query.$not);
  }

  // 字段操作符
  for (const key in query)
  {
    const value = query[key];
    const rowValue = get(row, key);

    if (typeof value === 'object' && value !== null && !Array.isArray(value))
    {
      const operators = Object.keys(value);
      if (operators.some(k => k.startsWith('$')))
      {
        return operators.every(op =>
        {
          const opValue = value[op];
          switch (op)
          {
            case '$eq': return deepEqual(rowValue, opValue);
            case '$ne': return !deepEqual(rowValue, opValue);
            case '$gt': return rowValue > opValue;
            case '$gte': return rowValue >= opValue;
            case '$lt': return rowValue < opValue;
            case '$lte': return rowValue <= opValue;
            case '$in': return opValue.some(item => deepEqual(item, rowValue));
            case '$nin': return !opValue.some(item => deepEqual(item, rowValue));
            case '$regex': return (opValue instanceof RegExp ? opValue : new RegExp(opValue)).test(rowValue);
            default: return false; // 不支持的操作符
          }
        });
      }
    }

    // 默认进行深度相等比较
    if (!deepEqual(rowValue, value)) return false;
  }

  return true;
}


class JsonDBDriver extends Driver
{
  static name = 'jsondb';
  static inject = ['logger'];

  private _path: string;
  private store: Dict<any[]> = Object.create(null);
  private _debounce: NodeJS.Timeout | null = null;
  private _transactionalStore: Dict<any[]> | null = null; // 用于事务的临时存储

  constructor(public ctx: Context, public config: JsonDBDriver.Config)
  {
    super(ctx, config);
    this.logger = ctx.logger(JsonDBDriver.name);
    this._path = resolve(ctx.baseDir, config.path);
  }

  async start()
  {
    // 确保目录存在
    await fs.mkdir(dirname(this._path), { recursive: true });
    try
    {
      // 读取并解析数据库文件
      const data = await fs.readFile(this._path, 'utf8');
      if (data) this.store = JSON.parse(data);
    } catch (err)
    {
      if (err.code !== 'ENOENT')
      {
        this.logger.warn('failed to read database file: %s', err);
      }
    }
    // 监听 dispose 事件，确保程序退出时保存数据
    this.ctx.on('dispose', () => this.stop());
  }

  async stop()
  {
    if (this._debounce)
    {
      clearTimeout(this._debounce);
      await this.flush();
    }
  }

  // 立即写入磁盘
  private async flush()
  {
    this._debounce = null;
    // 如果在事务中，则不执行写入操作
    if (this._transactionalStore) return;
    try
    {
      await fs.writeFile(this._path, JSON.stringify(this.store, null, 2));
    } catch (err)
    {
      this.logger.warn('failed to write database file: %s', err);
    }
  }

  // 防抖写入
  private write()
  {
    if (this._debounce) clearTimeout(this._debounce);
    this._debounce = setTimeout(() => this.flush(), 1000);
  }

  async drop(table: string)
  {
    const store = this._transactionalStore || this.store;
    delete store[table];
    this.write();
  }

  async dropAll()
  {
    if (this._transactionalStore)
    {
      this._transactionalStore = Object.create(null);
    } else
    {
      this.store = Object.create(null);
    }
    this.write();
  }

  async stats()
  {
    const stats: Driver.Stats = { size: 0, tables: {} };
    try
    {
      const fileStats = await fs.stat(this._path);
      stats.size = fileStats.size;
    } catch { /* file does not exist */ }

    const store = this._transactionalStore || this.store;
    for (const name in store)
    {
      if (name === '_indexes') continue;
      stats.tables[name] = {
        count: store[name].length,
        // 计算单表序列化后的大致字节数
        size: Buffer.from(JSON.stringify(store[name])).length,
      };
    }
    return stats;
  }

  async prepare(table: string)
  {
    const store = this._transactionalStore || this.store;
    store[table as string] ||= [];
  }

  async get(sel: Selection.Immutable)
  {
    const { table, query, args } = sel;
    const [options] = args;
    const store = this._transactionalStore || this.store;
    const tableData = store[table as string] || [];

    // 筛选
    let result = tableData.filter(row => evaluateQuery(row, query));

    // 排序
    if (options.sort)
    {
      result.sort((a, b) =>
      {
        for (const key in options.sort)
        {
          const order = options.sort[key];
          const valA = get(a, key);
          const valB = get(b, key);
          if (valA < valB) return order === 'asc' ? -1 : 1;
          if (valA > valB) return order === 'asc' ? 1 : -1;
        }
        return 0;
      });
    }

    // 分页
    const offset = options.offset || 0;
    const limit = options.limit === undefined ? Infinity : options.limit;
    result = result.slice(offset, offset + limit);

    // 投影
    if (options.fields && Object.keys(options.fields).length)
    {
      const inclusion = Object.values(options.fields)[0];
      const keys = Object.keys(options.fields);
      if (inclusion)
      {
        result = result.map(row =>
        {
          const newRow = {};
          for (const key of keys)
          {
            const value = get(row, key);
            if (value !== undefined) newRow[key] = value;
          }
          return newRow;
        });
      } else
      {
        result = result.map(row =>
        {
          const newRow = { ...row };
          for (const key of keys)
          {
            delete newRow[key];
          }
          return newRow;
        });
      }
    }

    return result;
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr)
  {
    const rows = await this.get(sel);
    const aggrKey = Object.keys(expr)[0] as keyof Eval.Aggr<any>;
    const aggrValue = expr[aggrKey];

    // $count: 1 的情况
    if (aggrKey === '$count') return rows.length;

    const values = rows.map(row =>
    {
      // 如果 aggrValue 是字符串（字段名），则从行中取值
      // 否则，直接使用 aggrValue 本身（常量）
      const value = typeof aggrValue === 'string' ? get(row, aggrValue) : aggrValue;
      return typeof value === 'number' ? value : 0; // 只处理数字
    });

    if (values.length === 0) return 0;

    switch (aggrKey)
    {
      case '$sum': return values.reduce((a, b) => a + b, 0);
      case '$avg': return values.reduce((a, b) => a + b, 0) / values.length;
      case '$max': return Math.max(...values);
      case '$min': return Math.min(...values);
    }

    this.logger.warn('unsupported aggregation operator', aggrKey);
  }

  async set(sel: Selection.Mutable, update: {})
  {
    const { table, query, ref } = sel;
    const store = this._transactionalStore || this.store;
    const tableData = store[table as string] || [];
    let matchedCount = 0;
    let modifiedCount = 0;

    tableData.forEach(row =>
    {
      if (evaluateQuery(row, query))
      {
        matchedCount++;
        const original = { ...row };
        executeUpdate(row, update, ref);
        if (!deepEqual(original, row))
        {
          modifiedCount++;
        }
      }
    });

    if (modifiedCount > 0) this.write();
    return { matched: matchedCount, modified: modifiedCount };
  }

  async remove(sel: Selection.Mutable)
  {
    const { table, query } = sel;
    const store = this._transactionalStore || this.store;
    const tableData = store[table as string] || [];
    let removedCount = 0;
    const newTableData = tableData.filter(row =>
    {
      if (evaluateQuery(row, query))
      {
        removedCount++;
        return false;
      }
      return true;
    });
    if (removedCount > 0)
    {
      store[table as string] = newTableData;
      this.write();
    }
    return { matched: removedCount, removed: removedCount };
  }

  async create(sel: Selection.Mutable, data: {})
  {
    const { table, model } = sel;
    const store = this._transactionalStore || this.store;
    const tableData = store[table as string] ||= [];
    const newRow = model.create(data);

    if (model.primary && model.autoInc && !Array.isArray(model.primary))
    {
      const primaryKey = model.primary;
      if (newRow[primaryKey] === undefined || newRow[primaryKey] === null)
      {
        const maxId = tableData.reduce((max, row) => Math.max(max, row[primaryKey] || 0), 0);
        newRow[primaryKey] = maxId + 1;
      }
    }

    tableData.push(newRow);
    this.write();
    return newRow;
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[])
  {
    const { table, model, ref } = sel;
    const store = this._transactionalStore || this.store;
    const tableData = store[table as string] ||= [];
    const result = { inserted: 0, matched: 0, modified: 0 };

    for (const item of data)
    {
      const query = {};
      for (const key of keys)
      {
        query[key] = item[key];
      }
      const existingRows = tableData.filter(row => evaluateQuery(row, query));

      if (existingRows.length > 0)
      {
        for (const row of existingRows)
        {
          result.matched++;
          const original = { ...row };
          executeUpdate(row, item, ref);
          if (!deepEqual(original, row))
          {
            result.modified++;
          }
        }
      } else
      {
        // 先创建一个空的行，然后用更新表达式计算出最终值
        const newRow = executeUpdate(model.create(), item, ref);
        if (model.primary && model.autoInc && !Array.isArray(model.primary))
        {
          const primaryKey = model.primary;
          if (newRow[primaryKey] === undefined || newRow[primaryKey] === null)
          {
            const maxId = tableData.reduce((max, row) => Math.max(max, row[primaryKey] || 0), 0);
            newRow[primaryKey] = maxId + 1;
          }
        }
        tableData.push(newRow);
        result.inserted++;
      }
    }

    if (result.inserted > 0 || result.modified > 0) this.write();
    return result;
  }
  async withTransaction(callback: () => Promise<void>)
  {
    if (this._transactionalStore)
    {
      // 如果已经在一个事务中，直接在当前事务中执行
      return callback();
    }

    // 创建事务快照
    this._transactionalStore = JSON.parse(JSON.stringify(this.store));
    try
    {
      await callback();
      // 提交事务
      this.store = this._transactionalStore;
      this.write();
    } finally
    {
      // 结束事务
      this._transactionalStore = null;
    }
  }

  async getIndexes(table: string): Promise<Driver.Index[]>
  {
    const store = this._transactionalStore || this.store;
    const indexes = store['_indexes'] || {};
    return indexes[table] || [];
  }

  async createIndex(table: string, index: Driver.Index): Promise<void>
  {
    const store = this._transactionalStore || this.store;
    const indexes = store['_indexes'] ||= Object.create(null);
    const tableIndexes = indexes[table] ||= [];
    const name = index.name || Object.keys(index.keys).join('_');
    if (tableIndexes.some(i => i.name === name))
    {
      this.logger.warn(`index ${name} on table ${table} already exists.`);
      return;
    }
    tableIndexes.push({ ...index, name });
    this.write();
  }

  async dropIndex(table: string, name: string): Promise<void>
  {
    const store = this._transactionalStore || this.store;
    const indexes = store['_indexes'] || {};
    const tableIndexes = indexes[table] || [];
    const indexPos = tableIndexes.findIndex(i => i.name === name);
    if (indexPos !== -1)
    {
      tableIndexes.splice(indexPos, 1);
      this.write();
    }
  }
}

namespace JsonDBDriver
{
  export interface Config
  {
    path: string;
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.path({
      filters: ['file'],
      allowCreate: true,
    }).description('数据库文件的路径。').default('data/database/koishi.json'),
  });
}

export default JsonDBDriver;