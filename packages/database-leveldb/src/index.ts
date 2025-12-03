import { Context, Schema, Driver, Query, Selection, Dict, deepEqual, executeUpdate, Eval, Model } from 'koishi';
import { Level, ChainedBatch } from 'level';
import { resolve } from 'node:path';
import { AbstractLevel } from 'abstract-level';

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


class LevelDBDriver extends Driver
{
  static name = 'leveldb';
  static inject = ['logger'];

  public db: Level<string, any>;
  private _path: string;
  private _activeBatch: ChainedBatch<any, string, any> | null = null;

  constructor(public ctx: Context, public config: LevelDBDriver.Config)
  {
    super(ctx, config);
    this.logger = ctx.logger(LevelDBDriver.name);
    this._path = resolve(ctx.baseDir, config.path);
  }

  async start()
  {
    this.db = new Level(this._path, { valueEncoding: 'json' });
    await this.db.open();
    this.logger.info('数据库已在 %c 打开', this._path);
    this.ctx.on('dispose', () => this.stop());
  }

  async stop()
  {
    await this.db.close();
    this.logger.info('数据库已关闭');
  }

  private getTable(name: string): AbstractLevel<any, string, any>
  {
    // 使用 sublevel 来隔离不同表的数据，key 会自动添加前缀
    return this.db.sublevel(name, { valueEncoding: 'json' });
  }

  private getCounter(name: string): AbstractLevel<any, string, number>
  {
    // 为自增主键创建一个独立的 sublevel
    return this.db.sublevel(name, { valueEncoding: 'json' });
  }

  async prepare(table: string)
  {
    // leveldb 无需 prepare 操作，表会在第一次使用时自动创建
  }

  async drop(table: string)
  {
    if (!table) return;
    await this.getTable(table).clear();
  }

  async dropAll()
  {
    await this.db.clear();
  }

  async stats()
  {
    const stats: Driver.Stats = { size: 0, tables: {} };
    const tableNames = new Set<string>();

    // 通过遍历 key 并解析 sublevel 前缀来获取所有表名
    // sublevel 的 key 格式为 !<sublevel-name>!<key>
    for await (const key of this.db.keys({ gte: '!', lte: '~' }))
    {
      if (key.startsWith('!'))
      {
        const tableName = key.split('!')[1];
        if (tableName && tableName !== '_counters' && tableName !== '_indexes')
        {
          tableNames.add(tableName);
        }
      }
    }

    for (const name of tableNames)
    {
      const table = this.getTable(name);
      let count = 0;
      let tableSize = 0;
      for await (const value of table.values())
      {
        count++;
        tableSize += Buffer.from(JSON.stringify(value)).length;
      }
      stats.tables[name] = { count, size: tableSize };
      stats.size += tableSize;
    }

    return stats;
  }

  private async _getRows(sel: Selection.Immutable): Promise<any[]>
  {
    const { query, model } = sel;
    const tableName = model.name;
    if (!tableName) return []; // 安全检查
    const tableDb = this.getTable(tableName);
    const allData = await tableDb.values().all();
    return allData.filter(row => evaluateQuery(row, query));
  }

  async get(sel: Selection.Immutable)
  {
    const { args } = sel;
    const [options] = args;
    let result = await this._getRows(sel);

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

    if (aggrKey === '$count') return rows.length;

    const values = rows.map(row =>
    {
      const value = typeof aggrValue === 'string' ? get(row, aggrValue) : aggrValue;
      return typeof value === 'number' ? value : 0;
    });

    if (values.length === 0) return 0;

    switch (aggrKey)
    {
      case '$sum': return values.reduce((a, b) => a + b, 0);
      case '$avg': return values.reduce((a, b) => a + b, 0) / values.length;
      case '$max': return Math.max(...values);
      case '$min': return Math.min(...values);
    }

    this.logger.warn('不支持的聚合运算符', aggrKey);
    return 0;
  }

  // 构造唯一的字符串键，支持复合主键
  private _constructKey(keys: string | string[], row: any)
  {
    if (Array.isArray(keys))
    {
      return keys.map(key => row[key]).join('|');
    }
    return String(row[keys]);
  }

  async set(sel: Selection.Mutable, update: {})
  {
    const { ref, model } = sel;
    const tableName = model.name;
    const tableDb = this.getTable(tableName);
    const primaryKeys = model.primary;

    const rows = await this._getRows(sel);
    if (!rows.length) return { matched: 0, modified: 0 };

    let modifiedCount = 0;
    const batch = this._activeBatch || tableDb.batch();

    for (const row of rows)
    {
      const original = { ...row };
      executeUpdate(row, update, ref);
      if (!deepEqual(original, row))
      {
        modifiedCount++;
        const key = this._constructKey(primaryKeys, row);
        batch.put(key, row);
      }
    }

    if (!this._activeBatch) await batch.write();
    return { matched: rows.length, modified: modifiedCount };
  }

  async remove(sel: Selection.Mutable)
  {
    const { model } = sel;
    const tableName = model.name;
    const tableDb = this.getTable(tableName);
    const primaryKeys = model.primary;

    const rows = await this._getRows(sel);
    if (!rows.length) return { matched: 0, removed: 0 };

    const batch = this._activeBatch || tableDb.batch();
    for (const row of rows)
    {
      const key = this._constructKey(primaryKeys, row);
      batch.del(key);
    }
    if (!this._activeBatch) await batch.write();

    return { matched: rows.length, removed: rows.length };
  }

  async create(sel: Selection.Mutable, data: {})
  {
    const { model } = sel;
    const tableName = model.name;
    const tableDb = this.getTable(tableName);
    const primaryKeys = model.primary;
    const newRow = model.create(data);

    if (model.autoInc && typeof primaryKeys === 'string')
    {
      const counterDb = this.getCounter('_counters');
      let maxId = 0;
      try
      {
        maxId = await counterDb.get(tableName) || 0;
      } catch { /* 键未找到，忽略 */ }
      const newId = maxId + 1;
      newRow[primaryKeys] = newId;
      // 计数器更新也应包含在事务批处理中
      const batch = this._activeBatch || this.db.batch();
      batch.put(tableName, newId);
      if (!this._activeBatch) await batch.write();
    }

    const key = this._constructKey(primaryKeys, newRow);
    const mainBatch = this._activeBatch || tableDb.batch();
    mainBatch.put(key, newRow);
    if (!this._activeBatch) await mainBatch.write();
    return newRow;
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[])
  {
    const { model, ref } = sel;
    const tableName = model.name;
    const tableDb = this.getTable(tableName);
    const primaryKeys = model.primary;
    const result = { inserted: 0, matched: 0, modified: 0 };

    const allData = await tableDb.values().all();
    const batch = this._activeBatch || tableDb.batch();

    for (const item of data)
    {
      const query = {};
      for (const key of keys)
      {
        query[key] = item[key];
      }
      const existingRows = allData.filter(row => evaluateQuery(row, query));

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
            const key = this._constructKey(primaryKeys, row);
            batch.put(key, row);
          }
        }
      } else
      {
        const newRow = executeUpdate(model.create(), item, ref);
        if (model.autoInc && typeof primaryKeys === 'string' && !newRow[primaryKeys])
        {
          const counterDb = this.getCounter('_counters');
          let maxId = 0;
          try
          {
            maxId = await counterDb.get(tableName) || 0;
          } catch { /* 键未找到，忽略 */ }
          const newId = maxId + 1;
          newRow[primaryKeys] = newId;
          // 计数器更新也应包含在事务批处理中
          const counterBatch = this._activeBatch || this.db.batch();
          counterBatch.put(tableName, newId);
          if (!this._activeBatch) await counterBatch.write();
        }
        const key = this._constructKey(primaryKeys, newRow);
        batch.put(key, newRow);
        // 添加到内存数组中，以便后续项目可以找到
        allData.push(newRow);
        result.inserted++;
      }
    }

    if (!this._activeBatch) await batch.write();
    return result;
  }

  async withTransaction(callback: () => Promise<void>)
  {
    if (this._activeBatch)
    {
      // 如果已经在一个事务中，直接执行回调
      return callback();
    }

    this._activeBatch = this.db.batch();
    try
    {
      await callback();
      await this._activeBatch.write();
    } catch (e)
    {
      // 如果发生任何错误，则不写入批处理
      this.logger.error('事务失败，更改已丢弃。');
      throw e;
    } finally
    {
      // 确保批处理被清除
      this._activeBatch = null;
    }
  }

  async getIndexes(table: string): Promise<Driver.Index[]>
  {
    const indexDb = this.getTable('_indexes');
    try
    {
      const indexes = await indexDb.get(table);
      return indexes || [];
    } catch (error)
    {
      // @ts-ignore
      if (error.code === 'LEVEL_NOT_FOUND') return [];
      throw error;
    }
  }

  async createIndex(table: string, index: Driver.Index): Promise<void>
  {
    const indexDb = this.getTable('_indexes');
    const indexes = await this.getIndexes(table);
    const name = index.name || Object.keys(index.keys).join('_');
    if (indexes.some(i => i.name === name))
    {
      this.logger.warn(`表 ${table} 上的索引 ${name} 已存在。`);
      return;
    }
    indexes.push({ ...index, name });
    await indexDb.put(table, indexes);
  }

  async dropIndex(table: string, name: string): Promise<void>
  {
    const indexDb = this.getTable('_indexes');
    let indexes = await this.getIndexes(table);
    const indexPos = indexes.findIndex(i => i.name === name);
    if (indexPos !== -1)
    {
      indexes.splice(indexPos, 1);
      await indexDb.put(table, indexes);
    }
  }
}

namespace LevelDBDriver
{
  export interface Config
  {
    path: string;
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.path({
      filters: ['directory'],
      allowCreate: true,
    }).description('数据库目录的文件夹路径。').default('data/database/leveldb'),
  });
}

export default LevelDBDriver;