import { Context, Schema, Driver, Query, Selection, Dict, deepEqual, executeUpdate, Eval } from 'koishi';
import { promises as fs } from 'node:fs';
import { resolve } from 'node:path';

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

// 辅助函数：转义CSV单元格内容
function escapeCsvCell(value: any): string
{
  if (value === null || value === undefined) return '';
  let str = typeof value === 'object' ? JSON.stringify(value) : String(value);
  // 如果字符串包含逗号、双引号或换行符，则需要用双引号包裹
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r'))
  {
    // 将字符串中的双引号替换为两个双引号
    str = str.replace(/"/g, '""');
    return `"${str}"`;
  }
  return str;
}

// 将对象数组序列化为 CSV 字符串
function stringifyCsv(data: any[]): string
{
  if (data.length === 0) return '';
  const headers = Object.keys(data[0]);
  const headerRow = headers.map(escapeCsvCell).join(',');
  const rows = data.map(row =>
  {
    return headers.map(header => escapeCsvCell(row[header])).join(',');
  });
  return [headerRow, ...rows].join('\n');
}

// 将 CSV 字符串解析为对象数组
function parseCsv(content: string): any[]
{
  if (!content.trim()) return [];
  const lines = content.split(/\r?\n/);
  const headers = lines[0].split(',').map(h => h.replace(/^"|"$/g, '').replace(/""/g, '"'));
  const data: any[] = [];

  for (let i = 1; i < lines.length; i++)
  {
    const line = lines[i];
    if (!line.trim()) continue;

    const values: string[] = [];
    let currentVal = '';
    let inQuotes = false;

    for (let j = 0; j < line.length; j++)
    {
      const char = line[j];
      if (char === '"')
      {
        if (inQuotes && line[j + 1] === '"')
        {
          currentVal += '"';
          j++; // 跳过下一个引号
        } else
        {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes)
      {
        values.push(currentVal);
        currentVal = '';
      } else
      {
        currentVal += char;
      }
    }
    values.push(currentVal); // 添加最后一个值

    const row = {};
    headers.forEach((header, index) =>
    {
      const value = values[index] || '';
      // 尝试解析值
      if (value === 'true')
      {
        row[header] = true;
      } else if (value === 'false')
      {
        row[header] = false;
      } else if (!isNaN(Number(value)) && value.trim() !== '' && !value.includes('e') && !value.includes('x'))
      {
        row[header] = Number(value);
      } else
      {
        try
        {
          // 尝试解析为 JSON (用于对象和数组)
          row[header] = JSON.parse(value);
        } catch (e)
        {
          row[header] = value;
        }
      }
    });
    data.push(row);
  }
  return data;
}


class CsvDBDriver extends Driver
{
  static name = 'csvdb';
  static inject = ['logger'];

  private _path: string; // 数据库目录路径
  private store: Dict<any[]> = Object.create(null); // 内存中的数据存储
  private _debounceTimers: Dict<NodeJS.Timeout> = Object.create(null); // 按表防抖写入的计时器
  private _transactionalStore: Dict<any[]> | null = null; // 用于事务的临时存储

  constructor(public ctx: Context, public config: CsvDBDriver.Config)
  {
    super(ctx, config);
    this.logger = ctx.logger(CsvDBDriver.name);
    this._path = resolve(ctx.baseDir, config.path);
  }

  async start()
  {
    // 确保目录存在
    await fs.mkdir(this._path, { recursive: true });
    try
    {
      // 读取目录下所有的 .csv 文件
      const files = await fs.readdir(this._path);
      for (const file of files)
      {
        if (file.endsWith('.csv'))
        {
          const tableName = file.slice(0, -4);
          try
          {
            const data = await fs.readFile(resolve(this._path, file), 'utf8');
            if (data) this.store[tableName] = parseCsv(data);
          } catch (err)
          {
            this.logger.warn('failed to read table %s: %s', tableName, err);
          }
        }
      }
    } catch (err)
    {
      this.logger.warn('failed to read database directory: %s', err);
    }
    // 监听 dispose 事件，确保程序退出时保存数据
    this.ctx.on('dispose', () => this.stop());
  }

  async stop()
  {
    // 停止时，将所有待写入的表立即写入磁盘
    const tables = Object.keys(this._debounceTimers);
    if (tables.length > 0)
    {
      await Promise.all(tables.map(table =>
      {
        clearTimeout(this._debounceTimers[table]);
        return this.flushTable(table);
      }));
    }
  }

  // 立即将单个表写入磁盘
  private async flushTable(table: string)
  {
    delete this._debounceTimers[table];
    // 如果在事务中，则不执行写入操作
    if (this._transactionalStore) return;

    const tablePath = resolve(this._path, `${table}.csv`);
    try
    {
      const tableData = this.store[table];
      if (tableData)
      {
        // 如果表有数据，则写入文件
        await fs.writeFile(tablePath, stringifyCsv(tableData));
      } else
      {
        // 如果表数据已不存在（被 drop），则删除对应的文件
        await fs.unlink(tablePath).catch(err =>
        {
          // 如果文件本身就不存在，忽略错误
          if (err.code !== 'ENOENT') throw err;
        });
      }
    } catch (err)
    {
      this.logger.warn('failed to write table %s: %s', table, err);
    }
  }

  // 防抖写入
  private write(table: string)
  {
    if (this._debounceTimers[table]) clearTimeout(this._debounceTimers[table]);
    this._debounceTimers[table] = setTimeout(() => this.flushTable(table), 1000);
  }

  async drop(table: string)
  {
    const store = this._transactionalStore || this.store;
    delete store[table];
    this.write(table);
  }

  async dropAll()
  {
    const store = this._transactionalStore || this.store;
    const tables = Object.keys(store);
    if (this._transactionalStore)
    {
      this._transactionalStore = Object.create(null);
    } else
    {
      this.store = Object.create(null);
    }
    // 将所有旧的表标记为待删除
    for (const table of tables)
    {
      this.write(table);
    }
  }

  async stats()
  {
    const stats: Driver.Stats = { size: 0, tables: {} };
    let totalSize = 0;
    try
    {
      const files = await fs.readdir(this._path);
      await Promise.all(files.map(async (file) =>
      {
        if (file.endsWith('.csv'))
        {
          const tablePath = resolve(this._path, file);
          try
          {
            const fileStats = await fs.stat(tablePath);
            totalSize += fileStats.size;
            const tableName = file.slice(0, -4);
            // _indexes 是内部表，不计入
            if (tableName !== '_indexes')
            {
              stats.tables[tableName] = {
                count: (this._transactionalStore || this.store)[tableName]?.length || 0,
                size: fileStats.size,
              };
            }
          } catch { /* a file may be deleted during stats() */ }
        }
      }));
    } catch (err)
    {
      if (err.code !== 'ENOENT')
      {
        this.logger.warn('failed to read database stats: %s', err);
      }
    }
    stats.size = totalSize;
    return stats;
  }

  async prepare(table: string)
  {
    const store = this._transactionalStore || this.store;
    if (!store[table as string])
    {
      this.logger.info('auto creating table %c', table);
      store[table as string] = [];
    }
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

    if (modifiedCount > 0) this.write(table);
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
      this.write(table);
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
    this.write(table);
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

    if (result.inserted > 0 || result.modified > 0) this.write(table);
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
      // 提交事务，找出变更的表并写入
      const oldStore = this.store;
      this.store = this._transactionalStore;
      const allKeys = new Set([...Object.keys(oldStore), ...Object.keys(this.store)]);
      for (const table of allKeys)
      {
        if (!deepEqual(oldStore[table], this.store[table]))
        {
          this.write(table);
        }
      }
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
    this.write('_indexes');
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
      this.write('_indexes');
    }
  }
}

namespace CsvDBDriver
{
  export interface Config
  {
    path: string;
  }

  export const Config: Schema<Config> = Schema.object({
    path: Schema.path({
      filters: ['directory'],
      allowCreate: true,
    }).description('数据库目录的路径。').default('data/database/csvdb'),
  });
}

export default CsvDBDriver;