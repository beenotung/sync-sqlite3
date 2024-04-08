import type { Database } from 'better-sqlite3'
import type fs from 'fs'
import type path from 'path'

export type SchemaRow = {
  type: 'table' | 'index'
  name: string
  sql: string
}

export function scanSchema(db: Database) {
  let rows = db
    .prepare(
      /* sql */ `
select type, name, sql
from sqlite_master
where sql is not null
`,
    )
    .all()
  return rows as SchemaRow[]
}

export type SchemaDiff = {
  type: 'created' | 'updated' | 'deleted'
  schema: SchemaRow
}

export function compareSchema(options: {
  src: SchemaRow[]
  dest: SchemaRow[]
}): SchemaDiff[] {
  let diffs: SchemaDiff[] = []
  for (let src of options.src) {
    let { type, name } = src
    let dest = options.dest.find(row => row.type == type && row.name == name)
    if (!dest) {
      diffs.push({ type: 'created', schema: src })
      continue
    }
    if (dest.sql != src.sql) {
      diffs.push({ type: 'updated', schema: src })
      continue
    }
  }
  for (let dest of options.dest) {
    let { type, name } = dest
    let src = options.src.find(row => row.type == type && row.name == name)
    if (!src) {
      diffs.push({ type: 'deleted', schema: dest })
    }
  }
  return diffs
}

export function dropSchema(db: Database, schema: SchemaRow) {
  switch (schema.type) {
    case 'table':
      db.exec(`drop table ${wrapName(scanSchema.name)}`)
      break
    case 'index':
      db.exec(`drop index ${wrapName(scanSchema.name)}`)
      break
    default:
      throw new Error('unknown schema type: ' + schema.type)
  }
}

export function syncSchema(options: {
  db: Database
  diffs: SchemaDiff[]
  skipTransaction?: boolean
}) {
  let { db } = options
  run(db, options.skipTransaction, () => {
    for (let diff of options.diffs) {
      let { schema } = diff
      switch (diff.type) {
        case 'created':
          db.exec(schema.sql)
          break
        case 'updated':
          dropSchema(db, schema)
          db.exec(schema.sql)
          break
        case 'deleted':
          dropSchema(db, schema)
          break
        default:
          throw new Error('unknown schema diff type: ' + diff.type)
      }
    }
  })
}

function run(
  db: Database,
  skipTransaction: boolean | undefined,
  fn: () => void,
) {
  if (skipTransaction) {
    fn()
  } else {
    db.transaction(fn)()
  }
}

export function scanTableNames(db: Database): string[] {
  let rows = db
    .prepare(
      /* sql */ `
select name
from sqlite_master
where type = 'table'
  and sql is not null
`,
    )
    .pluck()
    .all()
  return rows as string[]
}

export type RowOverview<ID = number> = {
  id?: ID
  created_at?: string | null
  updated_at?: string | null
}

export function scanTableIds<Id = number>(options: {
  db: Database
  table: string
  /** @default 'id' */
  field?: string
}): Id[] {
  let { db, table } = options
  let field = getIdField(options.table, options.field)
  let ids = db
    .prepare(`select ${wrapName(field)} from ${wrapName(table)}`)
    .all() as Id[]
  return ids
}

function getIdField(table: string, field?: string): string {
  if (field) return field
  switch (table) {
    case 'sqlite_sequence':
      return 'name'
    case 'knex_migrations_lock':
      return 'index'
    default:
      return 'id'
  }
}

export type TableIdDiff<Id = number> = {
  created: Id[]
  deleted: Id[]
}

export function compareTableIds<Id = number>(options: {
  src: Id[]
  dest: Id[]
}): TableIdDiff<Id> {
  let created: Id[] = []
  let deleted: Id[] = []
  let src = new Set(options.src)
  let dest = new Set(options.dest)
  for (let id of src) {
    if (!dest.has(id)) {
      created.push(id)
    }
  }
  for (let id of dest) {
    if (!src.has(id)) {
      deleted.push(id)
    }
  }
  return { created, deleted }
}

export function syncTableIds<Id = number>(options: {
  db: Database
  table: string
  diff: TableIdDiff<Id>
  skipTransaction?: boolean
  /** @default 200 */
  /** @description 0 or false to do buck-delete all in once */
  batchSize?: number | false
}) {
  let { db, table } = options
  let batchSize = options.batchSize ?? 200
  let deleteStatement = db.prepare(
    `delete from ${wrapName(table)} where id in ?`,
  )
  function deleteAll() {
    if (!batchSize) {
      deleteStatement.run(options.diff.deleted)
      return
    }
    let buffer: Id[] = []
    for (let id of options.diff.deleted) {
      buffer.push(id)
      if (buffer.length > batchSize) {
        deleteStatement.run(buffer)
        buffer = []
      }
    }
    if (buffer.length > 0) {
      deleteStatement.run(buffer)
    }
  }
  function copyAll() {
    // TODO
  }
  run(db, options.skipTransaction, () => {
    deleteAll()
    copyAll()
  })
}

export function scanLastUpdate<T = string>(options: {
  db: Database
  table: string
  /** @default 'updated_at' */
  field?: string
}): T | null {
  let { db, table } = options
  let field = options.field ?? 'updated_at'
  let last_update = db
    .prepare(`select max(${wrapName(field)}) from ${wrapName(table)}`)
    .pluck()
    .get() as T | null
  return last_update
}

export function wrapName(name: string): string {
  return '`' + name + '`'
}

// TODO support incremental export
export function exportTableData(options: {
  fs: typeof fs
  path: typeof path
  db: Database
  name: string
  dir: string
}) {
  let { fs, path, db, name, dir } = options
  let file = path.join(dir, name)
  fs.writeFileSync(file, '')
  let n = db
    .prepare(`select count(*) from ${wrapName(name)}`)
    .pluck()
    .get() as number
  let i = 0
  let rows = db
    .prepare(`select * from ${wrapName(name)}`)
    .iterate() as Iterable<object>
  let lastP = 0
  let lastLine = ''
  for (let row of rows) {
    i++
    let p = (i * 100) / n
    if (p - lastP > 1) {
      lastLine = `\r ${i}/${n} (${p.toFixed(1)}%)`
      process.stdout.write(lastLine)
      lastP = p
    }
    let line = JSON.stringify(Object.values(row)) + '\n'
    fs.appendFileSync(file, line)
  }
  process.stdout.write(`\r${' '.repeat(lastLine.length)}\r`)
}
