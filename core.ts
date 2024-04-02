import { Database } from 'better-sqlite3'

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
  function run() {
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
  }
  if (options.skipTransaction) {
    run()
  } else {
    db.transaction(run)()
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
  id: ID
  created_at?: string | null
  updated_at?: string | null
}

export function scanTableOverview<Row extends RowOverview>(options: {
  db: Database
  name: string

  /** @default 'id' */
  id?: string

  /** @default 'created_at' */
  created_at?: string | false

  /** @default 'updated_at' */
  updated_at?: string | false
}): Row[] {
  let { db, name } = options
  let id = options.id ?? 'id'
  let created_at = options.created_at ?? 'created_at'
  let updated_at = options.updated_at ?? 'updated_at'
  let fields = [id]
  if (created_at) fields.push(created_at)
  if (updated_at) fields.push(updated_at)
  let rows = db
    .prepare(
      /* sql */ `
select ${fields.map(wrapName)} from ${wrapName(name)}
`,
    )
    .all() as any[]
  return rows.map(row => {
    let res: Row = { id: row[id] } as Row
    if (created_at) {
      res.created_at = row[created_at]
    }
    if (updated_at) {
      res.updated_at = row[updated_at]
    }
    return res
  })
}

export function wrapName(name: string): string {
  return '`' + name + '`'
}
