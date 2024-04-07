import { Database } from 'better-sqlite3'
import { Field, Table } from 'quick-erd/dist/core/ast'
import { parseTableSchema } from 'quick-erd/dist/db/sqlite-parser'

export type SchemaRow = {
  type: 'table' | 'index'
  name: string
  sql: string
}

export type Schema = {
  tables: Table[]
  rows: SchemaRow[]
}

export function scanSchema(db: Database): Schema {
  let rows = db
    .prepare(
      /* sql */ `
select type, name, sql
from sqlite_master
where sql is not null
`,
    )
    .all() as SchemaRow[]
  let tables = parseTableSchema(rows)
  return { tables, rows }
}

export type SchemaDiff =
  | { type: 'create-table'; table: Table }
  | { type: 'drop-table'; name: string }
  | { type: 'alter-table'; changes: FieldDiff[] }

export type FieldDiff =
  | { type: 'add-column'; field: Field }
  | { type: 'drop-column'; name: string }
  | { type: 'alter-column'; field: Field }

export function compareSchema(options: {
  src: Schema
  dest: Schema
}): SchemaDiff[] {
  let diffs: SchemaDiff[] = []
  for (let srcTable of options.src.tables) {
    let destTable = options.dest.tables.find(
      table => table.name == srcTable.name,
    )
    if (!destTable) {
      diffs.push({ type: 'create-table', table: srcTable })
      continue
    }
    if (!isSame(srcTable, destTable)) {
      diffs.push({
        type: 'alter-table',
        changes: compareTable({ src: srcTable, dest: destTable }),
      })
      continue
    }
  }
  for (let destTable of options.dest.tables) {
    let srcTable = options.src.tables.find(
      table => table.name == destTable.name,
    )
    if (!srcTable) {
      diffs.push({ type: 'drop-table', name: destTable.name })
    }
  }
  return diffs
}

function compareTable(options: { src: Table; dest: Table }): FieldDiff[] {
  let diffs: FieldDiff[] = []
  for (let srcField of options.src.field_list) {
    let destField = options.dest.field_list.find(
      field => field.name == srcField.name,
    )
    if (!destField) {
      diffs.push({ type: 'add-column', field: srcField })
      continue
    }
    srcField = {...srcField, is_null:true}
    destField = {...destField, is_null:true}
    if (!isSame(srcField, destField)) {
      diffs.push({ type: 'alter-column', field: srcField })
      continue
    }
    for (let destField of options.dest.field_list) {
      let srcField = options.src.field_list.find(
        field => field.name == destField.name,
      )
      if (!srcField) {
        diffs.push({ type: 'drop-column', name: destField.name })
      }
    }
  }
  return diffs
}

function isSame<T>(a: T, b: T): boolean {
  let aStr = JSON.stringify(a)
  let bStr = JSON.stringify(b)
  return aStr == bStr
}

export function dropSchema(db: Database, name:string) {
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
      switch (diff.type) {
        case 'create-table':
          db.exec(schema.sql)
          break
        case 'alter-table':
          dropSchema(db, schema)
          db.exec(schema.sql)
          break
        case 'drop-table':
          dropSchema(db, diff.name)
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
