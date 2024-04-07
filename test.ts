import fs from 'fs'
import path from 'path'
import Database from 'better-sqlite3'
import {
  scanSchema,
  compareSchema,
  syncSchema,
  scanTableOverview,
  scanTableNames,
  exportTableData,
} from './core'

let src = Database('res/exp.sqlite3')
let dest = Database('res/mirror.sqlite3')

let diffs = compareSchema({
  src: scanSchema(src),
  dest: scanSchema(dest),
})

syncSchema({ db: dest, diffs })

let dir = 'res/data'
fs.mkdirSync(dir, { recursive: true })
let tableNames = scanTableNames(src)
for (let name of tableNames) {
  // let overview = scanTableOverview({ db: src, name })
  // console.log('overview:', { name, len: overview.length })
  exportTableData({ fs, path, db: src, name, dir })
}
