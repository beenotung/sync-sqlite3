import fs from 'fs'
import path from 'path'
import Database from 'better-sqlite3'
import {
  scanSchema,
  compareSchema,
  syncSchema,
  scanTableNames,
  exportTableData,
  scanTableIds,
  compareTableIds,
} from './core'

let srcDB = Database('res/exp.sqlite3')
let destDB = Database('res/mirror.sqlite3')

let schemaDiffs = compareSchema({
  src: scanSchema(srcDB),
  dest: scanSchema(destDB),
})

syncSchema({ db: destDB, diffs: schemaDiffs })

let dir = 'res/data'
fs.mkdirSync(dir, { recursive: true })
let tableNames = scanTableNames(srcDB)
for (let table of tableNames) {
 let idDiffs= compareTableIds({
    src:scanTableIds({db:srcDB,table}),
    dest:scanTableIds({db:srcDB,table}),
  })
}

console.log('done.')
