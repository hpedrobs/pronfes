import sqlite3 from 'sqlite3'
import { config } from 'dotenv'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

config({
    path: path.join(__dirname, '../../.env')
})

export class Schema {
    constructor (form) {
        this.form = form
    }
}

export class Bie {
    get schema () {
        return {
            sname: this._sname,
            sform: this._sform
        }
    }

    model (name, schema) {
        this._sname = name || undefined
        this._sform = schema ? schema.form : undefined

        this._schema()
    }

    _bieError (error) {
        console.log('bie:', error)
    }

    _schemaParametersError (params) {
        params.forEach(parm => {
            if (parm === 'sname') {
                this._bieError('Schema name not defined!')
            } else if (parm === 'sform') {
                this._bieError('Schema shape not defined!')
            }
        })
    }

    async _checkSchema () {
        const sname = this._sname === undefined
        const sform = this._sform === undefined
        let definedProperty

        const undefinedProperty = []

        if (sname || sform) {
            if (sname) undefinedProperty.push('sname')
            if (sform) undefinedProperty.push('sform')

            definedProperty = false
        } else {
            definedProperty = true
        }

        if (undefinedProperty.length) {
            this._schemaParametersError(undefinedProperty)
        }

        this._newSchema = definedProperty
    }

    _typesColumn (type) {
        let types = []

        switch (type.toUpperCase()) {
        case 'TEXT':
            types.push('TEXT')
            break
        case 'NUMBER':
            types.push('INT')
            break
        case 'BOOLEAN':
            types.push('BLOB')
            break
        case 'LONGTEXT':
            types.push('LONGTEXT')
            break

        default:
            break
        }

        types = types.join(' ')

        return types
    }

    _mapAttr (shape) {
        let types; let unique = false

        if ('type' in shape) {
            types = this._typesColumn(shape.type)
        }

        if ('unique' in shape && shape.unique) {
            unique = true
        }

        return {
            types,
            unique
        }
    }

    _colTypes = []
    _colExclusive = []
    _query = {
        columns: ''
    }

    _setAttrs (name, types) {
        this._colTypes[name] = types
        this._colExclusive.push(name)
    }

    _insertValue (types) {
        this._query.columns = '(' + types.join(', ') + ')'
    }

    // _insertUnique() {
    //     if (this._colExclusive.length) {
    //         this._query.unique = 'UNIQUE('+this._colExclusive.join(', ')+')';
    //     }
    // }

    _setQuery () {
        const types = ['id INTEGER PRIMARY KEY']

        for (const [name, type] of Object.entries(this._colTypes)) {
            types.push(`${name} ${type}`)
        }

        this._insertValue(types)
    }

    _formColumns () {
        for (const [name, shape] of Object.entries(this._sform)) {
            const attrs = this._mapAttr(shape)
            this._setAttrs(name, attrs.types)
        }
        this._setQuery()
    }

    _schema () {
        if (this._sname !== undefined && this._sform !== undefined) {
            this._formColumns()
        }
    }

    _db () {
        try {
            const filename = process.env.SQLITE_FILENAME ? process.env.SQLITE_FILENAME : path.resolve(__dirname, '../../database/nfe.sqlite')
            return new sqlite3.Database(filename, (error) => {
                if (error) console.log(error)
            })
        } catch (error) {
            console.log(error)
        }
    }

    _checkTable () {
        return new Promise(resolve => {
            const db = this._db()

            db.run(`CREATE TABLE IF NOT EXISTS ${this._sname} ${this._query.columns}`, (error) => {
                if (error) {
                    this._bieError(error.message)
                    resolve()
                } else {
                    resolve()
                }
            })

            db.close()
        })
    }

    dropTable () {
        return new Promise(resolve => {
            try {
                const db = this._db()

                db.run(`DELETE FROM ${this._sname}`, (error) => {
                    if (error) console.log(error)
                })

                db.close()

                resolve()
            } catch (error) {
                this._bieError(error)
                resolve()
            }
        })
    }

    _mapStr (s) {
        return s.split('\'').join('')
    }

    _checkValue (v) {
        if (typeof v === 'string') {
            return `'${this._mapStr(v)}'`
        } else {
            return v
        }
    }

    _mapQuery (query) {
        let KEYS = []
        let VALUES = []

        for (const [key, value] of Object.entries(query)) {
            KEYS.push(key)
            VALUES.push(this._checkValue(value))
        }

        KEYS = KEYS.join(', ')
        VALUES = VALUES.join(', ')

        return {
            KEYS,
            VALUES
        }
    }

    _mapDocument (document) {
        let values = []

        for (const [key, value] of Object.entries(document)) {
            values.push(`${key}=${this._checkValue(value)}`)
        }

        values = values.join(', ')

        return values
    }

    _conditions (query) {
        let conditions = []

        for (const [key, value] of Object.entries(query)) {
            if (key === 'id') {
                conditions.push(`${key}=${this._checkValue(value)}`)
                break
            } else {
                conditions.push(`${key}=${this._checkValue(value)}`)
            }
        }

        conditions = conditions.join(' AND ')

        return conditions
    }

    async _checkQuery (query) {
        return new Promise((resolve) => {
            try {
                const columns = Object.keys(this._sform)

                if (Object.keys(query).length) {
                    for (const column of Object.keys(query)) {
                        if (column !== 'id' && columns.indexOf(column) === -1) {
                            throw new Error(`${column} column does not exist`)
                        }
                    }
                } else {
                    throw new Error('no column found')
                }

                resolve(true)
            } catch (error) {
                this._bieError(error.message)
                resolve(false)
            }
        })
    }

    async find (query = {}) {
        await this._checkSchema()

        if (this._newSchema) {
            await this._checkTable()

            return new Promise((resolve) => {
                const conditions = this._conditions(query)

                if (conditions) {
                    const db = this._db()

                    db.serialize(() => {
                        const sql = `SELECT * FROM ${this._sname} WHERE ${conditions}`

                        db.all(sql, (error, rows) => {
                            if (error) {
                                this._bieError(error.message)
                                resolve([])
                            } else {
                                resolve(rows)
                            }
                        })
                    })

                    db.close()
                } else {
                    const db = this._db()

                    db.serialize(() => {
                        const sql = `SELECT * FROM ${this._sname}`

                        db.all(sql, (error, rows) => {
                            if (error) {
                                this._bieError(error.message)
                                resolve([])
                            } else {
                                resolve(rows)
                            }
                        })
                    })

                    db.close()
                }
            })
        } else {
            return []
        }
    }

    async findOne (query = {}) {
        await this._checkSchema()

        if (this._newSchema) {
            await this._checkTable()

            return new Promise((resolve) => {
                const conditions = this._conditions(query)

                const db = this._db()

                db.serialize(() => {
                    const sql = `SELECT * FROM ${this._sname} WHERE ${conditions}`

                    db.all(sql, (error, rows) => {
                        if (error) {
                            this._bieError(error.message)
                            resolve([])
                        } else {
                            resolve(rows)
                        }
                    })
                })

                db.close()
            })
        } else {
            return []
        }
    }

    async createOne (document = {}) {
        await this._checkSchema()

        if (this._newSchema) {
            await this._checkTable()

            return new Promise(resolve => {
                const db = this._db()

                db.serialize(() => {
                    if (document && Object.values(document).length) {
                        const { KEYS, VALUES } = this._mapQuery(document)

                        const sql = `INSERT INTO ${this._sname} (${KEYS}) VALUES (${VALUES})`

                        db.run(sql, (error) => {
                            if (error) {
                                this._bieError(error.message)
                                resolve({ createOne: false })
                            } else {
                                resolve({ createOne: true })
                            }
                        })
                    }
                })

                db.close()
            })
        } else {
            return { createOne: false }
        }
    }

    async updateOne (query = {}, document = {}) {
        await this._checkSchema()

        const isQuery = await this._checkQuery(query)
        const isDocument = await this._checkQuery(document)

        if (this._newSchema && isQuery && isDocument) {
            await this._checkTable()

            return new Promise(resolve => {
                const conditions = this._conditions(query)

                const values = this._mapDocument(document)

                const sql = `UPDATE ${this._sname} SET ${values} WHERE ${conditions}`

                const db = this._db()

                db.serialize(() => {
                    db.run(sql, (error) => {
                        if (error) {
                            this._bieError(error)
                            resolve({ updateOne: false })
                        } else {
                            resolve({ updateOne: true })
                        }
                    })
                })

                db.close()
            })
        } else {
            return { updateOne: false }
        }
    }

    async deleteOne (query = {}) {
        await this._checkSchema()

        if (this._newSchema) {
            await this._checkTable()

            return new Promise(resolve => {
                const conditions = this._conditions(query)

                const sql = `DELETE FROM ${this._sname} WHERE ${conditions}`

                const db = this._db()

                db.serialize(() => {
                    db.run(sql, (error) => {
                        if (error) {
                            this._bieError(error)
                            resolve({ deleteOne: false })
                        } else {
                            resolve({ deleteOne: true })
                        }
                    })
                })

                db.close()
            })
        } else {
            return { updateOne: false }
        }
    }
}
