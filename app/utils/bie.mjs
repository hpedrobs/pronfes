import sqlite3 from 'sqlite3'
import dotenv from 'dotenv'
dotenv.config()

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

    _checkSchema () {
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

        switch (type) {
        case 'text':
            types.push('TEXT')
            break
        case 'number':
            types.push('INT')
            break
        case 'boolean':
            types.push('BLOB')
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
            if (process.env.SQLITE_FILENAME) {
                return new sqlite3.Database(process.env.SQLITE_FILENAME, (error) => {
                    if (error) console.log(error)
                })
            } else {
                throw new Error('Database path name is incorrect or not defined!')
            }
        } catch (error) {
            console.log(error)
        }
    }

    _checkTable () {
        const db = this._db()

        db.run(`CREATE TABLE IF NOT EXISTS ${this._sname} ${this._query.columns}`, (error) => {
            if (error) this._bieError(error.message)
        })

        db.close()
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

    _checkValue (v) {
        if (typeof v === 'string') {
            return `'${v}'`
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
            conditions.push(`${key}=${this._checkValue(value)}`)
        }

        conditions = conditions.join(' AND ')

        return conditions
    }

    _checkQuery (query) {
        return new Promise((resolve) => {
            try {
                const columns = Object.keys(this._sform)

                if (Object.keys(query).length) {
                    for (const column of Object.keys(query)) {
                        if (columns.indexOf(column) === -1) {
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

    find (query = {}) {
        return new Promise((resolve) => {
            this._checkSchema()

            if (this._newSchema) {
                this._checkTable()

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
            }
        })
    }

    findOne (query = {}) {
        return new Promise((resolve) => {
            this._checkSchema()

            if (this._newSchema) {
                this._checkTable()

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
            }
        })
    }

    createOne (document) {
        return new Promise(resolve => {
            this._checkSchema()

            if (this._newSchema) {
                this._checkTable()

                const db = this._db()

                db.serialize(() => {
                    if (document && Object.values(document).length) {
                        const { KEYS, VALUES } = this._mapQuery(document)

                        const sql = `INSERT INTO ${this._sname} (${KEYS}) VALUES (${VALUES})`

                        db.run(sql, (error) => {
                            if (error) {
                                this._bieError(error.message)
                                resolve({ create: false })
                            } else {
                                resolve({ create: true })
                            }
                        })
                    }
                })

                db.close()
            }
        })
    }

    updateOne (query = {}, document = {}) {
        return new Promise(resolve => {
            this._checkSchema()

            if (
                this._newSchema &&
                this._checkQuery(query) &&
                this._checkQuery(document)
            ) {
                this._checkTable()

                const conditions = this._conditions(query)

                const values = this._mapDocument(document)

                const sql = `UPDATE ${this._sname} SET ${values} WHERE ${conditions}`

                const db = this._db()

                db.serialize(() => {
                    db.run(sql, (error, rows) => {
                        if (error) {
                            this._bieError(error)
                            resolve({ update: false })
                        }

                        resolve({ update: true })
                    })
                })

                db.close()
            }
        })
    }
}
