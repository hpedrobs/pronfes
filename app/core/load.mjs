/* Internal */
import xmlpaths from '../schemas/xmlpaths.js'
import Marker from '../utils/advanced-marker.mjs'
import path from 'path'
import outstandingNfes from '../schemas/outstanding-nfes.js'
import clg from './clg.mjs'

/* Core Module */
import fs from 'fs'

const mk = new Marker()

export default class Load {
    async _alertNote (pathname) {
        clg('Note added to queue!', 'success')
        console.log(`> ${pathname}\n`.gray)
    }

    async _add (pathname) {
        const filter = {
            company: this._company,
            pathname
        }

        const nfe = await outstandingNfes.findOne(filter)

        if (nfe.length) {
            const document = {
                company: this._company,
                pathname,
                updateAt: new Date().toJSON(),
                active: true
            }

            const updDoc = await outstandingNfes.updateOne(filter, document)
            if (updDoc.update) await this._alertNote(document.pathname)
        } else {
            const document = {
                company: this._company,
                pathname,
                createAt: new Date().toJSON(),
                updateAt: '',
                active: true
            }

            const createNewDoc = await outstandingNfes.createOne(document)
            if (createNewDoc.create) await this._alertNote(document.pathname)
        }
    }

    async _file (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                setTimeout(async () => {
                    await this._file(path.join(pathname, file.name))
                }, 500)
            } else if (file.isFile()) {
                if (
                    path.extname(file.name) === '.xml' ||
                    path.extname(file.name) === '.zip'
                ) await this._add(path.join(pathname, file.name))
            }
        }
    }

    _isMonth (month) {
        if (isNaN(month)) month = 0
        return month >= 1 && month <= 12
    }

    _checkMonth (month) {
        month = parseInt(month)
        this._filters.monthStart = parseInt(this._filters.monthStart)
        this._filters.monthEnd = parseInt(this._filters.monthEnd)

        if (
            !this._isMonth(month) &&
            !this._isMonth(this._filters.monthStart) &&
            !this._isMonth(this._filters.monthEnd)
        ) return false

        if (
            !(this._filters.monthStart <= month) ||
            !(month <= this._filters.monthEnd)
        ) return false

        return true
    }

    async _month (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                let access = true

                if (this._filters.monthStart || this._filters.monthEnd) {
                    access = this._checkMonth(file.name)
                }

                if (access) await this._file(path.join(pathname, file.name))
            }
        }
    }

    _checkPeriod (y) {
        return this._filters.yearStart <= y && y <= this._filters.yearEnd
    }

    async _year (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                let access = true

                if (this._filters.yearStart && this._filters.yearEnd) {
                    access = this._checkPeriod(file.name)
                }

                if (access) await this._month(path.join(pathname, file.name))
            }
        }
    }

    async _setCodeCompany (comp) {
        const parts = comp.split('-')

        if (parts.length === 2) {
            this._company = parseInt(parts[1])
        }
    }

    async _company (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                this._setCodeCompany(file.name)

                let access = true

                if (this._filters.company) {
                    const isCompany = mk.check(file.name, this._filters.company)
                    if (!isCompany) access = false
                }

                if (access) await this._year(path.join(pathname, file.name))
            }
        }
    }

    async exec (args) {
        this._filters = args

        const xmls = await xmlpaths.find({})

        for await (const xml of xmls) await this._company(xml.pathname)
    }
}
