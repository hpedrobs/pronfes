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
    async _add (pathname) {
        const filter = {
            company: this._company,
            pathname
        }

        const nfe = await outstandingNfes.findOne(filter)

        if (!nfe.length) {
            const document = {
                company: this._company,
                pathname,
                createAt: new Date().toJSON(),
                reprocess: '',
                period: `${this._year}/${this._month}`,
                active: true
            }

            const createNewDoc = await outstandingNfes.createOne(document)
            if (createNewDoc.createOne) {
                clg('Note added to queue!', 'success')
                console.log(`> ${pathname}\n`.gray)
            }
        }
    }

    async _processfile (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                await this._processfile(path.join(pathname, file.name))
            } else if (file.isFile()) {
                if (path.extname(file.name) === '.xml' || path.extname(file.name) === '.zip') {
                    await this._add(path.join(pathname, file.name))
                }
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

    async _processMonth (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                let access = true

                if (this._filters.monthStart || this._filters.monthEnd) {
                    access = this._checkMonth(file.name)
                }

                if (access) {
                    this._month = file.name
                    await this._processfile(path.join(pathname, file.name))
                }
            }
        }
    }

    _checkPeriod (y) {
        return this._filters.yearStart <= y && y <= this._filters.yearEnd
    }

    async _processYear (pathname) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                let access = true

                if (this._filters.yearStart && this._filters.yearEnd) {
                    access = this._checkPeriod(file.name)
                }

                if (access) {
                    this._year = file.name
                    await this._processMonth(path.join(pathname, file.name))
                }
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

                if (access) await this._processYear(path.join(pathname, file.name))
            }
        }
    }

    async exec (args) {
        this._filters = args

        const xmls = await xmlpaths.find({})

        for await (const xml of xmls) this._company(xml.pathname)

        setTimeout(async () => {
            const load = new Load()
            await load.exec(args)
        }, 30000)
    }
}
