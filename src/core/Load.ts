// /* Internal */
import maker from "../utils/marker";
import Root, { IRoot } from "../schemas/Root"
import Pending, { IPending } from "../schemas/Pending";

/* Core Module */
import path from 'path'
import fs from 'fs'
import marker from "../utils/marker";

export interface IFilters {
    company: string;
    yearStart: number;
    monthStart: number;
    yearEnd: number;
    monthEnd: number;
    loop: boolean;
}

export default class Load {
    _filters: IFilters;
    _code: number;
    _year: number;
    _month: number;

    constructor () {
        this._filters = {
            company: String(),
            yearStart: Number(),
            monthStart: Number(),
            yearEnd: Number(),
            monthEnd: Number(),
            loop: Boolean()
        }
        this._code = Number()
        this._year = Number()
        this._month = Number()
    }

    async _add (filepath:string) {
        const filter = {
            company: this._code,
            filepath
        }

        if (!Boolean(await Pending.exists(filter))) {
            const newPending = new Pending({
                company: this._code,
                filepath,
                period: `${this._year}/${this._month}`
            })

            newPending.save()
                .then((result: IPending) => {
                    console.log("Nota inserida com sucesso!")
                    console.log("Nota: ", result.filepath)
                })
                .catch((err: any) => console.error(err))
        }
    }

    async _processfile (pathname:string) {
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

    _isMonth (month:number) {
        if (isNaN(month)) month = 0
        return month >= 1 && month <= 12
    }

    _checkMonth (month:number) {
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

    async _processMonth (pathname:string) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                let access = true

                if (this._filters.monthStart || this._filters.monthEnd) {
                    access = this._checkMonth(parseInt(file.name))
                }

                if (access) {
                    this._month = parseInt(file.name)
                    await this._processfile(path.join(pathname, file.name))
                }
            }
        }
    }

    _checkPeriod (y:number) : boolean {
        return this._filters.yearStart <= y && y <= this._filters.yearEnd
    }

    async _processYear (pathname:string) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                let access = true

                if (Boolean(this._filters.yearStart) && Boolean(this._filters.yearEnd)) {
                    access = this._checkPeriod(parseInt(file.name))
                }

                if (access) {
                    console.log(path.join(pathname, file.name))
                    this._year = parseInt(file.name)
                    await this._processMonth(path.join(pathname, file.name))
                }
            }
        }
    }

    async _setCodeCompany (comp:string) {
        const parts = comp.split('-')

        if (parts.length === 2) {
            this._code = parseInt(parts[1])
        }
    }

    async _company (pathname:string) {
        const files = fs.readdirSync(pathname, { withFileTypes: true })

        for await (const file of files) {
            if (file.isDirectory()) {
                this._setCodeCompany(file.name)

                let access = true

                if (this._filters.company) {
                    const mk = new marker()
                    const isCompany = mk.check(file.name, this._filters.company)
                    if (!isCompany) access = false
                }

                if (access) await this._processYear(path.join(pathname, file.name))
            }
        }
    }

    async exec (args: IFilters) {
        this._filters = args

        const roots: Array<IRoot> = await Root.find()

        for (const root of roots) await this._company(root.pathname)

        if (args.loop) {
            setTimeout(async () => {
                const load = new Load()
                await load.exec(args)
            }, 600000)
        }
    }
}
