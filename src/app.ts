import help from "./utils/help"
import minimist from "minimist"
import { insertRootDir, showListRoots, deleteRootDir } from "./services/rootDir"
import Load, { IFilters } from "./core/Load"
import { Work } from "./core/Work"

(async function () {
    const args = minimist(process.argv.slice(2))
    
    if ('insertRootDir' in args && args.insertRootDir) {
        await insertRootDir(args.insertRootDir)
    } else if ('listRoots' in args && args.listRoots) {
        showListRoots()
    } else if ('deleteRootDir' in args && args.deleteRootDir) {
        await deleteRootDir(args.deleteRootDir)
    } else if ('loader' in args && args.loader) {
        const loader = new Load()

        const attrs : IFilters = {
            company: '',
            yearStart: 0,
            monthStart: 0,
            yearEnd: 0,
            monthEnd: 0
        }

        if ('period' in args && args.period) {
            const periods = args.period.split("-")
            periods.forEach((p: String, idx: Number) => {
                attrs[idx === 0 ? 'yearStart' : 'yearEnd'] = parseInt(p.substring(0, 4))
                attrs[idx === 0 ? 'monthStart' : 'monthEnd'] = parseInt(p.substring(5, 7))
            })
        }

        if ('company' in args && args.company) attrs['company'] = args.company

        loader.exec(attrs)
    } else if ('work' in args && args.work) {
        const work = new Work()
        work.exec()
    } else if (('help' in args && args.help) || ('h' in args && args.h)) {
        help()
    }
})()
