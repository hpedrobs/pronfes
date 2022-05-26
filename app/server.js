import minimist from 'minimist'
import insertXmlPath from './services/insertXmlPath.mjs'
import listXmlPaths from './services/listXmlPaths.mjs'
import dropXmlPath from './services/clearXmlPath.mjs'
import Load from './core/load.mjs'
import Work from './core/work.mjs'
import dropOutstandingNfes from './services/clearOutstandingNfes.mjs'
import help from './utils/help.mjs'

(async () => {
    const args = minimist(process.argv.slice(2))

    if ('insertXmlPath' in args && args.insertXmlPath) {
        await insertXmlPath(args.insertXmlPath)
    } else if ('listXmlPaths' in args && args.listXmlPaths) {
        await listXmlPaths()
    } else if ('removeXmlPaths' in args && args.removeXmlPaths) {
        await dropXmlPath()
    } else if ('processLoader' in args && args.processLoader) {
        const attrs = {}

        if ('company' in args && args.company) attrs.company = args.company
        if ('yearStart' in args && args.yearStart) attrs.yearStart = args.yearStart
        if ('monthStart' in args && args.monthStart) attrs.monthStart = args.monthStart
        if ('yearEnd' in args && args.yearEnd) attrs.yearEnd = args.yearEnd
        if ('monthEnd' in args && args.monthEnd) attrs.monthEnd = args.monthEnd
        if ('months' in args && args.months) attrs.months = args.months

        const processLoader = new Load()
        await processLoader.exec(args)
    } else if ('processWork' in args && args.processWork) {
        const processWork = new Work()
        await processWork.exec(args)
    } else if ('removeOutstandingNfes' in args && args.removeOutstandingNfes) {
        await dropOutstandingNfes()
    } else if (('help' in args && args.help) || ('h' in args && args.h)) {
        help()
    }
})()
