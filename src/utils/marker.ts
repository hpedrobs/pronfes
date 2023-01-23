export default class {
    _before (folder:string, filterValue:string) {
        const filterIndex = folder.indexOf(filterValue)
        if (filterIndex === 0) return true
        else return false
    }

    _middle (folder:string, filter:string) {
        const filterValues = filter.split('%')

        if (filterValues.length) {
            const before = filterValues[0]
            const after = filterValues[1]

            const checkMarkerBefore = this._before(folder, before)
            const checkMarkerAfter = this._after(folder, after)

            if (checkMarkerBefore && checkMarkerAfter) {
                return true
            }
        }

        return false
    }

    _after (folder:string, filterValue:string) {
        return folder.substr(
            (folder.length - filterValue.length),
            filterValue.length
        ) === filterValue
    }

    check (folder:string, filter:string) {
        const filterValue = filter.replace('%', '')

        const marker = filter.indexOf('%')
        const existMarker = marker !== -1

        if (existMarker) {
            if (marker === 0) {
                const existFolder = this._after(folder, filterValue)
                if (existFolder) return true
            } else if (marker === (filter.length - 1)) {
                const existFolder = this._before(folder, filterValue)
                if (existFolder) return true
            } else {
                const existFolder = this._middle(folder, filter)
                if (existFolder) return true
            }
        } else if (filter === folder) {
            return true
        }

        return false
    }
}
