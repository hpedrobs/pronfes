export default class {
    _before (folder, filterValue) {
        const filterIndex = folder.indexOf(filterValue)
        if (filterIndex === 0) return true
        else return false
    }

    _middle (folder, filter) {
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

    _after (folder, filterValue) {
        return folder.substr(
            (folder.length - filterValue.length),
            filterValue.length
        ) === filterValue
    }

    check (folder, filter) {
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
