/* Internal */
import path from 'path'
import outstandingNfes from '../schemas/outstanding-nfes.js'
import clg from './clg.mjs'

/* Core Module */
import fs from 'fs'

export default class Work {
    async _remove (id) {
    }

    async _process (nfe) {
    }

    async _inactive (id) {
        const updateDocument = await outstandingNfes.updateOne({ id }, { active: false })
        if (updateDocument.update) {
            clg('inactive note', 'info')
        } else {
            clg('problem to inactivate the note', 'error')
        }
    }

    async exec () {
        const outstanding = await outstandingNfes.find({ active: true })

        for await (const nfe of outstanding) {
            clg(`process note: ${nfe.pathname}`)
            await this._inactive(nfe.id)
            await this._process(nfe)
        }
    }
}
