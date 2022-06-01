/* Internal */
import outstandingNfes from '../schemas/outstanding-nfes.js'
import logNfes from '../schemas/log-nfes.js'
import processedNfes from '../schemas/processed-nfes.js'
import clg from './clg.mjs'

/* Core Module */
import { spawn } from 'node:child_process'
import('dotenv/config')

export default class Work {
    async _fieldActive (id, value) {
        const updateNfe = await outstandingNfes.updateOne({ id }, { active: value })
        if (updateNfe.updateOne) {
            if (value) {
                clg('active note', 'info')
            } else {
                clg('inactive note', 'info')
            }
        } else {
            clg('problem to update the field: activate', 'error')
        }
    }

    async _remove (id) {
        const deleteNfe = await outstandingNfes.deleteOne({ id })

        if (deleteNfe.deleteOne) {
            clg('delete note', 'info')
        } else {
            clg('problem to delete note', 'error')
        }
    }

    async _insertLog (document) {
        const createDocument = await logNfes.createOne(document)
        if (createDocument.createOne) {
            clg('log created', 'info')
        } else {
            clg('problem creating the log', 'error')
        }
    }

    async _existProcess (company, pathname) {
        const findDocument = await processedNfes.findOne({ company, pathname })
        return findDocument.length
    }

    async _createProcess (document) {
        if (await this._existProcess(document.company, document.pathname)) {
            const updateDocument = await processedNfes.updateOne({
                company: document.company,
                pathname: document.pathname
            }, { updateAt: new Date().toJSON() })

            if (updateDocument.updateOne) {
                clg('Process completed successfully', 'success')
            } else {
                clg('Problem finishing the process', 'error')
            }
        } else {
            const createDocument = await processedNfes.createOne(document)
            if (createDocument.createOne) {
                clg('Process completed successfully', 'success')
            } else {
                clg('Problem finishing the process', 'error')
            }
        }
    }

    async _reprocess (company, pathname) {
        const updateDocument = await outstandingNfes.updateOne({ company, pathname }, {
            reprocess: new Date().toJSON()
        })

        if (updateDocument.updateOne) {
            clg('Reprocess the note', 'success')
        } else {
            clg('failed to reprocess the note', 'error')
        }
    }

    async _process (nfe) {
        return new Promise((resolve) => {
            const params = [
                'nfe.py',
                '"' + nfe.pathname + '"',
                `empresa="${nfe.company}"`,
                '-foldered'
            ]

            const nfepy = spawn('python3', params, {
                windowsHide: true,
                shell: true,
                cwd: process.env.CWD
            })

            nfepy.stdout.setEncoding('utf8')
            nfepy.stdout.on('data', (data) => {
                console.log(String(data))
            })

            this._errstd = false
            this._logErr = []
            nfepy.stderr.on('data', (err) => {
                console.log(String(err))
                this._errstd = true
                this._logErr.push(String(err))
            })

            nfepy.on('close', async () => {
                if (this._errstd) {
                    await this._insertLog({
                        company: nfe.company,
                        pathname: nfe.pathname,
                        createAt: new Date().toJSON(),
                        log: this._logErr.join(' '),
                        type: 'error'
                    })
                    await this._fieldActive(nfe.id, true)
                } else {
                    await this._createProcess({
                        company: nfe.company,
                        pathname: nfe.pathname,
                        createAt: new Date().toJSON(),
                        updateAt: '',
                        period: nfe.period
                    })
                    await this._remove(nfe.id)
                }
                resolve()
            })
        })
    }

    async exec () {
        const outstanding = await outstandingNfes.find({ active: true })

        for await (const nfe of outstanding) {
            clg(`process note: ${nfe.pathname}`)
            await this._fieldActive(nfe.id, false)
            await this._process(nfe)
        }
    }
}
