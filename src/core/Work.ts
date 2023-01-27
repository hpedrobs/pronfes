import { ObjectId } from "mongoose"
import Pending, { IPending } from "../schemas/Pending"
import Log from "../schemas/Log"
import { spawn } from "child_process"
import dotenv from "dotenv"
import path from "path"
import Hist, { IHist } from "../schemas/Hist"
import mongoose from "mongoose"

dotenv.config({ path: path.join(__dirname, "../../.env") })

export class Work {
    _miss: Array<string>

    constructor() {
        this._miss = new Array(0)
    }

    _process(nfe: IPending): Promise<void> {
        return new Promise((resolve, reject) => {
            if (process.env.CWD) {
                const params = []

                params.push("nfe.py")
                params.push("-foldered")
                params.push("-empresa=" + nfe.company)
                params.push(path.resolve(nfe.filepath))

                const pro = spawn("python3", params, {
                    cwd: process.env.CWD
                })

                pro.stdout.setEncoding('utf8')

                pro.stdout.on('data', data => {
                    data = data.toString()
                    console.log(data)
                })

                pro.stderr.on('data', data => {
                    data = data.toString()
                    this._miss.push(data)
                    console.log(data)
                })

                pro.on('close', code => {
                    if (this._miss.length) {
                        const log = new Log({
                            company: nfe.company,
                            filepath: nfe.filepath,
                            log: new Error(this._miss.join("")),
                            type: "error"
                        })

                        log.save()
                            .then(async res => {
                                await this._active(nfe._id, false)
                                resolve()
                            })
                            .catch(err => {
                                console.error(err)
                                reject()
                            })
                    } else {
                        Pending.findByIdAndDelete({ _id: nfe._id })
                            .then(res => {
                                console.log("Nota processada com sucesso!")

                                Hist.updateOne({ company: res?.company, filepath: res?.filepath, period: res?.period }, {
                                    company: res?.company,
                                    filepath: res?.filepath,
                                    period: res?.period,
                                }, { upsert: true })
                                    .then(() => resolve())
                                    .catch(err => {
                                        console.error(err)
                                        reject()
                                    })

                            })
                            .catch(err => {
                                console.error(err)
                                reject()
                            })
                    }
                })
            } else {
                const log = new Log({
                    company: nfe.company,
                    filepath: nfe.filepath,
                    log: new Error("Undefined executable path"),
                    type: "error"
                })

                log.save()
                    .then(async res => {
                        console.error(res.log)
                        await this._active(nfe._id, false)
                        resolve()
                    })
                    .catch(err => {
                        console.error(err)
                        reject()
                    })
            }
        })
    }

    _active(_id: ObjectId, active: Boolean): Promise<void> {
        return new Promise((resolve, reject) => {
            Pending.findByIdAndUpdate({ _id }, { active })
                .then(res => {
                    if (active) {
                        console.log("Nota preparada para o processamento")
                        console.log("Caminho do arquivo:", res?.filepath)
                    } else {
                        console.log("Nota de volta à fila")
                    }
                    resolve()
                })
                .catch(err => {
                    console.log(err)
                    reject()
                })
        })
    }

    async exec(): Promise<void> {

        const pendings = await Pending.aggregate([
            { $sort: { updatedAt: 1 } }
        ],
            { allowDiskUse: true }
        )

        for await (const nfe of pendings) {
            await this._active(nfe._id, true)
            await this._process(nfe)
        }

        setTimeout(() => {
            if (mongoose.connection.readyState === 1) {
                this.exec()
            } else {
                throw new Error("Falha na conexão com o banco de dados")
            }
        }, 100)
    }
}