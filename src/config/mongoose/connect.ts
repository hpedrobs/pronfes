import { set, connect } from "mongoose"
import env from "../env"

set('strictQuery', true)
connect(`${env.MONGO_URL}:${env.MONGO_PORT}/${env.MONGO_DB}`)
    .catch(error => console.error(error))
