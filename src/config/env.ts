import { config } from "dotenv"
import { join } from "path"

config({ path: join(__dirname, "../../.env") })

if (!('MONGO_URL' in process.env)) process.env.MONGO_URL = "mongodb://localhost"
else if (!('MONGO_PORT' in process.env)) process.env.MONGO_PORT = "27017"
else if (!('MONGO_DB' in process.env)) process.env.MONGO_DB = "nfe"

for (const key in process.env) {
    switch (key) {
        case 'MONGO_URL':
            console.log(`MONGO_URL: ${process.env.MONGO_URL}`)
            break

        case 'MONGO_PORT':
            console.log(`MONGO_PORT: ${process.env.MONGO_PORT}`)
            break

        case 'MONGO_DB':
            console.log(`MONGO_DB: ${process.env.MONGO_DB}`)
            break
    
        default:
            break
    }
}

export default process.env
