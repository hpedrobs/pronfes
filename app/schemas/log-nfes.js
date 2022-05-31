import { Bie, Schema } from '../utils/bie.mjs'

const logNfes = new Schema({
    company: {
        type: 'number'
    },
    pathname: {
        type: 'text'
    },
    type: {
        type: 'text'
    },
    log: {
        type: 'longtext'
    },
    createAt: {
        type: 'text'
    }
})

const bie = new Bie()

bie.model('log_nfes', logNfes)

export default bie
