import { Bie, Schema } from '../utils/bie.mjs'

const outstandingNfes = new Schema({
    company: {
        type: 'number'
    },
    pathname: {
        type: 'text'
    },
    createAt: {
        type: 'text'
    },
    updateAt: {
        type: 'text'
    },
    active: {
        type: 'boolean'
    }
})

const bie = new Bie()

bie.model('outstanding_nfes', outstandingNfes)

export default bie
