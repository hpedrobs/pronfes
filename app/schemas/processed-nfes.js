import { Bie, Schema } from '../utils/bie.mjs'

const processedNfes = new Schema({
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
    period: {
        type: 'text'
    }
})

const bie = new Bie()

bie.model('processed_nfes', processedNfes)

export default bie
