import { Bie, Schema } from '../utils/bie.mjs'

const xmlPathsSchema = new Schema({
    pathname: {
        type: 'text'
    }
})

const bie = new Bie()

bie.model('xml_paths', xmlPathsSchema)

export default bie
