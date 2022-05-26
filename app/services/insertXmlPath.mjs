import model from '../schemas/xmlpaths.js'

export default async function (pathname) {
    try {
        await model.createOne({ pathname })
    } catch (err) {
        console.log(err)
    }
}
