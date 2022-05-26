import model from '../schemas/xmlpaths.js'

export default async function () {
    try {
        await model.dropTable()
    } catch (err) {
        console.log(err)
    }
}
