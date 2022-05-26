import model from '../schemas/outstanding-nfes.js'

export default async function () {
    try {
        await model.dropTable()
    } catch (err) {
        console.log(err)
    }
}
