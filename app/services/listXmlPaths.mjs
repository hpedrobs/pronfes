import model from '../schemas/xmlpaths.js'

export default async () => {
    try {
        const xmls = await model.find({})

        for await (const xml of xmls) {
            console.log('\n- ', xml.pathname)
        }

        if (!xmls.length) console.log('\nNo registered path name!\n')
    } catch (error) {
        console.log(error)
    }
}
