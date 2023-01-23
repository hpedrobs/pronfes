import Root from "../schemas/Root"

export async function insertRootDir(pathname: string) : Promise<void> {
    const document : Object = { pathname }

    if (!await Root.exists(document)) {
        const root = new Root(document)
        root.save()
            .then(result => {
                console.log("Caminho inserido com sucesso!")
                console.log("Caminho: ", result.pathname)
            })
            .catch(err => console.error(err))
    }
}

export function showListRoots () : void {
    Root.find()
        .then(roots => roots.forEach(root => console.log(`Caminho: ${root.pathname}`)))
        .catch(err => console.error(err))
}
    
export async function deleteRootDir (pathname: string) : Promise<void> {
    const document = { pathname }
    if (await Root.exists(document)) {
        Root.deleteOne(document)
            .then(result => {
                if (result.acknowledged) {
                    console.log("Caminho excluído com sucesso!")
                    console.log("Caminho: ", pathname)
                }
            })
            .catch(err => console.error(err))
    } else {
        console.log("Esse caminho não existe!")
        console.log("Caminho: ", pathname)
    }
}
