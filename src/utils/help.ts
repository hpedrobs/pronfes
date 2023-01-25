export default () : void => {
    console.log('\nUse:\n')
    console.log('   --insertRootDir=string          Realiza a inserção do caminho raiz que contém os arquivos .xml')
    console.log('   --listRoots=string              Mostra a lista de diretórios raiz')
    console.log('   --daleteRootDir=string          Remove um caminho raiz')
    console.log('   --loader                        Realiza a inserção do arquivo .xml na fila de processamento')
    console.log(`   --company=string                Filtra por nome das pastas de cada empresa. Existe um marcador para definir como filtro deve se posicionar. Exemplos de uso: filtro ou fil% ou %tro ou fi%ro`)
    console.log('   --period=string                 Filtra pelo perído inicial e final. Exemplos de uso: "2022/12-2022/01"')
    // console.log('   --deletePendings                Deletear todas as notas pendentes')
    console.log('   --work                          Processa os xmls da lista de arquivos')
    process.exit(9)
}
