# -*- coding: utf-8 -*-
import codecs
import os
import sys

from subheaven.arg_parser import *


def show_help():
    print("Cria um arquivo de comando em batch para o script informado")
    print("")
    print("Exemplo:")
    print("    create_cmd buscar_lista_documentos.py")


@arg_parser(".".join(os.path.basename(__file__).split(".")[0:-1]), 'Cria um arquivo de comando em batch para o script informado')
@positional_param('arquivo', 'Nome do script a ser processado', sample='buscar_notas.py', required=True, special='filepicker')
def criar():
    if os.path.isfile(args[0]):
        code = "@echo off\n"
        code += f"set filename={args[0]}\n"
        code += "python3 %~dp0%filename% %*"
        cmd_path = ".".join(args[0].split(".")[0:-1])
        with codecs.open(f"{cmd_path}.cmd", "w+", "latin1") as file:
            file.write(code)
    else:
        print(f"O arquivo informado ({args[0]}) não pôde ser encontrado!")


if __name__ == "__main__":
    criar()
