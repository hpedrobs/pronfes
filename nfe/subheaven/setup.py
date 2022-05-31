# -*- coding: utf-8 -*-
import codecs
import os
import shutil
import sys

from folders import check_folder

def copy_files(origin, destination):
    check_folder(destination)
    for item in os.listdir(origin):
        if item not in ['.git', '__pycache__']:
            print(f"    {item}")
            o = os.path.join(origin, item)
            if os.path.isfile(o):
                d = os.path.join(destination, item)
                shutil.copyfile(o, d)
            elif os.path.isdir(o):
                d = os.path.join(destination, item)
                copy_files(o, d)
            else:
                print("        I don't know what's thaaaat!")

def config_path(destination):
    cli_path = os.path.join(destination, 'cli')
    if not cli_path in os.environ['PATH']:
        os.system(f"setx /M path \"%path%;{cli_path}\"")

def atualizar_codigo():
    os.system('git pull')

def atualizar():
    print("Atualizar código:")
    atualizar_codigo()
    origin = os.path.abspath(os.path.dirname(__file__))
    destination = os.path.join(os.path.dirname(os.__file__), 'site-packages', 'subheaven')
    print("Atualizando arquivos:")
    copy_files(origin, destination)
    print("configurando PATH:")
    config_path(destination)

if __name__ == "__main__":
    atualizar()