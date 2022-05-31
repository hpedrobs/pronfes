# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))

import functools
import inspect
import json
import tools

from subheaven.input_directory import *
from subheaven.input_openfile import *
from subheaven.input_savefile import *

def arg_parser(name, description):
    global iacon_arg_config
    if not 'iacon_arg_config' in globals():
        iacon_arg_config = {
            'description': description,
            'positional': [],
            'named': {},
            'boolean': {},
            'params': {
                'positional': [],
                'named': {},
                'boolean': {}
            },
            'validated': True,
            'show_help': False,
            'argv0': sys.argv[0]
        }
        del sys.argv[0]
        if len(sys.argv) == 1:
            if sys.argv[0].strip() in ['-h', '-help', 'help', '-ajuda', 'ajuda', '?']:
                iacon_arg_config['show_help'] = True
                del sys.argv[0]
    def decorator_arg_parser(func):
        def show_help():
            print(name)
            print(f"    {iacon_arg_config['description']}")
            print("")
            print("Exemplos:")
            base_com = name
            opt_pos = []
            base_nam = ""
            opt_nam = []
            solo_nam = []
            for i in range(len(iacon_arg_config['positional'])):
                p = iacon_arg_config['positional'][i]
                if p['required']:
                    base_com += f" \"{p['sample']}\""
                else:
                    opt_pos.append(f"\"{p['sample']}\"")
            for k in iacon_arg_config['named']:
                p = iacon_arg_config['named'][k]
                if p['required']:
                    base_nam += f" {k}=\"{p['sample']}\""
                elif p['solo']:
                    solo_nam.append(f" {k}=\"{p['sample']}\"")
                else:
                    opt_nam.append(f"{k}=\"{p['sample']}\"")
            for k in iacon_arg_config['boolean']:
                p = iacon_arg_config['boolean'][k]
                if p['required']:
                    base_nam += f"-{k}"
                elif p['solo']:
                    solo_nam.append(f" --{k}")
                else:
                    opt_nam.append(f"--{k}")
            print(f"    {base_com} {base_nam}")
            for item in solo_nam:
                print(f"    {base_com} {item}")
            for item in opt_pos:
                base_com += f" {item}"
                print(f"    {base_com} {base_nam}")
            for item in opt_nam:
                base_nam += f" {item}"
                print(f"    {base_com} {base_nam}")
            
            print("")
            print("Parâmetros posicionais:")
            for i in range(len(iacon_arg_config['positional'])):
                p = iacon_arg_config['positional'][i]
                opt = f"(Optional. Default=\"{p['default']}\")" if not p['required'] else ''
                if 'options' in p and len(p['options']) > 0:
                    print(f"    ({str(i+1)}) {p['name']}: {opt} {p['description']} Opções aceitas:")
                    for option in p['options']:
                        print(f"        - {option}")
                else:
                    print(f"    ({str(i+1)}) {p['name']}: {opt} {p['description']}")
            print("")
            print("Parâmetros nomeados:")
            for n in iacon_arg_config['named']:
                p = iacon_arg_config['named'][n]
                opt = f"(Optional. Default=\"{p['default']}\")" if not p['required'] else ''
                if 'options' in p and len(p['options']) > 0:
                    print(f"    {p['name']}: {opt} {p['description']} Opções aceitas:")
                    for option in p['options']:
                        print(f"        - {option}")
                else:
                    print(f"    {p['name']}: {opt} {p['description']}")
            print("")
            print("Parâmetros booleanos:")
            for n in iacon_arg_config['boolean']:
                p = iacon_arg_config['boolean'][n]
                opt = f"(Optional. Default=False)" if not p['required'] else ''
                print(f"    {p['name']}: {opt} {p['description']}")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if 'iacon_arg_config' in globals():
                global iacon_arg_config
            if '--doc' in sys.argv or '-doc' in sys.argv:
                print(json.dumps(iacon_arg_config, indent=4))
                sys.exit(0)
            if iacon_arg_config['validated'] and not iacon_arg_config['show_help']:
                func.__globals__['args'] = iacon_arg_config['params']['positional']
                func.__globals__['params'] = iacon_arg_config['params']['named']
                for k in iacon_arg_config['params']['boolean']:
                    func.__globals__['params'][k] = iacon_arg_config['params']['boolean'][k]
                sys.argv.insert(0, iacon_arg_config['argv0'])
                return func(*args, **kwargs)
            else:
                return show_help()
        return wrapper
    return decorator_arg_parser

def positional_param(name, description, required=False, default="", sample="", label="", options=[], special=""):
    if sample == "":
        sample = default if default != "" else name

    global iacon_arg_config
    if not 'iacon_arg_config' in globals():
        iacon_arg_config = {
            'description': 'Informar a descrição do script com o decorator @parser_description',
            'positional': [{
                "name": name,
                "description": description,
                'required': required,
                'default': default,
                'sample': sample,
                'label': label,
                'options': options
            }],
            'named': {},
            'boolean': {},
            'params': {
                'positional': [],
                'named': {
                    name: default
                },
                'boolean': {}
            },
            'validated': True,
            'show_help': False,
            'argv0': sys.argv[0]
        }
        del sys.argv[0]
        if len(sys.argv) == 1:
            if sys.argv[0].strip() in ['-h', '-help', 'help', '-ajuda', 'ajuda', '?']:
                iacon_arg_config['show_help'] = True
                del sys.argv[0]
    else:
        iacon_arg_config['positional'].append({
            "name": name,
            "description": description,
            'required': required,
            'default': default,
            'sample': sample,
            'label': label,
            'special': special,
            'options': options
        })
        iacon_arg_config['params']['named'][name] = default

    arg_found = False
    if len(sys.argv) > 0:
        for i in range(len(sys.argv)):
            if not "-" == sys.argv[i][0]:
                pi = len(iacon_arg_config['positional']) - 1
                if len(iacon_arg_config['positional'][pi]['options']) > 0 and not sys.argv[i] in iacon_arg_config['positional'][pi]['options']:
                    iacon_arg_config['validated'] = False
                    print(f"Opção inválida para o parâmetro {name}.")
                    print("")
                elif special == "filepicker":
                    if sys.argv[i] != "" and os.path.isfile(sys.argv[i]):
                        iacon_arg_config['params']['positional'].append(sys.argv[i])
                        iacon_arg_config['params']['named'][name] = sys.argv[i]
                        arg_found = True
                        del sys.argv[i]
                    else:
                        print(f"O arquivo informado no parâmetro {name} não existe.")
                        print(f"    - {sys.argv[i]}")
                        print("")
                else:
                    iacon_arg_config['params']['positional'].append(sys.argv[i])
                    iacon_arg_config['params']['named'][name] = sys.argv[i]
                    arg_found = True
                    del sys.argv[i]
                break

    if not arg_found:
        if required and not special in ['filepicker', 'folderpicker', 'filesave']:
            print(f"Parâmetros posicionais são obrigatórios e não foi encontrado valor para o parâmetro {name}")
            print("")
            iacon_arg_config['validated'] = False
        else:
            iacon_arg_config['params']['positional'].append(default)

    def decorator_positional_param(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            config = [item for item in iacon_arg_config['positional'] if item['name'] == name][0]
            if config['special'] == "filepicker" and (not name in iacon_arg_config['params']['named'] or iacon_arg_config['params']['named'][name] == ""):
                iacon_arg_config['params']['named'][name] = input_openfile().run()
            elif config['special'] == "filesave" and (not name in iacon_arg_config['params']['named'] or iacon_arg_config['params']['named'][name] == ""):
                iacon_arg_config['params']['named'][name] = input_savefile().run()
            elif config['special'] == "folderpicker" and (not name in iacon_arg_config['params']['named'] or iacon_arg_config['params']['named'][name] == ""):
                iacon_arg_config['params']['named'][name] = input_directory().run()
            if iacon_arg_config['params']['named'][name] == "" and config['required']:
                print(f"O parâmetro {name} é obrigatório.")
                sys.exit(0)

            func.__globals__['args'] = iacon_arg_config['params']['positional']
            func.__globals__['params'] = iacon_arg_config['params']['named']
            return func(*args, **kwargs)
        return wrapper
    return decorator_positional_param

def named_param(name, description, required=False, default="", sample="", label="", options=[], special="", solo=False):
    if sample == "":
        sample = default if default != "" else name
    global iacon_arg_config
    if not 'iacon_arg_config' in globals():
        iacon_arg_config = {
            'description': 'Informar a descrição do script com o decorator @parser_description',
            'positional': [],
            'named': {},
            'boolean': {},
            'params': {
                'positional': [],
                'named': {},
                'boolean': {}
            },
            'validated': True,
            'show_help': False,
            'argv0': sys.argv[0]
        }
        del sys.argv[0]
        if len(sys.argv) == 1:
            if sys.argv[0].strip() in ['-h', '-help', 'help', '-ajuda', 'ajuda', '?']:
                iacon_arg_config['show_help'] = True
                del sys.argv[0]

    iacon_arg_config['named'][name] = {
        "name": name,
        "description": description,
        'required': required,
        'default': default,
        'sample': sample,
        'label': label,
        'special': special,
        'options': options,
        'solo': solo
    }

    iacon_arg_config['params']['named'][name] = default
    arg_found = False
    if len(sys.argv) > 0:
        for i in range(len(sys.argv)):
            if '=' in sys.argv[i]:
                t = sys.argv[i].split("=")
                n = t[0]
                while "-" in n[0]:
                    del n[0]
                if n == name:
                    if len(iacon_arg_config['named'][name]['options']) > 0 and not t[1] in iacon_arg_config['named'][name]['options']:
                        iacon_arg_config['validated'] = False
                        print(f"Opção inválida para o parâmetro {name}.")
                        print("")
                    else:
                        iacon_arg_config['params']['named'][name] = t[1]
                        arg_found = True
                        del sys.argv[i]
                    break

    if required and not arg_found:
        iacon_arg_config['validated'] = False

    def decorator_named_param(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            config = iacon_arg_config['named'][name]
            if config['special'] == "filepicker" and (not name in iacon_arg_config['params']['named'] or iacon_arg_config['params']['named'][name] == ""):
                iacon_arg_config['params']['named'][name] = input_openfile().run()
            elif config['special'] == "filesave" and (not name in iacon_arg_config['params']['named'] or iacon_arg_config['params']['named'][name] == ""):
                iacon_arg_config['params']['named'][name] = input_savefile().run()
            elif config['special'] == "folderpicker" and (not name in iacon_arg_config['params']['named'] or iacon_arg_config['params']['named'][name] == ""):
                iacon_arg_config['params']['named'][name] = input_directory().run()
            if iacon_arg_config['params']['named'][name] == "" and config['required']:
                print(f"O parâmetro {name} é obrigatório.")
                sys.exit(0)

            func.__globals__['args'] = iacon_arg_config['params']['positional']
            func.__globals__['params'] = iacon_arg_config['params']['named']
            return func(*args, **kwargs)
        return wrapper
    return decorator_named_param

def boolean_param(name, description, required=False, label="", solo=False):
    global iacon_arg_config
    if not 'iacon_arg_config' in globals():
        iacon_arg_config = {
            'description': 'Informar a descrição do script com o decorator @parser_description',
            'positional': [],
            'named': {},
            'boolean': {},
            'params': {
                'positional': [],
                'named': {},
                'boolean': {}
            },
            'validated': True,
            'show_help': False,
            'argv0': sys.argv[0]
        }
        del sys.argv[0]
        if len(sys.argv) == 1:
            if sys.argv[0].strip() in ['-h', '-help', 'help', '-ajuda', 'ajuda', '?']:
                iacon_arg_config['show_help'] = True
                del sys.argv[0]

    iacon_arg_config['boolean'][name] = {
        "name": name,
        "description": description,
        'required': False,
        'default': False,
        'sample': name,
        'label': label,
        'solo': solo
    }

    iacon_arg_config['params']['boolean'][name] = False
    if len(sys.argv) > 0:
        for i in range(len(sys.argv)):
            n = sys.argv[i]
            while "-" == n[0]:
                n = n[1:]
            if n == name:
                iacon_arg_config['params']['boolean'][name] = True

    def decorator_boolean_param(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func.__globals__['args'] = iacon_arg_config['params']['positional']
            func.__globals__['params'] = iacon_arg_config['params']['named']
            return func(*args, **kwargs)
        return wrapper
    return decorator_boolean_param

if __name__ == "__main__":
    @arg_parser(".".join(os.path.basename(__file__).split(".")[0:-1]), 'Aqui eu escrevo a descrição da função')
    @positional_param('nome', 'Nome de quem será cumprimentado.', required=True)
    @positional_param('idade', 'Idade da pessoa que será cumprimentada.')
    @named_param('caractere', 'Caractere usado para montar a borda.', default="#")
    @boolean_param('mostrar', 'Mostrar borda.')
    def hello_world(msg):
        hello = f"Hello {args[0]}!"
        if params['mostrar']:
            print(params['caractere'] * 60)
            print(f"{params['caractere']} { hello.center(56) } {params['caractere']}")
            print(params['caractere'] * 60)
        else:
            print(hello)

    hello_world("SubHeaven")