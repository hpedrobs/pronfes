# -*- coding: utf-8 -*-
import codecs
import json
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))

import foldertool

def empty_smtp():
    return {
        "url": "smtplw.com.br",
        "port": 587,
        "acc": "",
        "pass": "",
        "email": "",
        "imap": "",
        "imap_port": "",
        "email_pass": "",
        "imap_folder": ""
    }

def default_config():
    def default_smtp():
        return {
            "cobranca": empty_smtp(),
            "documentos": empty_smtp(),
            "processos": empty_smtp()
        }
    
    return {
        "smtp": default_smtp()
    }

def config():
    user_folder = os.path.join(os.environ['USERPROFILE'], "subheaven")
    foldertool.check_folder(user_folder)
    path = os.path.join(user_folder, "config.json")
    config = {}

    if os.path.isfile(path):
        with codecs.open(path, "r", "latin1") as file:
            config = json.loads(file.read())
    
    default = default_config()
    for k in default:
        if not k in config:
            config[k] = default[k]
        elif isinstance(default[k], dict):
            for d in default[k]:
                if not d in config[k]:
                    config[k][d] = default[k][d]

    return config

def save_config(config):
    user_folder = os.path.join(os.environ['USERPROFILE'], "subheaven")
    foldertool.check_folder(user_folder)
    path = os.path.join(user_folder, "config.json")
    with codecs.open(path, "w+", "latin1") as file:
        file.write(json.dumps(config, indent=4, ensure_ascii=False))
    while not os.path.isfile(path):
        with codecs.open(path, "w+", "latin1") as file:
            file.write(json.dumps(config, indent=4, ensure_ascii=False))
