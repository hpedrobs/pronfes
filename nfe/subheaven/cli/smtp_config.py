# -*- coding: utf-8 -*-
import codecs
import os
import smtplib
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))

from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr

import res.config_loader
import smtptool
import input_base

from subheaven.arg_parser import *

def list_mailboxes():
    smtptool.list_mailboxes()

def do_config(smtp):
    new_smtp = res.config_loader.empty_smtp()
    new_smtp['url'] = input_base.input_text("Por favor, informe o endereço do servidor de smtp: \n", smtp['url'])
    new_smtp['port'] = input_base.input_number("Por favor, informe a porta do servidor de smtp: \n", smtp['port'])
    new_smtp['acc'] = input_base.input_text("Por favor, informe a conta de login no servidor: \n", smtp['acc'])
    new_smtp['pass'] = input_base.input_text("Por favor, informe a senha de login no servidor: \n", smtp['pass'])
    new_smtp['email'] = input_base.input_text("Por favor, informe o email configurado para envio: \n", smtp['email'])
    new_smtp['imap'] = input_base.input_text("Por favor, informe o servidor de IMAP (Opcional): \n", smtp['imap'])
    new_smtp['imap_port'] = input_base.input_number("Por favor, informe a porta do servidor de IMAP (Opcional): \n", smtp['imap_port'])
    new_smtp['email_pass'] = input_base.input_text("Por favor, informe a senha do email (Opcional): \n", smtp['email_pass'])
    new_smtp['imap_folder'] = input_base.input_text("Por favor, informe o nome da pasta de emails enviados (Opcional): \n", smtp['imap_folder'])
    return new_smtp

def new_config(config, nome):
    if nome in config['smtp']:
        base = config['smtp'][nome]
    else:
        base = res.config_loader.empty_smtp()
    config['smtp'][nome] = do_config(base)
    res.config_loader.save_config(config)
    print("Configuração salva com sucesso!")
    print("")
    show_config(config['smtp'], nome)

def show_config(config, nome):
    if nome in config:
        print(f"Configuração de SMTP do {nome}")
        for k in config[nome]:
            print(f"    {k} = {config[nome][k]}")
    else:
        print(f"Não existe configuração de SMTP com esse nome \"{nome}\"")

def list_config(config):
    for nome in config['smtp']:
        print("")
        print(nome)
        for param in config['smtp'][nome]:
            print(f"    {param} = {config['smtp'][nome][param]}")

def test_email(config, nome):
    if nome in config['smtp']:
        smtp = config['smtp'][nome]
        if smtp["url"] == "" or smtp["acc"] == "" or smtp["pass"] == "":
            print("Configuração de email não está completo. Configure-o com o comando smtp_config")
            return
        else:
            email = input_base.input_text("Informe o email de envio de teste: ", default="subheaven.paulo@gmail.com", obrigatorio=True)
            smtptool.send_email(nome, "Olá Mundo!", "Teste de email do smtptool", email)
    else:
        print(f"Não existe configuração de SMTP com esse nome \"{nome}\"")

@arg_parser(".".join(os.path.basename(__file__).split(".")[0:-1]), 'Configura uma conta para envio de emails via smtp')
@named_param('view', 'Mostrar a configuração informada.', default="", required=False, sample="cobranca", solo=True)
@named_param('new', 'Cria uma nova configuração de email para envio.', default="", required=False, sample="cobranca", solo=True)
@named_param('test', 'Testa o envio de email com a configuração informada.', default="", required=False, sample="cobranca", solo=True)
@named_param('mailboxes', 'Listar caixas de email do servidor de IMAP. Usado para verificar o nome da pasta \n               de emails enviados para que sejam gravadas as devidas cópias.', default="", required=False, sample="cobranca", solo=True)
@boolean_param('list', 'Lista todas a configurações existentes.', solo=True)
def process():
    config = res.config_loader.config()
    if params['view'] != "":
        show_config(config['smtp'], params['view'])
    elif params['mailboxes'] != "":
        list_mailboxes(params['mailboxes'])
    elif params['list']:
        list_config(config)
    elif params['test'] != "":
        test_email(config, params['test'])
    elif params['new'] != "":
        new_config(config, params['new'])
    else:
        print("!!!!!!!!!!!!!!!!!!")

if __name__ == "__main__":
    process()
