# -*- coding: utf-8 -*-
import codecs
import os
import subprocess
import sys
import shlex

from subheaven.arg_parser import *

@arg_parser(".".join(os.path.basename(__file__).split(".")[0:-1]), 'Busca o pid do servico que está usando a porta especificada.')
@positional_param('port', 'Porta a ser procurada', sample='1560', required=True)
@boolean_param('all', 'Se informado, procura todos os serviços, senão busca apenas a porta que possui o status LISTENING', required=False)
def find():
    print(f"for /f \"tokens=5\" %a in ('netstat -aon ^| findstr {params['port']}') do @echo %~nxa")
    command = shlex.split(f"for /f \"tokens=5\" %a in ('netstat -aon ^| findstr {params['port']}') do @echo %~nxa")
    print(command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    for x in stderr.decode("utf-8").split("\r\n"):
        if x != "" and x[0:12].lower() != "deprecation:":
            print(x)

    for line in stdout.decode("utf-8").split("\r\n"):
        print(line)


if __name__ == "__main__":
    find()