# -*- coding: utf-8 -*-
import bson
import base64
import codecs
import datetime
import io
import json
import OpenSSL.crypto
import operator
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import unicodedata
import xlrd
import winreg
import yaml
import tempfile
from bson.objectid import ObjectId
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from mongo import *
import xml.etree.ElementTree as ET
import unicodedata
from contextlib import contextmanager

_internal_log = []
internal_config = None

summa_files = "C:\\inetpub\\summa\\files"

def isUserAdmin():
    if os.name == 'nt':
        import ctypes
        # WARNING: requires Windows XP SP2 or higher!
        try:
            return ctypes.windll.shell32.IsUserAnAdmin()
        except:
            traceback.print_exc()
            print("Admin check failed, assuming not an admin.")
            return False
    elif os.name == 'posix':
        # Check for root on Posix
        return os.getuid() == 0
    else:
        raise RuntimeError("Unsupported operating system for this module: %s" % (os.name,))

def runAsAdmin(cmdLine=None, wait=True):
    if os.name != 'nt':
        raise RuntimeError("This function is only implemented on Windows.")

    import win32api, win32con, win32event, win32process
    from win32com.shell.shell import ShellExecuteEx
    from win32com.shell import shellcon

    python_exe = sys.executable

    if cmdLine is None:
        cmdLine = [python_exe] + sys.argv
    elif type(cmdLine) not in (types.TupleType,types.ListType):
        raise ValueError("cmdLine is not a sequence.")
    cmd = '"%s"' % (cmdLine[0],)

    params = " ".join(['"%s"' % (x,) for x in cmdLine[1:]])
    cmdDir = ''
    showCmd = win32con.SW_SHOWNORMAL
    # showCmd = win32con.SW_HIDE
    lpVerb = 'runas'

    print(" ".join(cmdLine))
    procInfo = ShellExecuteEx(nShow=showCmd,
                            fMask=shellcon.SEE_MASK_NOCLOSEPROCESS,
                            lpVerb=lpVerb,
                            lpFile=cmd,
                            lpParameters=params)

    if wait:
        procHandle = procInfo['hProcess']    
        obj = win32event.WaitForSingleObject(procHandle, win32event.INFINITE)
        rc = win32process.GetExitCodeProcess(procHandle)
    else:
        rc = None

    return rc

def conferir_privilegios():
    rc = 0
    if not isUserAdmin():
        rc = runAsAdmin()
    else:
        rc = 0
    return rc

def run_as_admin(function):
    def wrapper(*args, **kwargs):
        if conferir_privilegios() > 0:
            sys.exit(0)
        return function(*args, **kwargs)

    return wrapper

def config_alerta(*args):
    print("###############################################################################")
    print("  Desculpe-me, mas não foi possível encontrar as configurações necessárias para")
    print("    executar a tarefa. Por favor, execute o seguinte comando e informe os dados")
    print("    de configuração faltantes:")
    print("")
    for arg in args:
        print(f"    C:\\>iacon-config {arg}")
        break
    print("###############################################################################")

def config(*args):
    config_root = mongo('configuracao').find_one()
    if config_root != None:
        root = config_root
        for arg in args:
            if arg in root:
                root = root[arg]
            else:
                return ""
        return root
    else:
        config_alerta(*args)
        return ""

def setConfig(*args):
    config_root = mongo('configuracao').find_one()
    args = list(args)
    bloco = config_root
    l = len(args)
    for i in range(l - 1):
        if i == l - 2:
            bloco[args[i]] = args[i+1]
        else:
            if not args[i] in bloco:
                bloco[args[i]] = {}
            bloco = bloco[args[i]]

    mongo('configuracao').update_one({ "_id": config_root["_id"] }, { "$set": config_root})
    # if config_root != None:
    #     root = config_root
    #     for arg in args:
    #         if arg in root:
    #             root = root[arg]
    #         else:
    #             config_alerta(*args)
    #             return ""
    #     return root
    # else:
    #     config_alerta(*args)
    #     return ""

class dharmaLogger(object):
    _log = ""
    name = ""
    uuid = ""
    _config = None

    def __init__(self):
        self.init()

    def logconfig(self):
        self.log("")
        self.log("Exemplo de configuração:")
        self.log("{")
        self.log("    \"tabela\": \"sped_ecf\",")
        self.log("    \"filtro\": {")
        self.log("        \"codigo\": \"615\",")
        self.log("        \"periodo\": \"2019\"")
        self.log("    }")
        self.log("}")

    def config(self, data):
        if not "tabela" in data:
            self.log("Erro de log:")
            self.log("    - O dicionário de configuração do log tem que informar o nome da tabela ou collection no banco de dados")
            self.logconfig()
        elif not "filtro" in data:
            self.log("Erro de log:")
            self.log("    - O dicionário de configuração do log tem que informar o filtro usado para saber em qual registro gravar")
            self.logconfig()
        else:
            self._config = data

    def init(self):
        self._log = ""

    def log(self, msg):
        try:
            # self._log += f"\n{str(msg).encode('cp1252', errors='ignore')}"
            # print(str(msg).encode('cp1252', errors='ignore'))
            self._log += f"\n{str(msg)}"
            print(str(msg).encode('cp1252', errors='replace').decode('latin1').replace('?', '#'))
        except:
            self._log += f"\n{str(msg)}"
            print(str(msg))

    def save(self, name=""):
        if self.name != "":
            name = self.name

        if self._log.strip() != "":
            d = datetime.datetime.now()
            now = str(d).split(".")[0]
            slog = f"{now}\n{self._log.strip()}"
            log = {
                "name": name,
                "uuid": self.uuid,
                "date": f"{d.year}-{str(d.month).rjust(2, '0')}-{str(d.day).rjust(2, '0')}-{str(d.hour).rjust(2, '0')}-{str(d.minute).rjust(2, '0')}-{str(d.second).rjust(2, '0')}",
                "log": slog
            }
            mongo('logs').insert_one(log)
            if not self._config == None:
                mongo(self._config['tabela']).update_one(self._config['filtro'], { "$set": {"log": slog} })
            return self.limpar(name, log)
        return os.getpid(), None

    def limpar(self, name, log):
        logs =list(mongo('logs').find({ "name": name }).sort("date", pymongo.ASCENDING))
        if len(logs) > 40:
            sl = logs[0:(len(logs)-40)]
            for l in sl:
                mongo('logs').delete_one({ "_id": ObjectId(l['_id']) })
        return os.getpid(), log

#def Log(_msg, end="{none}", flush=False):
def Log(_msg):
    try:
        _internal_log.append(str(_msg).decode("utf-8"))
        print(str(_msg).decode("utf-8"))
    except:
        _internal_log.append(str(_msg))
        print(str(_msg))

def log(_msg = ""):
    Log(_msg)

def clear():
    _internal_log = []

def history():
    return _internal_log

def backlog(msg = ""):
    log("\r", end="")
    log(msg, end="")

def Warn(_msg):
    _warn = "\033[31m{}".format(_msg)
    log(_warn)
    sys.stdout.flush()

def CheckFolder(_path):
    if (not os.path.isdir(_path)):
        os.makedirs(_path)

def FileExists(filepath):
    return os.path.isfile(filepath)

def FormatFloat(s, thou=",", dec="."):
    if (isinstance(s, float)):
        s = str(round(s, 2))
    if (isinstance(s, int)):
        s = str(s)
    if len(s.split(".")) > 1:
        integer, decimal = s.split(".")
    else:
        integer, decimal = s, "0"
    integer = re.sub(r"\B(?=(?:\d{3})+$)", thou, integer)
    decimal = decimal.ljust(2, "0")
    _r = integer + dec + decimal
    _r = _r.replace(",", "|")
    _r = _r.replace(".", ",")
    _r = _r.replace("|", ".")
    return _r

# Minimaliza, ou seja, transforma todas as instancias repetidas de espaços em espaços simples.
#   Exemplo, o texto "  cnpj:      09.582.876/0001-68    Espécie Documento          Aceite" viraria
#   "cnpj: 09.582.876/0001-68 Espécie Documento Aceite"
#
# Nota: Ele faz um trim do texto também
def minimalizeSpaces(text):
    _result = text
    while ("  " in _result):
        _result = _result.replace("  ", " ")
    _result = _result.strip()
    return _result

# Retira de um texto, qualquer caracter que seja diferente da lista passada
# Exemplo clearPermittedText("CEP: 74210-122", "1234567890-")
# Resultado: "74210-122"
def clearPermittedText(text, allowed):
    _result = ""
    for ch in text:
        if ch in allowed:
            _result += ch
    return _result

def apenasNumeros(text):
    return onlyNumbers(text)

def onlyNumbers(text):
    _valid = "1234567890"
    return clearPermittedText(text, _valid)

def formatarMask(text, mask):
    _i = 0
    _result = ""
    for _m in range(len(mask)):
        if mask[_m] == "9":
            if len(text) >= _i + 1:
                _result += text[_i]
                _i += 1
            else:
                break
        elif len(text) >= _i + 1:
            _result += mask[_m]
    return _result

def formatarCPF(text):
    return formatarMask(text, "999.999.999-99")

def formatarCNPJ(text):
    return formatarMask(text, "99.999.999/9999-99")

def formatarDocumento(text):
    _result = ""
    if text == None:
        return ""
    _t = apenasNumeros(text)
    if _t == "":
        _result = ""
    if len(_t) <= 11:
        while len(_t) < 11:
            _t = "0" + _t
        _result = formatarCPF(_t)
    else:
        while len(_t) < 14:
            _t = "0" + _t
        _result = formatarCNPJ(_t)
    return _result

def swapText(text, oldChar, newChar):
    _result = ""
    for _c in text:
        if oldChar.find(_c) > -1:
            _result += newChar[oldChar.find(_c)]
        else:
            _result += _c
    return _result

def RemoveAccents(text):
    _accents = "áàâãäÁÀÂÃÄéèêêÉÈÊËíìîïÍÌÎÏóòôõöÓÒÔÕÖúùûüÚÙÛçÇñÑ"
    _allowed = "aaaaaAAAAAeeeeEEEEiiiiIIIIoooooOOOOOuuuuUUUcCnN"
    return swapText(text, _accents, _allowed)

def LoadYAML(filepath):
    with open(filepath) as file:
        _text = file.read()
    return yaml.load(_text)

def LoadJSON(filepath):
    if (os.path.isfile(filepath)):
        with codecs.open(filepath, "r", "utf-8") as temp:
            return json.loads(temp.read())
    else:
        return None

# Modos:
#    layout
#    simple
#    table
#    raw

def PDFtoText(origin, dest, mode = "simple"):
    try:
        if (sys.version_info[0] < 3 and isinstance(origin, unicode)):
            origin = origin.encode("utf-8")
        _command = "{}\\pdftotext.exe -{} \"{}\" \"{}\"".format(os.path.dirname(os.path.abspath(__file__)), mode, origin, dest)
        print("")
        print("    {}".format(_command))
        print("")
        _output = os.system(_command)
        # Log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # Log(_output)
        if (_output == 0):
            _to = 100
            while (not FileExists(dest)):
                Log("    Waiting...")
                _to = _to - 1
                if _to <= 0:
                    sys.exit(1)
                time.sleep(0.05 * 1000)
    except Exception as ex:
        Log(str(ex))

def cleartagname(tag):
    result = tag
    search_results = re.finditer(r'\{.*?\}', tag)
    for item in search_results:
        result = result.replace(item.group(0), "")
    return result

def exit(log = "", code = 9, history=1):
    count = history
    back = -1 - history
    while count > 0:
        filename = traceback.extract_stack()[back].filename.split('\\')[-1]
        log = f"{log} " if log != "" else ""
        print("")
        print(f"{log}{filename} [{traceback.extract_stack()[back].lineno}]:")
        print("Terminando...")
        back += 1
        count -= 1
    sys.exit(code)

def stack(history=1):
    count = history
    back = -1 - history
    while count > 0:
        filename = traceback.extract_stack()[back].filename.split('\\')[-1]
        print(f"{filename} [{traceback.extract_stack()[back].lineno}]")
        back += 1
        count -= 1

def dumps(o, stack=True):
    if stack:
        print("")
        filename = traceback.extract_stack()[-2].filename.split('\\')[-1]
        print(f"{filename} [{traceback.extract_stack()[-2].lineno}]:")

    msg = json.dumps(o, default=defaultConverter, indent=4)
    print(msg)

    if stack:
        print("")
    return msg

def debug(*data, stack=True):
    if stack:
        print("")
        filename = traceback.extract_stack()[-2].filename.split('\\')[-1]
        print(f"{filename} [{traceback.extract_stack()[-2].lineno}]:")

    if isinstance(data, tuple):
        if len(data) > 1:
            for i in range(len(data)):
                debug(data[i], stack=False)
        elif len(data) == 1:
            d = data[0]
            if isinstance(d, list) or isinstance(d, dict):
                dumps(d, stack=False)
            else:
                print(d)

    if stack:
        print("")

def ReadLines(filepath):
    _lines = []
    if (FileExists(filepath)):
        if (sys.version_info[0] < 3):
            with io.open(filepath, mode="r", encoding="latin1") as f:
                _lines = f.readlines()
        else:
            if os.path.isfile(filepath):
                with open(filepath) as f:
                    _lines = f.readlines()
    return _lines

def ToUnicode(text):
    text = text.replace("á", "\u00e1")
    text = text.replace("à", "\u00e0")
    text = text.replace("â", "\u00e2")
    text = text.replace("ã", "\u00e3")
    text = text.replace("ä", "\u00e4")
    text = text.replace("Á", "\u00c1")
    text = text.replace("À", "\u00c0")
    text = text.replace("Â", "\u00c2")
    text = text.replace("Ã", "\u00c3")
    text = text.replace("Ä", "\u00c4")
    text = text.replace("é", "\u00e9")
    text = text.replace("è", "\u00e8")
    text = text.replace("ê", "\u00ea")
    text = text.replace("ê", "\u00ea")
    text = text.replace("É", "\u00c9")
    text = text.replace("È", "\u00c8")
    text = text.replace("Ê", "\u00ca")
    text = text.replace("Ë", "\u00cb")
    text = text.replace("í", "\u00ed")
    text = text.replace("ì", "\u00ec")
    text = text.replace("î", "\u00ee")
    text = text.replace("ï", "\u00ef")
    text = text.replace("Í", "\u00cd")
    text = text.replace("Ì", "\u00cc")
    text = text.replace("Î", "\u00ce")
    text = text.replace("Ï", "\u00cf")
    text = text.replace("ó", "\u00f3")
    text = text.replace("ò", "\u00f2")
    text = text.replace("ô", "\u00f4")
    text = text.replace("õ", "\u00f5")
    text = text.replace("ö", "\u00f6")
    text = text.replace("Ó", "\u00d3")
    text = text.replace("Ò", "\u00d2")
    text = text.replace("Ô", "\u00d4")
    text = text.replace("Õ", "\u00d5")
    text = text.replace("Ö", "\u00d6")
    text = text.replace("ú", "\u00fa")
    text = text.replace("ù", "\u00f9")
    text = text.replace("û", "\u00fb")
    text = text.replace("ü", "\u00fc")
    text = text.replace("Ú", "\u00da")
    text = text.replace("Ù", "\u00d9")
    text = text.replace("Û", "\u00db")
    text = text.replace("ç", "\u00e7")
    text = text.replace("Ç", "\u00c7")
    text = text.replace("ñ", "\u00f1")
    text = text.replace("Ñ", "\u00d1")
    text = text.replace("&", "\u0026")
    text = text.replace("'", "\u0027")
    #return unicode(text)
    return text

#Verificar se o cnpj e a tag informados estão na observação da empresa
def checarCNPJBase64(empresa, cnpj, tag):
    #Calcular a base 64 do cnpj e a tag
    b64 = "".join(map(chr, base64.b64encode(bytes(f"{cnpj}{tag}", 'latin1'))))
    #Se o cnpj em base 64 for encontrado na observação, retornar verdadeiro
    return b64 in empresa['obs']

#Verificar se a empresa está desativada para processamento no sistema
def empresaDesativadaNoSistema(empresa):
    if empresa['cnpj'] == None:
        return False
    else:
        #Armazenar o cnpj da empresa formatado
        cnpj_formatado = formatarDocumento(empresa['cnpj'])
        b64_formatado = "".join(map(chr, base64.b64encode(bytes(cnpj_formatado, 'latin1'))))
        b64_limpo = "".join(map(chr, base64.b64encode(bytes(empresa['cnpj'].strip(), 'latin1'))))
        #Se existir cnpj e existir observação na empresa e a empresa tiver cnpj
        if cnpj_formatado != None and empresa['obs'] != None and empresa['cnpj'] != None:
            #Se a versão do python usada por menor que o 3
            if sys.version_info[0] < 3:
                #Calcular a base 64 do cnpj formatado
                b64_formatado = base64.b64encode(cnpj_formatado)
                #Calcular a base 64 do cnpj limpo
                b64_limpo = base64.b64encode(empresa['cnpj'].strip())
            #Senão, se a versão do python usada por igual ou maior que o 3
            else:
                #Calcular a base 64 do cnpj formatado
                b64_formatado = "".join(map(chr, base64.b64encode(bytes(cnpj_formatado, 'latin1'))))
                #Calcular a base 64 do cnpj limpo
                b64_limpo = "".join(map(chr, base64.b64encode(bytes(empresa['cnpj'].strip(), 'latin1'))))

            #Se o cnpj formatado ou o cnpj limpo em base 64 for encontrado na observação, retornar verdadeiro
            return b64_formatado in empresa['obs'] or b64_limpo in empresa['obs']
        #Senão
        else:
            #Retornar falso
            return False

def empresaValidadaPraMalha(empresa):
    _result = True
    _result = _result and empresa["ie"] != ""
    _result = _result and empresa["uf"] == "GO"
    return _result

def RAMUsage():
    return psutil.virtual_memory()[2]

def PCUsage():
    ram = RAMUsage()
    cpu = psutil.cpu_percent(interval=1)
    return ram, cpu

# @timeit decorator
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            log('%r  %2.2f ms'.format(method.__name__, (te - ts) * 1000))
        return result
    return timed

def resumoDataset(filepath, empresa, dataset, filtro):
    try:
        data = {}
        with codecs.open(filepath, 'r', 'utf-8') as file:
            data = json.loads(file.read())
        
        if (len(dataset) == 0):
            dataset.append(dict(codigo = data["empresa"], entradas = data["entradas"], saidas = data["saidas"], total = data["total"]))
        else:
            passed = False
            for dt in dataset:
                if (dt["codigo"] == data["empresa"]):
                    dt["entradas"] += data["entradas"]
                    dt["saidas"] += data["saidas"]
                    dt["total"] += data["total"]
                    passed = True
            if (not passed):
                dataset.append(dict(codigo = data["empresa"], entradas = data["entradas"], saidas = data["saidas"], total = data["total"]))
    except Exception as ex:
        log("ERRO!!")
        log(type(ex))
        log(ex.args)
        message = "Erro no processamento do arquivo de resumo!"
        log(message)
    return dataset

def carregar_resumo():
    # response.view = 'generic.json'
    dataset = []
    args = {
        "pini": "01/2018",
        "pfim": "12/2018",
        "lancadas": True,
        "nao_lancadas": True
    }
    try:
        path = "C:\\inetpub\\summa\\files\\consolidacao\\malha-total"
        filtro = {
            "pini": args["pini"],
            "pfim": args["pfim"],
            "lancadas": args["lancadas"],
            "nao_lancadas": args["nao_lancadas"],
        }
        
        for root, dirs, files in os.walk(path):
            for name in files:
                filepath = os.path.join(root, name)
                name = name.split('.')[0]
                empresa = name.split('-')[0]
                dataset = resumoDataset(filepath, empresa, dataset, filtro)
        message = "Resumos carregados com sucesso!"
    except Exception as ex:
        log("ERRO!!")
        log(type(ex))
        log(ex.args)
        message = "Erro na busca dos resumos!"
        log(message)
    return dict(dataset = dataset, message = message)

def removerAcentosECaracteresEspeciais(palavra):
    # Unicode normalize transforma um caracter em seu equivalente em latin.
    nfkd = unicodedata.normalize('NFKD', palavra).encode('ASCII', 'ignore').decode('ASCII')
    palavraTratada = u"".join([c for c in nfkd if not unicodedata.combining(c)])

    # Usa expressão regular para retornar a palavra apenas com valores corretos
    return re.sub('[^a-zA-Z0-9.!+)(/*,\- \\\]', '', palavraTratada)

# Vasculha uma pasta em buscas de arquivos excel, compara seus conteúdos e retorna uma lista de arquivos duplicados
def procurarPlanilhaDuplicada(pasta):
    duplicadas = []

    #Funções internas
    def removerAcentosECaracteresEspeciais(palavra):
        # Unicode normalize transforma um caracter em seu equivalente em latin.
        nfkd = unicodedata.normalize('NFKD', palavra).encode('ASCII', 'ignore').decode('ASCII')
        palavraTratada = u"".join([c for c in nfkd if not unicodedata.combining(c)])

        # Usa expressão regular para retornar a palavra apenas com valores corretos
        return re.sub('[^a-zA-Z0-9.!+:=)?$(/*,\-_ \\\]', '', palavraTratada)

    def lerExcel(arquivo):
        lista_dados = []
        dados_linha = []

        tamanho_arquivo = os.path.getsize(arquivo)

        if tamanho_arquivo > 0:
            try:
                arquivo = xlrd.open_workbook(arquivo, logfile=open(os.devnull, 'w'))
            except Exception:
                arquivo = xlrd.open_workbook(arquivo, logfile=open(os.devnull, 'w'), encoding_override='Windows-1252')

            # guarda todas as planilhas que tem dentro do arquivo excel
            planilhas = arquivo.sheet_names()

            # lê cada planilha
            for p in planilhas:

                # pega o nome da planilha
                planilha = arquivo.sheet_by_name(p)

                # pega a quantidade de linha que a planilha tem
                max_row = planilha.nrows
                # pega a quantidade de colunca que a planilha tem
                max_column = planilha.ncols

                # lê cada linha e coluna da planilha e imprime
                for i in range(0, max_row):

                    valor_linha = planilha.row_values(rowx=i)

                    # ignora linhas em branco
                    if valor_linha.count("") == max_column:
                        continue

                    # lê as colunas
                    for j in range(0, max_column):

                        # as linhas abaixo analisa o tipo de dado que está na planilha e retorna no formato correto, sem ".0" para números ou a data no formato numérico
                        tipo_valor = planilha.cell_type(rowx=i, colx=j)
                        valor_celula = removerAcentosECaracteresEspeciais(str(planilha.cell_value(rowx=i, colx=j)))
                        if tipo_valor == 2:
                            valor_casas_decimais = valor_celula.split('.')
                            valor_casas_decimais = valor_casas_decimais[1]
                            if int(valor_casas_decimais) == 0:
                                valor_celula = valor_celula.split('.')
                                valor_celula = valor_celula[0]
                        elif tipo_valor == 3:
                            valor_celula = float(planilha.cell_value(rowx=i, colx=j))
                            valor_celula = xlrd.xldate.xldate_as_datetime(valor_celula, datemode=0)
                            valor_celula = valor_celula.strftime("%d/%m/%Y")

                        # retira espaços e quebra de linha da célula
                        valor_celula = str(valor_celula).strip().replace('\n', '')

                        # adiciona o valor da célula na lista de dados_linha
                        dados_linha.append(valor_celula)

                    # copia os dados da linha para o vetor de lista_dados
                    lista_dados.append(dados_linha[:])

                    # limpa os dados da linha para ler a próxima
                    dados_linha.clear()

        # retorna uma lista dos dados
        return lista_dados

    # Algoritmo
    saida = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) + "\\" + "log.csv"
    saida = open(saida, "w", encoding='utf-8')

    #pasta = input(str('- Informe o caminho da pasta: '))

    dadosArquivos = {}

    for raiz, diretorios, arquivos in os.walk(pasta):
        for arquivo in arquivos:
            caminhoArquivo = os.path.join(raiz,arquivo)
            if arquivo.upper().endswith(('.XLS')):
                dadosArquivos[arquivo] = lerExcel(caminhoArquivo)
            else:
                continue

    ranking = []
    ranking = sorted(dadosArquivos.items(), key = operator.itemgetter(1), reverse=False)
    
    for key, value in enumerate(ranking):
        if key > 0:
            if ranking[key-1][1] == ranking[key][1]:
                log(f'\n- Os arquivos {ranking[key-1][0]} e {ranking[key][0]} possuem exatamente a mesma informação')
                duplicadas.append([ranking[key-1][0], ranking[key][0]])
                saida.write(f'{ranking[key-1][0]};{ranking[key][0]}\n')

    saida.close()
    return duplicadas

regimes = {
    "1": "Real",
    "2": "Simples",
    "5": "Presumido"
}
def regimeNome(codigo):
    if str(codigo) in regimes:
        return regimes[str(codigo)]
    else:
        return "Sem Regime"

def nodetodict(node):
    if node.text != None:
        return node.text
    else:
        result = {}
        for attribute in node.attrib:
            result[f"@{attribute}"] = node.attrib[attribute]
        for child in node:
            if cleartagname(child.tag) not in result:
                result[cleartagname(child.tag)] = nodetodict(child)
            elif isinstance(result[cleartagname(child.tag)], list):
                result[cleartagname(child.tag)].append(nodetodict(child))
            elif cleartagname(child.tag) in result:
                result[cleartagname(child.tag)] = [result[cleartagname(child.tag)]]
                result[cleartagname(child.tag)].append(nodetodict(child))
            else:
                tools.log("Erro ao lidar com o tipo de atributo do XML. Verificar")
                sys.exit(1)

        return result

def validatexml(param):
    valid = True
    valid = valid and param != ""
    valid = valid and "<" in param
    valid = valid and ">" in param
    return valid

def xmltodict(param):
    valid_data_initialized = False
    if len(param) < 240 and os.path.isfile(param) and param.split('.')[-1].lower() == "xml":
        with codecs.open(param, 'rb', 'latin1') as file:
            param = file.read()

    newparam = ""
    counter = 0
    for k in param:
        counter += 1
        if not valid_data_initialized and k == "<":
            valid_data_initialized = True
        if valid_data_initialized and k != "\n" and k != "\t" and ord(k) > 31:
            newparam = f"{newparam}{k}"
    param = newparam
    while " <" in param:
        param = param.replace(" <", "<")
    while "> " in param:
        param = param.replace("> ", ">")

    if validatexml(param):
        root = ET.fromstring(param)
    else:
        log("        XML inválido!")
        root = None

    data = {}
    if root != None:
        data[cleartagname(root.tag)] = nodetodict(root)
    return data

    # try:
    #     if len(param) < 240 and os.path.isfile(param) and param.split('.')[-1].lower() == "xml":
    #         with codecs.open(param, 'rb', 'latin1') as file:
    #             param = file.read()

    #     newparam = ""
    #     counter = 0
    #     for k in param:
    #         counter += 1
    #         if not valid_data_initialized and k == "<":
    #             valid_data_initialized = True
    #         if valid_data_initialized and k != "\n" and k != "\t" and ord(k) > 42:
    #             newparam = f"{newparam}{k}"
    #     param = newparam
    #     while " <" in param:
    #         param = param.replace(" <", "<")
    #     while "> " in param:
    #         param = param.replace("> ", ">")

    #     if validatexml(param):
    #         root = ET.fromstring(param)
    #     else:
    #         log("        XML inválido!")
    #         root = None

    #     data = {}
    #     if root != None:
    #         data[cleartagname(root.tag)] = nodetodict(root)
    #     return data
    # except Exception as e:
    #     debug(f"    {e}")
    #     exit()
    #     return None

def exception(exc, stack, message):
    print("|||||||||||||||||||||||||||||||||||||||||||||||||||")
    log(str(exc))
    print("|||||||||||||||||||||||||||||||||||||||||||||||||||")
    sys.exit(1)

def prepara_certificado_txt(self, cert_txt):
    #
    # Para dar certo a leitura pelo xmlsec, temos que separar o certificado
    # em linhas de 64 caracteres de extensão...
    #
    cert_txt = cert_txt.replace('\n', '')
    cert_txt = cert_txt.replace('-----BEGIN CERTIFICATE-----', '')
    cert_txt = cert_txt.replace('-----END CERTIFICATE-----', '')

    linhas_certificado = ['-----BEGIN CERTIFICATE-----\n']
    for i in range(0, len(cert_txt), 64):
        linhas_certificado.append(cert_txt[i:i+64] + '\n')
    linhas_certificado.append('-----END CERTIFICATE-----\n')

    self.certificado = ''.join(linhas_certificado)

    cert_openssl = crypto.load_certificate(crypto.FILETYPE_PEM, self.certificado)
    self.cert_openssl = cert_openssl

    self._emissor = dict(cert_openssl.get_issuer().get_components())
    self._proprietario = dict(cert_openssl.get_subject().get_components())
    self._numero_serie = cert_openssl.get_serial_number()
    self._data_inicio_validade = datetime.strptime(cert_openssl.get_notBefore(), '%Y%m%d%H%M%SZ')
    self._data_inicio_validade = UTC.localize(self._data_inicio_validade)
    self._data_fim_validade    = datetime.strptime(cert_openssl.get_notAfter(), '%Y%m%d%H%M%SZ')
    self._data_fim_validade    = UTC.localize(self._data_fim_validade)

    for i in range(cert_openssl.get_extension_count()):
        extensao = cert_openssl.get_extension(i)
        self._extensoes[extensao.get_short_name()] = extensao.get_data()

def byteDictToDict(d):
    n = {}
    for k in d:
        n[k.decode("utf8")] = d[k].decode("utf8")
    return n

def read_pfx(pfx_path, pfx_password):
    '''
    Decrypt the P12 (PFX) file and create a private key file and certificate file.

    Input:
        pfx_path    INPUT: This is the Google P12 file or SSL PFX certificate file
        pfx_password    INPUT: Password used to protect P12 (PFX)
        pkey_path   INPUT: File name to write the Private Key to
        pem_path    INPUT: File name to write the Certificate to
        pem_ca_path INPUT: File name to write the Certificate Authorities to
    '''

    print('Opening:', pfx_path)
    with open(pfx_path, 'rb') as f_pfx:
        pfx = f_pfx.read()

    print('Loading P12 (PFX) contents:')
    p12 = OpenSSL.crypto.load_pkcs12(pfx, pfx_password)

    data = {}
    
    with open('cert.key.pem', 'wb') as f:
        f.write(OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey()))
    with open('cert.crt.pem', 'wb') as f:
        f.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate()))

    print('Reading Private Key')
    data['key'] = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey()).decode('utf8')

    print('Reading Certificate')
    pem_data = OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate())
    data['pem'] = pem_data.decode('utf8')

    #Google P12 does not have certifiate authorities but SSL PFX certificates do
    data['ca'] = read_CAs(p12)

    data['expiration'] = x509.load_pem_x509_certificate(pem_data, default_backend()).not_valid_after

    cert = p12.get_certificate()
    emissor = byteDictToDict(dict(cert.get_issuer().get_components()))
    data['emissor'] = emissor
    proprietario = byteDictToDict(dict(cert.get_subject().get_components()))
    data['proprietario'] = proprietario
    data['numero_serie'] = cert.get_serial_number()
    data['inicio_validade'] = datetime.datetime.strptime(cert.get_notBefore().decode('utf8'), '%Y%m%d%H%M%SZ')
    data['fim_validade'] = datetime.datetime.strptime(cert.get_notAfter().decode('utf8'), '%Y%m%d%H%M%SZ')

    return data

def read_CAs(p12):
    ca = p12.get_ca_certificates()
    list = []

    if not ca is None:
        print('Reading Certificate CA')
        for cert in ca:
            list.append(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, cert).decode('utf8'))
    return list

def validade(pfx_path, pfx_password):
    pkcs12 = OpenSSL.crypto.load_pkcs12(open(pfx_path, "rb").read(), pfx_password)
    pem_data = OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, pkcs12.get_certificate())
    cert = x509.load_pem_x509_certificate(pem_data, default_backend())
    return cert.not_valid_after

def is_running_as_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

@run_as_admin
def set_reg(path, name, value):
    try:
        reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
        newkey = winreg.CreateKey(winreg.HKEY_LOCAL_MACHINE, path)
        winreg.SetValueEx(newkey, name, 0, winreg.REG_SZ, value)
        return True
    except WindowsError as ex:
        print("♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣")
        print(ex)
        print("♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣♣")
        return False

def get_reg(path, name):
    try:
        registry_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path, 0, winreg.KEY_READ)
        value, regtype = winreg.QueryValueEx(registry_key, name)
        winreg.CloseKey(registry_key)
        return value
    except WindowsError as ex:
        print(ex)
        return None

def normalizarDict(dicionario):
    for key in dicionario:
        dicionario[key] = normalizar(dicionario[key])
    return dicionario

def normalizarList(lista):
    for i in range(len(lista)):
        lista[i] = normalizar(lista[i])
    return lista

def normalizarObjectId(valor):
    return str(valor)

def normalizar(data):
    if isinstance(data, dict):
        return normalizarDict(data)
    elif isinstance(data, list):
        return normalizarList(data)
    elif isinstance(data, bson.objectid.ObjectId):
        return normalizarObjectId(data)
    elif isinstance(data, str) or isinstance(data, int) or data == None:
        return data
    elif isinstance(data, bytes):
        return data.decode("latin1")
    elif isinstance(data, float):
        return float(data)
    elif isinstance(data, datetime.datetime) or isinstance(data, bson.timestamp.Timestamp):
        return str(data)
    else:
        print(f"Tipo não tratado: {type(data)}")
        print("tools")
        print(data)
        sys.exit(1)
        #return data

def count_date(period_date, period_count, day_type, days, periodicidade):
    try:
        if periodicidade == 'Mensal':
            if period_count < 0:
                subtracted = period_count
                while subtracted != 0:
                    period_date = period_date + relativedelta(months = -1)
                    subtracted += 1
            else:
                added = period_count
                while added != 0:
                    period_date = period_date + relativedelta(months = +1)
                    added -= 1
        elif periodicidade == 'Anual':
            if period_count < 0:
                subtracted = period_count
                while subtracted != 0:
                    period_date = period_date + relativedelta(years = -1)
                    subtracted += 1
            else:
                added = period_count
                while added != 0:
                    period_date = period_date + relativedelta(years = +1)
                    added -= 1
        elif periodicidade == 'Semanal':
            if period_count < 0:
                subtracted = period_count
                while subtracted != 0:
                    period_date = period_date + relativedelta(weeks = -1)
                    subtracted += 1
            else:
                added = period_count
                while added != 0:
                    period_date = period_date + relativedelta(weeks = +1)
                    added -= 1

        month = period_date.month
        year = period_date.year
        if day_type == 1:
            period_date = datetime.datetime(year, month, days)
        elif day_type == 4:
            days_count = days - 1
            while days_count != 0:
                if period_date.weekday() < 5:
                    days_count -= 1
                period_date = period_date + datetime.timedelta(days = 1)
        elif day_type == 2:
            period_date = datetime.datetime(year, month, days)
            while period_date.weekday() > 4:
                period_date = period_date - datetime.timedelta(days = 1)
        elif day_type == 3:
            period_date = datetime.datetime(year, month, days)
            while period_date.weekday() > 4:
                period_date = period_date + datetime.timedelta(days = 1)
    except Exception:
        log(traceback.print_exc())
        
    return period_date

def subtrair_mes(mes, ano, quant):
    mes_ant = mes - quant
    ano_ant = ano
    while mes_ant < 0:
        mes_ant += 12
        ano_ant -= 1
    return mes_ant, ano_ant

def calculate_activity_dates(periodicidade, inicio, vencimento, competencia, period):
    try:
        meta_date = "01/01/2020"
        venc_date = "10/01/2020"
        period = period.split("/")
        mes_atual = int(period[0])
        ano_atual = int(period[1])

        if inicio['mes'] == 1:
            mes_meta, ano_meta = subtrair_mes(mes_atual, ano_atual, 1)
        elif inicio['mes'] == 2:
            mes_meta, ano_meta = subtrair_mes(mes_atual, ano_atual, inicio['meses'])
        else:
            mes_meta, ano_meta = mes_atual, ano_atual
        dia_meta = int(inicio['dias'])
        if dia_meta > 30:
            dia_meta = 30
        elif dia_meta < 1:
            dia_meta = 1
        meta_date = int("{}{}{}".format(ano_meta, str(mes_meta).rjust(2, '0'), str(dia_meta).rjust(2, '0')))

        if competencia['mes'] == 1:
            mes_venc, ano_venc = subtrair_mes(mes_atual, ano_atual, 1)
        elif competencia['mes'] == 2:
            mes_venc, ano_venc = subtrair_mes(mes_atual, ano_atual, competencia['meses'])
        else:
            mes_venc, ano_venc = mes_atual, ano_atual
        dia_venc = int(vencimento['dias'])
        if dia_venc > 30:
            dia_venc = 30
        elif dia_venc < 1:
            dia_venc = 1
        venc_date = int("{}{}{}".format(ano_venc, str(mes_venc).rjust(2, '0'), str(dia_venc).rjust(2, '0')))

        return meta_date, venc_date
    except Exception:
        log(traceback.print_exc())
        return None, None

def defaultConverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__().split('.')[0]
    elif isinstance(o, bson.ObjectId):
        return str(o)
    elif isinstance(o, bytes):
        s = o.decode('UTF-8')
        return str(s)
    else:
        return o

def now():
    d = datetime.datetime.now()
    return str(d).split(".")[0]

gauss_table = [
    {
        "yearA": 1900,
        "yearB": 2019,
        "x": 24,
        "y": 5
    },
    {
        "yearA": 2020,
        "yearB": 2099,
        "x": 24,
        "y": 5
    },
    {
        "yearA": 2100,
        "yearB": 2199,
        "x": 24,
        "y": 6
    },
    {
        "yearA": 2200,
        "yearB": 2299,
        "x": 25,
        "y": 7
    }
]

def getPascoa(year):
    gauss = [item for item in gauss_table if item["yearA"] <= year and item["yearB"] >= year][0]
    x = gauss["x"]
    y = gauss["y"]
    a = year % 19
    b = year % 4
    c = year % 7
    d = (19 * a + x) % 30
    e = (2 * b + 4 * c + 6 * d + y) % 7

    if (d + e) > 9:
        day = d + e - 9
        month = 4
    else:
        day = (d + e + 12)
        month = 3
    if month == 4 and day == 26:
        day = 19
    if month == 4 and day == 25 and d == 28 and a > 10:
        day = 18

    return datetime.date(year, month, day)

def feriados(year):
    feriados = {}
    # Dia 1º
    feriados[f"{year}/01/01"] = "Confraternização Universal"
    # Carnaval
    pascoa = getPascoa(year)
    carnaval = pascoa - datetime.timedelta(48)
    feriados[f"{carnaval.year}/{str(carnaval.month).rjust(2,'0')}/{str(carnaval.day).rjust(2,'0')}"] = "Carnaval"
    carnaval = pascoa - datetime.timedelta(47)
    feriados[f"{carnaval.year}/{str(carnaval.month).rjust(2,'0')}/{str(carnaval.day).rjust(2,'0')}"] = "Carnaval"
    # Paixão de Cristo
    paixao = pascoa - datetime.timedelta(2)
    feriados[f"{paixao.year}/{str(paixao.month).rjust(2,'0')}/{str(paixao.day).rjust(2,'0')}"] = "Paixão de Cristo"
    # Tiradentes
    feriados[f"{year}/04/21"] = "Tiradentes"
    # Dia do Trabalho
    feriados[f"{year}/05/01"] = "Dia do Trabalho"
    # Corpus Christi
    corpus = pascoa + datetime.timedelta(60)
    feriados[f"{corpus.year}/{str(corpus.month).rjust(2,'0')}/{str(corpus.day).rjust(2,'0')}"] = "Corpus Christi"
    # Independência do Brasil
    feriados[f"{year}/09/07"] = "Independência do Brasil"
    # Nossa Sr.a Aparecida
    feriados[f"{year}/10/12"] = "Nossa Sr.a Aparecida"
    # Finados
    feriados[f"{year}/11/02"] = "Finados"
    # Proclamação da República
    feriados[f"{year}/11/15"] = "Proclamação da República"
    # Natal
    feriados[f"{year}/12/25"] = "Natal"

    return feriados

def isStringNumber(value):
    for k in value:
        if k not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' ', '.', ',']:
            return False
    return True

def system(cmdLine=None, wait=True):
    cmd = '%s' % (shlex.split(cmdLine)[0],)
    params = " ".join(['"%s"' % (x,) for x in shlex.split(cmdLine)[1:]])

    # result = subprocess.run([cmd, params], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='ansi').stdout
    p = subprocess.Popen(cmdLine, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='ansi')
    out, err = p.communicate()
    return out, err


if __name__ == "__main__":
    # debug()
    carregar_resumo()