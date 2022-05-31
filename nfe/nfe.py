# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..")))

import atexit
import collections
import codecs
import datetime
import json
import multiprocessing
import re
import sybase
import threading
import time
import tools
import traceback
import difflib
import tempfile
import zipfile

from pymongo import MongoClient
from subheaven.xmltool import *

class Processor(object):
    summadir = "C:\\inetpub\\summa\\files\\nfe"
    mapedfile = "mapedfilesGeral.txt"
    empresa = None
    entregue = False
    fast = False
    mapedfolders = {}
    _receitas = {}
    entregue = False
    histname = "nfe.hist"
    historico = {}
    empresas = {}
    contas = {}
    alertas = {}
    mes_ini = 0
    ano_ini = 0
    mes_fim = 0
    ano_fim = 0
    locallog = []
    documents = []
    xmlns = "{http://www.portalfiscal.inf.br/nfe}"
    client = MongoClient()
    mongo = client['iacon']
    logger = None;
    notas_canceladas = {}

    def __init__(self, historico, logger):
        self.logger = logger;
        if historico != "":
            self.histname = f"{historico}.hist"

        @atexit.register
        def onExit():
            self.logger.save(sn);
            self.client.close()

    def log(self, msg):
        self.locallog.append(msg)
        self.logger.log(msg)

    def Log(self, msg):
        self.log(msg)

    def salvarLog(self, path):
        try:
            if os.path.isfile(path):
                _logpath = os.path.join(os.path.dirname(path), ".summa.log")
            else:
                _logpath = os.path.join(path, ".summa.log")
            with codecs.open(_logpath, "w", "utf-8") as temp:
                temp.write("\n".join(self.locallog))
            self.logger.log("Log gravado em:")
            self.logger.log("    {}".format(_logpath))
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def salvarHistorico(self):
        if sys.version_info[0] < 3:
            _json = json.dumps(self.historico, encoding="utf-8")
        else:
            _json = json.dumps(self.historico)
        with codecs.open(self.histname, "w", "utf-8") as temp:
            temp.write(_json)

    def carregarHistorico(self):
        if os.path.isfile(self.histname):
            with codecs.open(self.histname, "r", "utf-8") as f:
                _str = f.read()
            if _str != "":
                self.historico = json.loads(_str)

    def apagarHistorico(self):
        if os.path.isfile(self.histname):
            os.remove(self.histname)

    def carregar_canceladas(self, caminho):
        if not self.fast:
            self.logger.log("Consultando os arquivos de evento de notas canceladas")
            for (dirpath, dirnames, filenames) in os.walk(caminho):
                for filename in filenames:
                    if len(filename.split('.')) > 1 and filename.split('.')[-1].lower() == 'xml':
                        with codecs.open(os.path.join(dirpath, filename), "r", "utf-8") as f:
                            _str = f.read()
                        _str = _str.lower()
                        chave = _str.split("<chnfe>")
                        if len(chave) > 1:
                            chave = chave[1].split("</chnfe>")[0].strip()
                        else:
                            chave = ""

                        if "<descEvento>Cancelamento</descEvento>".lower() in _str:
                            self.notas_canceladas[chave] = os.path.join(dirpath, filename)
                            self.logger.log(f"    {chave}")

    def consultarEntradaBase(self, chave, empresa):
        try:
            headers = sybase.getHeaders("efentradas")
            sql = "SELECT nfe.{}, acuvig.SIMPLESN_CREDITO_PRESUMIDO_ACU AS tem_simples\n".format(", nfe.".join(headers))
            sql += "FROM bethadba.efentradas as nfe\n"
            sql += " LEFT JOIN bethadba.EFMVEPRO_ICMS_SIMPLES_NACIONAL as simp ON (\n"
            sql += "               nfe.codi_emp = simp.codi_emp\n"
            sql += "               AND nfe.codi_ent = simp.codi_ent)\n"
            sql += "INNER JOIN bethadba.EFACUMULADOR_VIGENCIA AS acuvig ON (\n"
            sql += "               acuvig.codi_emp = nfe.codi_emp\n"
            sql += "               AND acuvig.codi_acu = nfe.codi_acu)\n"
            sql += "WHERE nfe.chave_nfe_ent = '{}'".format(chave)
            if empresa != 0 and empresa != "":
                sql += "  AND nfe.codi_emp = '{}'".format(empresa)
            sql += "AND acuvig.vigencia_acu = ( SELECT MAX(acuvig2.vigencia_acu)\n"
            sql += "                          FROM bethadba.EFACUMULADOR_VIGENCIA AS acuvig2\n"
            sql += "                         WHERE acuvig2.codi_emp = acuvig.codi_emp\n"
            sql += "                           AND acuvig2.codi_acu = acuvig.codi_acu\n"
            sql += "                           AND acuvig2.vigencia_acu <= nfe.dent_ent)\n"
            sql += "GROUP BY nfe.{}, acuvig.SIMPLESN_CREDITO_PRESUMIDO_ACU\n".format(", nfe.".join(headers))
            headers.append("tem_simples")
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def consultarAliquotasEntrada(self, empresa, codigo):
        try:
            headers = sybase.getHeaders("efimpent")
            sql = "SELECT imp.{}\n".format(", imp.".join(headers))
            sql += "FROM bethadba.efentradas as nfe , bethadba.efimpent AS imp\n"
            sql += "WHERE nfe.codi_emp = imp.codi_emp\n"
            sql += "  AND nfe.codi_ent = imp.codi_ent\n"
            sql += "  AND imp.codi_imp = 1\n"
            sql += "  AND nfe.codi_emp = {}\n".format(empresa)
            sql += "  AND nfe.codi_ent = {}\n".format(codigo)
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def consultarProdutosEntrada(self, empresa, codigo):
        try:
            headers = sybase.getHeaders("efmvepro")
            sql = "SELECT prod.{}, cp.desc_pdi AS nome\n".format(
                ", prod.".join(headers))
            sql += "FROM bethadba.efentradas as nfe , bethadba.efmvepro AS prod, bethadba.efprodutos as cp\n"
            sql += "WHERE nfe.codi_emp = prod.codi_emp\n"
            sql += "  AND nfe.codi_ent = prod.codi_ent\n"
            sql += "  AND prod.codi_emp = cp.codi_emp\n"
            sql += "  AND TRIM(prod.codi_pdi) = TRIM(cp.codi_pdi)\n"
            sql += "  AND nfe.codi_emp = {}\n".format(empresa)
            sql += "  AND nfe.codi_ent = {}\n".format(codigo)
            headers.append("nome")
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    # def consultarSimplesEntrada(self, empresa, codigo):
    #     headers = sybase.getHeaders("EFMVEPRO_ICMS_SIMPLES_NACIONAL")
    #     sql = "SELECT simp.{}\n".format(", simp.".join(headers))
    #     sql += "FROM bethadba.efentradas as nfe , bethadba.EFMVEPRO_ICMS_SIMPLES_NACIONAL AS simp\n"
    #     sql += "WHERE nfe.codi_emp = simp.codi_emp\n"
    #     sql += "  AND nfe.codi_ent = simp.codi_ent\n"
    #     sql += "  AND nfe.codi_emp = {}\n".format(empresa)
    #     sql += "  AND nfe.codi_ent = {}\n".format(codigo)
    #     dataset = sybase.select(sql, headers)

    #     return dataset

    def consultarSimplesEntrada(self, chave, empresa):
        try:
            headers = sybase.getHeaders("EFMVEPRO_ICMS_SIMPLES_NACIONAL")
            sql = "SELECT simp.{}\n".format(", simp.".join(headers))
            sql += "FROM bethadba.efentradas as nfe , bethadba.EFMVEPRO_ICMS_SIMPLES_NACIONAL AS simp\n"
            sql += "WHERE nfe.codi_emp = simp.codi_emp\n"
            sql += "  AND nfe.codi_ent = simp.codi_ent\n"
            if empresa != 0 and empresa != "":
                sql += "  AND nfe.codi_emp = '{}'".format(empresa)
            sql += "  AND nfe.chave_nfe_ent = '{}'\n".format(chave)
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit() 

    def consultarSaidaBase(self, chave, empresa):
        try:
            headers = sybase.getHeaders("efsaidas")
            sql = "SELECT nfe.{}, 'N' AS tem_simples\n".format(", nfe.".join(headers))
            sql += "FROM bethadba.efsaidas as nfe\n"
            sql += "WHERE nfe.chave_nfe_sai = '{}'".format(chave)
            if empresa != 0 and empresa != "":
                sql += "  AND nfe.codi_emp = '{}'".format(empresa)
            sql += "GROUP BY nfe.{}\n".format(", nfe.".join(headers))

            headers.append("tem_simples")
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def consultarAliquotasSaida(self, empresa, codigo):
        try:
            headers = sybase.getHeaders("efimpsai")
            sql = "SELECT imp.{}\n".format(", imp.".join(headers))
            sql += "FROM bethadba.efsaidas as nfe , bethadba.efimpsai AS imp\n"
            sql += "WHERE nfe.codi_emp = imp.codi_emp\n"
            sql += "  AND nfe.codi_sai = imp.codi_sai\n"
            sql += "  AND imp.codi_imp = 1\n"
            sql += "  AND nfe.codi_emp = {}\n".format(empresa)
            sql += "  AND nfe.codi_sai = {}\n".format(codigo)
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def consultarProdutosSaida(self, empresa, codigo):
        try:
            headers = sybase.getHeaders("efmvspro")
            sql = "SELECT prod.{}\n".format(", prod.".join(headers))
            sql += "FROM bethadba.efsaidas as nfe, bethadba.efmvspro AS prod\n"
            sql += "WHERE nfe.codi_emp = prod.codi_emp\n"
            sql += "  AND nfe.codi_sai = prod.codi_sai\n"
            sql += "  AND nfe.codi_emp = {}\n".format(empresa)
            sql += "  AND nfe.codi_sai = {}\n".format(codigo)
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def consultarSimplesSaida(self, empresa, codigo):
        try:
            headers = sybase.getHeaders("EFMVSPRO_SIMPLES_NACIONAL_MONOFASICO")
            sql = "SELECT simp.{}\n".format(", simp.".join(headers))
            sql += "FROM bethadba.efsaidas as nfe, bethadba.EFMVSPRO_SIMPLES_NACIONAL_MONOFASICO AS simp\n"
            sql += "WHERE nfe.codi_emp = simp.codi_emp\n"
            sql += "  AND nfe.codi_sai = simp.codi_sai\n"
            sql += "  AND nfe.codi_emp = {}\n".format(empresa)
            sql += "  AND nfe.codi_sai = {}\n".format(codigo)
            dataset = sybase.select(sql, headers)

            return dataset
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def carregarRegrasDev(self):
        regrasEmpresa = []
        path = os.path.abspath(os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "..", "regras"))
        codigo = 0
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if filename.split(".")[0] != "__init__" and filename.split(".")[-1] == "py":
                    codigo += 1
                    regrasEmpresa.append({
                        "codigo": codigo,
                        "descricao": filename.split(".")[0],
                        "arquivo": os.path.join(dirpath, filename)
                    })
        return regrasEmpresa

    def carregarRegrasEmpresa(self, empresa, especial=False, dev=False):
        try:
            regrasEmpresa = []
            processosPath = "C:\\inetpub\\summa\\files\\processos"
            rePath = "{}\\regras_empresa\\{}.json".format(
                processosPath, empresa)
            empresaXML = sybase.buscarempresa(codigo=empresa)
            empresaRegime = empresaXML['regime']

            if especial:
                regrasEmpresa = self.carregarRegras(processosPath, [{"codigo": "003"}])
            elif dev:
                regrasEmpresa = self.carregarRegrasDev()
            else:
                if os.path.isfile(rePath):
                    with codecs.open(rePath, 'r', 'utf-8') as file:
                        reData = json.loads(file.read())

                    if len(reData['conjuntos']) > 0:
                        regrasEmpresa = self.carregarRegras(processosPath, reData['conjuntos'])
                    else:
                        if empresaRegime == '5' or empresaRegime == '1':
                            regrasEmpresa = self.carregarRegras(processosPath, [{"codigo": "001"}])
                        elif empresaRegime == '2':
                            regrasEmpresa = self.carregarRegras(processosPath, [{"codigo": "002"}])
                else:
                    if empresaRegime == '5' or empresaRegime == 1:
                        regrasEmpresa = self.carregarRegras(processosPath, [{"codigo": "001"}])
                    elif empresaRegime == '2':
                        regrasEmpresa = self.carregarRegras(processosPath, [{"codigo": "002"}])
            return regrasEmpresa
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def carregarRegras(self, processosPath, conjuntos):
        if conjuntos == 'nfse':
            regras = []
            regras_path = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'regras'))
            for (dirpath, dirnames, filenames) in os.walk(regras_path):
                for filename in filenames:
                    with codecs.open(os.path.join(regras_path, filename), "r", "utf-8") as f:
                        codigo = f.read()
                    if codigo != '':
                        name = filename.split('.')
                        name.pop()
                        name = ".".join(name)
                        regra = {
                            "name": name,
                            "path": os.path.join(regras_path, filename),
                            "code": codigo,
                            "tipo": ""
                        }

                        if codigo.split('\r\n')[0][0] == "#":
                            regra['tipo'] = codigo.split('\r\n')[0].lower()
                            while regra['tipo'][0] == "#":
                                regra['tipo'] = regra['tipo'][1: len(regra['tipo'])]
                        regras.append(regra)
            return regras
        else:
            confpath = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configuracoes"))
            try:
                regrasEmpresa = []
                regrasId = []
                for conjunto in conjuntos:
                    rcPath = "{}\\regras_conjuntos\\{}.json".format(confpath, conjunto['codigo'])

                    with codecs.open(rcPath, 'r', 'utf-8') as file:
                        rcData = json.loads(file.read())

                    for regra in rcData['regras']:
                        if regra['codigo'] not in regrasId:
                            regrasId.append(regra['codigo'])

                regrasPath = "{}\\regras\\regras.json".format(confpath)
                with codecs.open(regrasPath, 'r', 'utf-8') as file:
                    regras = json.loads(file.read())

                    for regra in regrasId:
                        regrasEmpresa.append(regras[str(regra)])

                return regrasEmpresa
            except Exception as exc:
                self.logger.log(traceback.print_exc())
                self.logger.log(exc)
                tools.exit()

    def consultarNFE(self, chave, empresa):
        try:
            nfe = self.consultarEntradaBase(chave, empresa)
            if len(nfe) > 0:
                for segmento in nfe:
                    segmento["tipo"] = "entrada"
                    segmento["aliquotas"] = self.consultarAliquotasEntrada(segmento["codi_emp"], segmento["codi_ent"])
                    segmento["produtos"] = self.consultarProdutosEntrada(segmento["codi_emp"], segmento["codi_ent"])
                    segmento["simples"] = self.consultarSimplesEntrada(segmento["chave_nfe_ent"], empresa)
            else:
                nfe = self.consultarSaidaBase(chave, empresa)
                if len(nfe) > 0:
                    for segmento in nfe:
                        segmento["tipo"] = "saida"
                        segmento["aliquotas"] = self.consultarAliquotasSaida(segmento["codi_emp"], segmento["codi_sai"])
                        segmento["produtos"] = self.consultarProdutosSaida(segmento["codi_emp"], segmento["codi_sai"])
                        segmento["simples"] = self.consultarSimplesSaida(segmento["codi_emp"], segmento["codi_sai"])

            return nfe
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    def gerarTotais(self):
        try:
            collection = self.mongo['conferencia_notas_periodos']

            mongo_empresas = collection.find({}, {"destinatario.codigo": 1, "emitente.codigo": 1, "_id": 0})
            empresas = {}
            for empresa in mongo_empresas:
                if "emitente" in empresa:
                    empresas[empresa['emitente']['codigo']] = 1
                if "destinatario" in empresa:
                    empresas[empresa['destinatario']['codigo']] = 1
            totais = {}

            for empresa in empresas:
                self.logger.log(f"Processando empresa {empresa}")
                resumo = collection.find({"$or": [{"destinatario.codigo": empresa}, {"emitente.codigo": empresa}]})

                for data in resumo:
                    self.logger.log(f"    {data['chave']}")
                    if not empresa in totais:
                        if not empresa in self.empresas:
                            self.empresas[empresa] = sybase.buscarempresa(codigo=empresa)
                        totais[empresa] = {
                            "codigo": str(empresa),
                            "nome": self.empresas[empresa]['nome'],
                            "periodos": {}
                        }

                    periodo = data['periodo'].replace("-", "")

                    if not periodo in totais[empresa]['periodos']:
                        totais[empresa]['periodos'][periodo] = {
                            "nao_lancadas": {
                                "entradas": 0,
                                "valor_entradas": 0.0,
                                "saidas": 0,
                                "valor_saidas": 0.0,
                                "total": 0
                            },
                            "todos": {
                                "entradas": 0,
                                "valor_entradas": 0.0,
                                "saidas": 0,
                                "valor_saidas": 0.0,
                                "total": 0
                            }
                        }

                    if str(data['tipo']) == "0":
                        totais[empresa]['periodos'][periodo]['todos']['total'] += 1
                        totais[empresa]['periodos'][periodo]['todos']['entradas'] += 1
                        totais[empresa]['periodos'][periodo]['todos']['valor_entradas'] += float(
                            data['valor'])
                    else:
                        totais[empresa]['periodos'][periodo]['todos']['total'] += 1
                        totais[empresa]['periodos'][periodo]['todos']['saidas'] += 1
                        totais[empresa]['periodos'][periodo]['todos']['valor_saidas'] += float(
                            data['valor'])

                    if str(data['dominio']) == "0":
                        if str(data['tipo']) == "0":
                            totais[empresa]['periodos'][periodo]['nao_lancadas']['total'] += 1
                            totais[empresa]['periodos'][periodo]['nao_lancadas']['entradas'] += 1
                            totais[empresa]['periodos'][periodo]['nao_lancadas']['valor_entradas'] += float(
                                data['valor'])
                        else:
                            totais[empresa]['periodos'][periodo]['nao_lancadas']['total'] += 1
                            totais[empresa]['periodos'][periodo]['nao_lancadas']['saidas'] += 1
                            totais[empresa]['periodos'][periodo]['nao_lancadas']['valor_saidas'] += float(
                                data['valor'])

                # _collection = self.mongo.iacon.conferencia_notas_totais
                _collection = self.mongo['conferencia_notas_periodos']
                for empdata in totais:
                    _query = {"$and": [{"codigo": empdata}]}
                    _collection.replace_one(_query, totais[empdata], True)#checked
        except Exception as exc:
            self.logger.log(traceback.print_exc())
            self.logger.log(exc)
            tools.exit()

    # def gerarTotais(self, resumo = None, rootpath="", debug = False):
    #     if resumo == None:
    #         with codecs.open(f"{rootpath}\\resumo.json", "r", "utf-8") as temp1:
    #             resumo = json.loads(temp1.read())

    #     totais_path = f"{rootpath}\\totais.json"
    #     totais = {}
    #     for empresa in resumo:
    #         if debug:
    #             self.logger.log(empresa)
    #         if not empresa in totais:
    #             if not empresa in self.empresas:
    #                 self.empresas[empresa] = sybase.buscarempresa(codigo=empresa)
    #             totais[empresa] = {
    #                 "codigo": str(empresa),
    #                 "nome": self.empresas[empresa]['nome'],
    #                 "periodos": {}
    #             }
    #         for periodo in resumo[empresa]:
    #             if debug:
    #                 self.logger.log(f"    {periodo}")
    #             if not periodo in totais[empresa]:
    #                 totais[empresa]['periodos'][periodo] = {
    #                     "nao_lancadas" : {
    #                         "entradas": 0,
    #                         "valor_entradas": 0.0,
    #                         "saidas": 0,
    #                         "valor_saidas": 0.0,
    #                         "total": 0
    #                     },
    #                     "todos" : {
    #                         "entradas": 0,
    #                         "valor_entradas": 0.0,
    #                         "saidas": 0,
    #                         "valor_saidas": 0.0,
    #                         "total": 0
    #                     }
    #                 }

    #             for chave in resumo[empresa][periodo]:
    #                 if debug:
    #                     self.logger.log(f"        {chave}")
    #                 nota = resumo[empresa][periodo][chave]
    #                 if str(nota['tipo']) == "0":
    #                     totais[empresa]['periodos'][periodo]['todos']['total'] += 1
    #                     totais[empresa]['periodos'][periodo]['todos']['entradas'] += 1
    #                     totais[empresa]['periodos'][periodo]['todos']['valor_entradas'] += float(nota['valor'])
    #                 else:
    #                     totais[empresa]['periodos'][periodo]['todos']['total'] += 1
    #                     totais[empresa]['periodos'][periodo]['todos']['saidas'] += 1
    #                     totais[empresa]['periodos'][periodo]['todos']['valor_saidas'] += float(nota['valor'])

    #                 if str(nota['dominio']) == "0":
    #                     if str(nota['tipo']) == "0":
    #                         totais[empresa]['periodos'][periodo]['nao_lancadas']['total'] += 1
    #                         totais[empresa]['periodos'][periodo]['nao_lancadas']['entradas'] += 1
    #                         totais[empresa]['periodos'][periodo]['nao_lancadas']['valor_entradas'] += float(nota['valor'])
    #                     else:
    #                         totais[empresa]['periodos'][periodo]['nao_lancadas']['total'] += 1
    #                         totais[empresa]['periodos'][periodo]['nao_lancadas']['saidas'] += 1
    #                         totais[empresa]['periodos'][periodo]['nao_lancadas']['valor_saidas'] += float(nota['valor'])
    #     with codecs.open(totais_path, mode="w", encoding="utf-8") as temp:
    #         temp.write(json.dumps(totais, ensure_ascii=False))

    def checkToUpdateFolderedXML(self, chave, data, path, rootpath=""):
        resultpath = os.path.abspath(f"{os.path.dirname(path)}\\..\\result.json")
        try:
            result = {}
            result[chave] = data

            # if os.path.isfile(resultpath):
            #     lockfile = f"{resultpath}.lock"
            #     lock = FileLock(lockfile)
            #     linha = ""
            #     with lock.acquire(timeout=10):
            #         with codecs.open(resultpath, "r", "utf-8") as f:
            #             result = json.loads(f.read())

            #         result[chave] = data

            #         with codecs.open(resultpath, "w", "utf8") as temp:
            #             temp.write(json.dumps(result, ensure_ascii=False))
            # else:
            #     result[chave] = data

            #     with codecs.open(resultpath, "w", "utf8") as temp:
            #         temp.write(json.dumps(result, ensure_ascii=False))

            # self.Log("    {}".format(resultpath))

            empresapath = os.path.abspath(f"{os.path.dirname(path)}\\..\\")
            blocos = empresapath.split("\\")[-1].split("-")
            if len(blocos) == 2 and (len(blocos[0]) == 2 or len(blocos[0]) == 4) and (len(blocos[1]) == 2 or len(blocos[1]) == 4):
                if len(blocos[0]) == 2:
                    mes = blocos[0]
                else:
                    mes = blocos[1]
                if len(blocos[0]) == 4:
                    ano = blocos[0]
                else:
                    ano = blocos[1]
                empresapath = os.path.abspath(f"{empresapath}\\..\\")
                blocos = empresapath.split("\\")[-1].split("-")
                if len(blocos) >= 2:
                    cod_empresa = blocos[0].strip()
                    # periodo = f"{ano}{mes}"
                    # #resumo_path = os.path.abspath(f"{empresapath}\\..\\resumo.json")
                    # resumo_path = f"{rootpath}\\resumo.json"
                    # if os.path.isfile(resumo_path):
                    #     lockfile = f"{resumo_path}.lock"
                    #     lock = FileLock(lockfile)
                    #     linha = ""
                    #     with lock.acquire(timeout=10):
                    #         with codecs.open(resumo_path, "r", "utf-8") as temp9:
                    #             _json = temp9.read()
                    #         resumo = json.loads(_json)

                    #         if not cod_empresa in resumo:
                    #             resumo[cod_empresa] = {}

                    #         if not periodo in resumo[cod_empresa]:
                    #             resumo[cod_empresa][periodo] = {}

                    #         for chave in result:
                    #             resumo[cod_empresa][periodo][chave] = result[chave]

                    #         # _json = json.dumps(resumo, ensure_ascii=False)
                    #         # with codecs.open(resumo_path, mode="w", encoding="utf-8") as temp:
                    #         #     temp.write(_json)
                    # else:
                    #     resumo = {}

                    #     if not cod_empresa in resumo:
                    #         resumo[cod_empresa] = {}

                    #     if not periodo in resumo[cod_empresa]:
                    #         resumo[cod_empresa][periodo] = {}

                    #     for chave in result:
                    #         resumo[cod_empresa][periodo][chave] = result[chave]

                    #     # _json = json.dumps(resumo, ensure_ascii=False)
                    #     # with codecs.open(resumo_path, mode="w", encoding="utf-8") as temp:
                    #     #     temp.write(_json)

                    # self.logger.log("Resumo geral criado em:")
                    # self.logger.log(f"    {resumo_path}")

                    # self.gerarTotais(resumo=resumo, rootpath=rootpath)

                    # self.gerarTotais()

            return True
        except Exception as ex:
            self.logger.log(traceback.print_exc())
            self.logger.log(str(ex))
            return False

    def atualizarCanceladas(self, path):
        collection = self.mongo['conferencia_notas_periodos']
        if os.path.isfile(path):
            pass
        else:
            for (dirpath, dirnames, filenames) in os.walk(path):
                self.logger.log(dirpath)
                for filename in filenames:
                    if filename[-9:].lower() == '_canc.xml':
                        self.logger.log(f"    {filename}")
                        with codecs.open(os.path.join(dirpath, filename), "r", "utf-8") as file:
                            string_xml = file.read()
                        xml = tools.xmltodict(string_xml)
                        mdfe = getFromXML(xml, chain = ["procEventoCTe", "eventoCTe", "infEvento", "detEvento", "evCTeCanceladoMDFe"])
                        if mdfe:
                            self.logger.log(f"        É cancelamento MDF-e. Abortando atualização.")
                        else:
                            self.logger.log(f"        Atualizando status da nota")
                            collection.update_one({"chave": filename[0:-9]}, {"$set": {"canc_arquivo": True}})

    def notaCancelada(self, chave):
        return self.mongo['conferencia_notas_canceladas'].count_documents({"chave": chave}) > 0

    # def notaDesconhecida(self, chave):
    #     if self.mongo['conferencia_notas_desconhecimento'].count_documents({"chave": chave}) > 0:
    #         return True
    #     else:
    #         return False

    # def carregarRegras(self):
    #     regras = []
    #     regras_path = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'regras'))
    #     for (dirpath, dirnames, filenames) in os.walk(regras_path):
    #         for filename in filenames:
    #             with codecs.open(os.path.join(regras_path, filename), "r", "utf-8") as f:
    #                 codigo = f.read()
    #             if codigo != '':
    #                 name = filename.split('.')
    #                 name.pop()
    #                 name = ".".join(name)
    #                 regra = {
    #                     "name": name,
    #                     "path": os.path.join(regras_path, filename),
    #                     "code": codigo,
    #                     "tipo": ""
    #                 }

    #                 if codigo.split('\r\n')[0][0] == "#":
    #                     regra['tipo'] = codigo.split('\r\n')[0].lower()
    #                     while regra['tipo'][0] == "#":
    #                         regra['tipo'] = regra['tipo'][1: len(regra['tipo'])]
    #                 regras.append(regra)
    #     return regras

    def processarRegras():
        pass

    def raw(self, data, key):
        k = "{}_ent".format(key)
        if k in data:
            return data[k]
        if k.upper() in data:
            return data[k.upper()]
        k = "{}_sai".format(key)
        if k in data:
            return data[k]
        if k.upper() in data:
            return data[k.upper()]
        k = "{}_mep".format(key)
        if k in data:
            return data[k]
        if k.upper() in data:
            return data[k.upper()]
        k = "{}_msp".format(key)
        if k in data:
            return data[k]
        if k.upper() in data:
            return data[k.upper()]
        k = "{}_ien".format(key)
        if k in data:
            return data[k]
        if k.upper() in data:
            return data[k.upper()]
        k = "{}_isa".format(key)
        if k in data:
            return data[k]
        if k.upper() in data:
            return data[k.upper()]
        if key in data:
            return data[key]
        if key.upper() in data:
            return data[key.upper()]

        return None

    def valor(self, data, key):
        return float(self.raw(data, key))

    def texto(self, data, key):
        return str(self.raw(data, key))

    def inteiro(self, data, key):
        return int(self.raw(data, key))

    def gravarCancelamento(self, chave, empresa, caminho):
        # Tratar a data atual
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Verificar se já existe registro de nota cancelada e atualizar
        cancelada = self.mongo['conferencia_notas_canceladas'].find_one({ 'chave': chave })
        if cancelada == None:
            self.mongo['conferencia_notas_canceladas'].insert_one({ 'chave': chave, 'caminho': caminho, 'processamento': now })
        else:
            self.mongo['conferencia_notas_canceladas'].update_one({ '_id': cancelada['_id'] }, { '$set': { 'caminho': caminho, 'processamento': now } })

        # Consultar estado da nota da Domínio
        nota_dominio = self.buscar_nota_dominio(empresa['codigo'], chave)
        nota_dominio = self.consultarNFE(chave, empresa['codigo'])
        canc_dominio = nota_dominio != None and len(nota_dominio) > 0 and self.inteiro(nota_dominio[0], "situacao") == 2

        #Verificar se já existe processamento da nota
        processamento = self.mongo['conferencia_notas_periodos'].find_one({ 'chave': chave })
        dominio = 0 if nota_dominio == None or len(nota_dominio) == 0 else 1
        if processamento == None:
            # Tratar periodo
            if int(chave[2:4]) >= 90:
                periodo = f"19{chave[2:4]}-{chave[4:6]}"
            else:
                periodo = f"20{chave[2:4]}-{chave[4:6]}"

            # Criar processamento
            processamento = {
                'empresa': str(empresa['codigo']),
                'arquivo': caminho,
                'processamento': now,
                'periodo': periodo,
                'numero': str(int(chave[25:34])),
                'serie': 'UNICA',
                'tipo': '1',
                'emitente': empresa,
                'destinatario': None,
                'dominio': dominio,
                'canc_arquivo': True,
                'canc_dominio': canc_dominio,
                'propria': False,
                'valor': '',
                'alertas': [
                    'Processamento feito apenas com o evento de cancelamento'
                ],
                'cfops': [],
                'desconhecimento': '',
                'modelo': '',
                'chave': chave
            }

            # Gravar processamento
            self.mongo['conferencia_notas_periodos'].insert_one(processamento)
        else:
            # Atualizar processamento existente
            self.mongo['conferencia_notas_periodos'].update_one({ '_id': processamento['_id'] }, { '$set': { 'canc_arquivo': True, 'dominio': dominio, 'canc_dominio': canc_dominio } })

    def buscar_nota_dominio(self, empresa, chave):
        chnfe = chave.lower().replace("nfe", "")
        sql = ""
        sql += "SELECT nfs.codi_emp AS codi_emp, nfs.nume_sai AS numero, nfs.seri_sai AS serie, MAX(nfs.vprod_sai) AS vprod, MAX(nfs.vcon_sai) AS contabil, MAX(nfs.ipi_sai) AS ipi,\n"
        sql += "      nfs.situacao_sai AS situacao, COALESCE(SUM(imp.vlor_isa),0) AS valor_imposto\n"
        sql += "FROM bethadba.efsaidas nfs\n"
        sql += "     LEFT OUTER JOIN bethadba.efimpsai AS imp\n"
        sql += "               ON    imp.codi_emp = nfs.codi_emp\n"
        sql += "                 AND imp.codi_sai = nfs.codi_sai\n"
        sql += "WHERE ( imp.codi_imp = 1 OR imp.codi_imp IS NULL )\n"
        sql += "  AND nfs.chave_nfe_sai LIKE '{}%'\n".format(chnfe)
        sql += "  AND nfs.codi_emp = {}\n".format(empresa)
        sql += "GROUP BY codi_emp, numero, serie, situacao\n"
        sql += "UNION\n"
        sql += "SELECT nfs.codi_emp AS codi_emp, nfs.nume_ent AS numero, nfs.seri_ent AS serie, MAX(nfs.vprod_ent) AS vprod, MAX(nfs.vcon_ent) AS contabil, MAX(nfs.ipi_ent) AS ipi,\n"
        sql += "      nfs.situacao_ent AS situacao, COALESCE(SUM(imp.vlor_ien),0) AS valor_imposto\n"
        sql += "FROM bethadba.efentradas nfs\n"
        sql += "     LEFT OUTER JOIN bethadba.efimpent AS imp\n"
        sql += "               ON    imp.codi_emp = nfs.codi_emp\n"
        sql += "                 AND imp.codi_ent = nfs.codi_ent\n"
        sql += "WHERE ( imp.codi_imp = 1 OR imp.codi_imp IS NULL )\n"
        sql += "  AND nfs.chave_nfe_ent LIKE '{}%'\n".format(chnfe)
        sql += "  AND nfs.codi_emp = {}\n".format(empresa)
        sql += "GROUP BY codi_emp, numero, serie, situacao"

        consulta = sybase.select(sql, ["codigo", "numero", "serie", "valor_produtos", "valor_contabil", "valor_ipi", "situacao", "imposto"])
        if consulta != None and len(consulta) > 0:
            return consulta[0]
        else:
            return None

    def process(self, path, pack=False, rootpath="", packname="", empresa="", foldered=False, especial=False, dev=False, zip_path=""):
        if True:
        # try:
            # O xml que vai ser carregado em formato de dicionário
            xml = {}
            # A chave da nota fiscal
            chave = ""
            arquivo = zip_path if zip_path != "" else path

            _alertas = []
            _alertas_local = []
            _local_printed = False
            # resultado = True
            abortar_missao = False

            self.alertas["data"] = {
                "nome": packname
            }

            valor = self.valor
            texto = self.texto
            inteiro = self.inteiro
            raw = self.raw

            def alertas(mensagem):
                # As mensagens vindas do script em separado sempre vinha com símbolos estranhos
                #   por isso é necessário converter
                try:
                    _alertas.append(bytes(mensagem, "latin1").decode("utf-8"))
                    _alertas_local.append(bytes(mensagem, "latin1").decode("utf-8"))
                except Exception as ex:
                    _alertas.append(mensagem)
                    _alertas_local.append(mensagem)

            def nota_servico_saida(empresa, numero):
                sql = ""
                sql += "SELECT codi_emp AS codi_emp, nume_ser AS numero, seri_ser AS serie, valor_servicos_ser AS valor, vcon_ser AS contabil, situ_ser AS situacao\n"
                sql += "FROM bethadba.efservicos\n"
                sql += "WHERE codi_emp = {}\n".format(empresa)
                sql += "  AND nume_ser = {}\n".format(numero)
                consulta = sybase.select(sql, ["codigo", "numero", "serie", "valor_servico", "valor_contabil", "situacao"])
                if consulta != None and len(consulta) > 0:
                    return consulta[0]
                else:
                    return None

            def nota_servico_entrada(empresa, numero):
                sql = ""
                sql += "SELECT codi_emp AS codi_emp, nume_ent AS numero, seri_ent AS serie, vcon_ent AS valor, situ_ent AS situacao\n"
                sql += "FROM bethadba.efentradas\n"
                sql += "WHERE codi_emp = {}\n".format(empresa)
                sql += "  AND nume_ent = {}\n".format(numero)
                consulta = sybase.select(sql, ["codigo", "numero", "serie", "valor", "situacao"])
                if consulta != None and len(consulta) > 0:
                    return consulta[0]
                else:
                    return None

            def total(nota, campo):
                valor = 0
                for segmento in nota:
                    if "{}_ent".format(campo) in segmento:
                        valor += float(segmento["{}_ent".format(campo)])
                    if "{}_sai".format(campo) in segmento:
                        valor += float(segmento["{}_sai".format(campo)])
                return valor

            def formatarValor(valor):
                return tools.FormatFloat(valor, thou='.', dec=',')

            def ler_chave(xml):
                chave = xml["@Id"]
                chave = chave.lower().replace("nfe", "").replace("cte", "")
                return chave

            def ler_canc_arquivo(xml):
                canc_arquivo = False
                if "protNFe" in xml:
                    canc_arquivo = int(xml["protNFe"]["infProt"]["cStat"]) == 101
                elif "protNFe" in xml:
                    canc_arquivo = int(xml["protNFe"]["infProt"]["cStat"]) == 101
                    
                return canc_arquivo or self.notaCancelada(chave)

            def get_cnpj(bloco):
                if "CNPJ" in bloco:
                    return bloco["CNPJ"]
                elif "CPF" in bloco:
                    return bloco["CPF"]
            
            def periodo_from_xml(xml):
                if 'GerarNfseResposta' in xml and 'ListaNfse' in xml['GerarNfseResposta']:
                    periodo = xml["GerarNfseResposta"]["ListaNfse"]["CompNfse"]["Nfse"]["InfNfse"]["DataEmissao"].split("T")[0]
                elif xml['versao'] <= 2:
                    periodo = xml["ide"]["dEmi"].split("T")[0]
                else:
                    periodo = xml["ide"]["dhEmi"].split("T")[0]
                periodo = "{}-{}".format(periodo.split("-")[0], periodo.split("-")[1])
                return periodo

            def validar_empresa_no_escritorio(empresa, xml, title):
                dnota = xml['ide']['dhEmi'].split('T')[0]
                if empresa == None:
                    return False
                elif (empresa['inicio'] == None or empresa['inicio'] == "" or empresa['inicio'] <= dnota) and (empresa['datasaida'] == "" or empresa['datasaida'] >= dnota):
                    return True
                else:
                    if empresa['inicio'] == "":
                        self.logger.log(f"        Empresa {title} saiu do escritório em {empresa['datasaida']} e a nota é de {dnota}")
                    elif empresa['datasaida'] == "":
                        self.logger.log(
                            f"        Empresa {title} entrou no escritótio em {empresa['inicio']} e a nota é de {dnota}")
                    else:
                        self.logger.log(
                            f"        Empresa {title} entrou no escritótio em {empresa['inicio']} e saiu em {empresa['datasaida']} e a nota é de {dnota}")
                    return False

            def configuradoParaSimples(empresa, chave):
                sql = "SELECT nfe.codi_emp, nfe.nume_ent, nfe.vcon_ent, acuvig.SIMPLESN_CREDITO_PRESUMIDO_ACU\n"
                sql += "  FROM bethadba.efentradas as nfe\n"
                sql += "       INNER JOIN bethadba.EFACUMULADOR_VIGENCIA AS acuvig\n"
                sql += "            ON    acuvig.codi_emp = nfe.codi_emp\n"
                sql += "              AND acuvig.codi_acu = nfe.codi_acu, \n"
                sql += "       bethadba.EFMVEPRO_ICMS_SIMPLES_NACIONAL as simp\n"
                sql += "WHERE nfe.codi_emp = simp.codi_emp\n"
                sql += "AND nfe.codi_ent = simp.codi_ent\n"
                sql += f"AND nfe.codi_emp = {empresa}\n"
                #sql += f"AND nfe.codi_emp = 118\n"
                sql += "AND acuvig.vigencia_acu = ( SELECT MAX(acuvig2.vigencia_acu)\n"
                sql += "                          FROM bethadba.EFACUMULADOR_VIGENCIA AS acuvig2\n"
                sql += "                         WHERE acuvig2.codi_emp = acuvig.codi_emp\n"
                sql += "                           AND acuvig2.codi_acu = acuvig.codi_acu\n"
                sql += "                           AND acuvig2.vigencia_acu <= nfe.dent_ent )\n"
                sql += f"AND nfe.chave_nfe_ent = '{chave}'\n"
                #sql += f"AND nfe.chave_nfe_ent = '52180404784160000120550010000182021000182022'\n"
                datalist = sybase.select(sql, ["empresa", "nota", "valor", "simples"])
                if len(datalist) > 0:
                    datalist = [data['simples'] for data in datalist if data['simples'] == "S"]
                    return len(datalist) > 0
                return False

            if pack:
                self.historicoID = {}
                p = os.path.join(rootpath, "historico.json")
                if os.path.isfile(p):
                    with codecs.open(p, "r", "utf8") as file:
                        for line in file.readlines():
                            self.historicoID[line] = 0

            # Verificar se o arquivo não está zerado
            if os.stat(path).st_size <= 10:
                alerta = "O xml é vazio. Provalmente é uma nota denegada"
                _alertas.append(alerta)
                _alertas_local.append(alerta)
            else:
                # Transformar o xml em um dicionário
                with codecs.open(path, "r", "utf-8") as file:
                    string_xml = file.read()

                xml = tools.xmltodict(string_xml)

                xml['referencia'] = None
                if 'nfeProc' in xml and 'NFe' in xml['nfeProc'] and 'infNFe' in xml['nfeProc']['NFe'] and 'ide' in xml['nfeProc']['NFe']['infNFe'] and 'NFref' in xml['nfeProc']['NFe']['infNFe']['ide']:
                    tools.debug(xml['nfeProc']['NFe']['infNFe']['ide']['NFref'])
                    ref = xml['nfeProc']['NFe']['infNFe']['ide']['NFref']['refNFe']
                    if isinstance(ref, str):
                        lancada = sybase.buscarnotadominio(chave=ref, empresa=empresa)
                        lancada = len(lancada) > 0
                        _query = {"$and": [{"chave": ref}, {"empresa": empresa}]}
                        encontrada = self.mongo['conferencia_notas_periodos'].find_one(_query) != None
                        xml['referencia'] = {
                            'chave': ref,
                            'lancada': lancada,
                            'encontrada': encontrada
                        }
                    else:
                        self.mongo['apagar_referencia'].insert_one({
                            'caminho': path,
                            'chave': chave
                        })

                if "nfeProc" in xml and "NFe" in xml["nfeProc"] and "NFe" in xml["nfeProc"] and "infNFeSupl" in xml["nfeProc"]["NFe"] and "urlChave" in xml["nfeProc"]["NFe"]["infNFeSupl"]:
                    xml["modelo"] = "NFC-e" if "nfce" in xml["nfeProc"]["NFe"]["infNFeSupl"]["urlChave"] else ""
                elif not "modelo" in xml:
                    xml["modelo"] = ""
                if 'GerarNfseResposta' in xml and 'ListaNfse' in xml['GerarNfseResposta']:
                    servico = xml['GerarNfseResposta']['ListaNfse']['CompNfse']['Nfse']

                    prestador = servico['InfNfse']['DeclaracaoPrestacaoServico']['InfDeclaracaoPrestacaoServico']['Prestador']
                    cnpj0 = prestador['CpfCnpj']['Cnpj'] if 'Cnpj' in prestador['CpfCnpj'] else prestador['CpfCnpj']['Cpf']
                    cnpj1 = str(int(cnpj0)).rjust(14, '0')
                    cnpj2 = str(int(cnpj0)).rjust(11, '0')
                    prestador = sybase.buscarempresa(cnpj=cnpj1)
                    if prestador == None:
                        prestador = sybase.buscarempresa(cnpj=cnpj2)

                    tomador = servico['InfNfse']['DeclaracaoPrestacaoServico']['InfDeclaracaoPrestacaoServico']['Tomador']
                    cnpj0 = tomador['IdentificacaoTomador']['CpfCnpj']['Cnpj'] if 'Cnpj' in tomador['IdentificacaoTomador']['CpfCnpj'] else tomador['IdentificacaoTomador']['CpfCnpj']['Cpf']
                    cnpj1 = str(int(cnpj0)).rjust(14, '0')
                    cnpj2 = str(int(cnpj0)).rjust(11, '0')
                    tomador = sybase.buscarempresa(cnpj=cnpj1)
                    if tomador == None:
                        tomador = sybase.buscarempresa(cnpj=cnpj2)

                    #tomador = sybase.buscarempresa(cnpj=tomador['IdentificacaoTomador']['CpfCnpj']['Cnpj']) if 'Cnpj' in tomador['IdentificacaoTomador']['CpfCnpj'] else sybase.buscarempresa(cnpj=tomador['IdentificacaoTomador']['CpfCnpj']['Cpf'])
                    regras = self.carregarRegras("", "nfse")
                    periodo = periodo_from_xml(xml)
                    d = datetime.datetime.now()
                    nota_dominio = None

                    if prestador != None:
                        empresa = str(prestador['codigo'])
                        tipo = 1
                        nota_dominio = nota_servico_saida(prestador['codigo'], servico['InfNfse']['Numero'])

                        emp = sybase.buscarempresa(codigo=empresa)

                        self.Log("")
                        self.Log("Prestador:")
                        for regra in regras:
                            if regra['tipo'] in ['nfse', 'servico']:
                                codigo = compile(regra['code'], regra['name'], "exec")
                                exec(codigo)
                                self.Log("    {}".format(regra['name']))
                                if len(_alertas_local) > 0:
                                    for alerta in _alertas_local:
                                        self.Log("        {}".format(alerta))
                                    _alertas_local = []
                                else:
                                    self.Log("        Nenhum problema encontrado")
                                _local_printed = True

                        if nota_dominio != None and len(nota_dominio) > 0:
                            tem_dominio = 1
                        else:
                            tem_dominio = 0

                        canc_dominio = nota_dominio != None and nota_dominio["situacao"] == 2

                        periodo = periodo_from_xml(xml)
                        d = datetime.datetime.now()

                        _query = {"$and": [{"periodo": periodo}, {"chave": servico["InfNfse"]["CodigoVerificacao"]}, {"empresa": empresa}]}
                        nota = self.mongo['conferencia_notas_periodos'].find_one(_query)
                        desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                        desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}

                        processamento = {
                            "empresa": empresa,
                            "arquivo": arquivo,
                            "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                            "periodo": periodo,
                            "numero": servico["InfNfse"]["Numero"],
                            "serie": servico["InfNfse"]["DeclaracaoPrestacaoServico"]["InfDeclaracaoPrestacaoServico"]["Rps"]["IdentificacaoRps"]["Serie"],
                            "tipo": tipo,
                            "emitente": prestador,
                            "destinatario": tomador,
                            "dominio": tem_dominio,
                            "canc_arquivo": False,
                            "canc_dominio": canc_dominio,
                            "propria": False,
                            "valor": servico["InfNfse"]["DeclaracaoPrestacaoServico"]["InfDeclaracaoPrestacaoServico"]["Servico"]["Valores"]["ValorServicos"],
                            "alertas": _alertas,
                            "chave": servico["InfNfse"]["CodigoVerificacao"],
                            "cfops": [],
                            "desconhecido": desconhecido,
                            "desconhecido_por": desconhecido_por,
                            "modelo" : "NFS-e"
                        }

                        _id = self.mongo['conferencia_notas_periodos'].replace_one(_query, processamento, True)

                    if tomador != None:
                        empresa = str(tomador['codigo'])
                        tipo = 0
                        nota_dominio = nota_servico_entrada(tomador['codigo'], servico['InfNfse']['Numero'])

                        emp = sybase.buscarempresa(codigo=empresa)

                        self.Log("")
                        self.Log("Tomador:")
                        for regra in regras:
                            if regra['tipo'] in ['nfse', 'servico']:
                                codigo = compile(regra['code'], regra['name'], "exec")
                                exec(codigo)
                                self.Log("    {}".format(regra['name']))
                                if len(_alertas_local) > 0:
                                    for alerta in _alertas_local:
                                        self.Log("        {}".format(alerta))
                                    _alertas_local = []
                                else:
                                    self.Log("        Nenhum problema encontrado")
                                _local_printed = True

                        if nota_dominio != None and len(nota_dominio) > 0:
                            tem_dominio = 1
                        else:
                            tem_dominio = 0

                        canc_dominio = nota_dominio != None and nota_dominio["situacao"] == 2

                        periodo = periodo_from_xml(xml)
                        d = datetime.datetime.now()

                        _query = {"$and": [{"periodo": periodo}, {"chave": servico["InfNfse"]["CodigoVerificacao"]}, {"empresa": empresa}]}
                        nota = self.mongo['conferencia_notas_periodos'].find_one(_query)
                        desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                        desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}

                        processamento = {
                            "empresa": empresa,
                            "arquivo": vars['origin'] if vars['origin'] != '' else arquivo,
                            "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                            "periodo": periodo,
                            "numero": servico["InfNfse"]["Numero"],
                            "serie": servico["InfNfse"]["DeclaracaoPrestacaoServico"]["InfDeclaracaoPrestacaoServico"]["Rps"]["IdentificacaoRps"]["Serie"],
                            "tipo": tipo,
                            "emitente": prestador,
                            "destinatario": tomador,
                            "dominio": tem_dominio,
                            "canc_arquivo": False,
                            "canc_dominio": canc_dominio,
                            "propria": False,
                            "propria_daianny": False,
                            "valor": servico["InfNfse"]["DeclaracaoPrestacaoServico"]["InfDeclaracaoPrestacaoServico"]["Servico"]["Valores"]["ValorServicos"],
                            "alertas": _alertas,
                            "chave": servico["InfNfse"]["CodigoVerificacao"],
                            "cfops": [],
                            "desconhecido": desconhecido,
                            "desconhecido_por": desconhecido_por,
                            "modelo" : "NFS-e"
                        }

                        _id = self.mongo['conferencia_notas_periodos'].replace_one(_query, processamento, True)

                    if empresa == "":
                        dirpath = os.path.dirname(path)
                        if len(dirpath.replace(path, '').split('\\')) > 1 and len(dirpath.replace(path, '').split('\\')[1].split(' -')) > 1:
                            empresa = dirpath.replace(path, '').split('\\')[1].split('- ')[-1]
                        elif prestador != None:
                            empresa = str(prestador['codigo'])
                        elif tomador != None:
                            tomador = str(tomador['codigo'])
                    
                    if prestador != None and str(prestador['codigo']) == empresa:
                        tipo = 1
                    else:
                        tipo = 0

                    emp = sybase.buscarempresa(codigo=empresa)
                    regras = self.carregarRegras("", "nfse")

                    for regra in regras:
                        if regra['tipo'] in ['nfse', 'servico']:
                            codigo = compile(regra['code'], regra['name'], "exec")
                            exec(codigo)
                            self.Log("    {}".format(regra['name']))
                            if len(_alertas_local) > 0:
                                for alerta in _alertas_local:
                                    self.Log("        {}".format(alerta))
                                _alertas_local = []
                            else:
                                self.Log("        Nenhum problema encontrado")
                            _local_printed = True

                    if nota_dominio != None and len(nota_dominio) > 0:
                        tem_dominio = 1
                    else:
                        tem_dominio = 0

                    canc_dominio = nota_dominio != None and nota_dominio["situacao"] == 2

                    periodo = periodo_from_xml(xml)
                    d = datetime.datetime.now()

                    _query = {"$and": [{"periodo": periodo}, {"chave": servico["InfNfse"]["CodigoVerificacao"]}, {"empresa": empresa}]}
                    nota = self.mongo['conferencia_notas_periodos'].find_one(_query)
                    desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                    desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}

                    processamento = {
                        "empresa": empresa,
                        "arquivo": vars['origin'] if vars['origin'] != '' else arquivo,
                        "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                        "periodo": periodo,
                        "numero": servico["InfNfse"]["Numero"],
                        "serie": servico["InfNfse"]["DeclaracaoPrestacaoServico"]["InfDeclaracaoPrestacaoServico"]["Rps"]["IdentificacaoRps"]["Serie"],
                        "tipo": tipo,
                        "emitente": prestador,
                        "destinatario": tomador,
                        "dominio": tem_dominio,
                        "canc_arquivo": False,
                        "canc_dominio": canc_dominio,
                        "propria": False,
                        "propria_daianny": False,
                        "valor": servico["InfNfse"]["DeclaracaoPrestacaoServico"]["InfDeclaracaoPrestacaoServico"]["Servico"]["Valores"]["ValorServicos"],
                        "alertas": _alertas,
                        "chave": servico["InfNfse"]["CodigoVerificacao"],
                        "cfops": [],
                        "desconhecido": desconhecido,
                        "desconhecido_por": desconhecido_por,
                        "modelo" : "NFS-e"
                    }

                    _id = self.mongo['conferencia_notas_periodos'].replace_one(_query, processamento, True)
                else:
                    if string_xml.strip().find("<nfeProc ") > -1:
                        xml = tools.xmltodict(string_xml)
                        #xml = xmltodict.parse(string_xml)

                    elif "<procEventoNFe" in string_xml:
                        xml = None
                        string_xml = string_xml.lower()
                        chave = string_xml.split("<chnfe>")
                        if len(chave) > 1:
                            chave = chave[1].split("</chnfe>")[0].strip()
                        else:
                            chave = ""

                        if "<descEvento>Cancelamento</descEvento>".lower() in string_xml:
                            _alertas.append("O xml é de evento de cancelamento")
                            _alertas_local.append("O xml é de evento de cancelamento")
                            emp = sybase.buscarempresa(codigo=empresa)
                            self.gravarCancelamento(chave, emp, arquivo)
                            # self.marcarNotaCancelada(chave, empresa)
                        # elif "<tpEvento>210220</tpEvento>".lower() in string_xml:
                        #     _alertas.append("O xml é de evento de desconhecimento")
                        #     _alertas_local.append("O xml é de evento de desconhecimento")
                        #     self.marcarDesconhecimento(self, chave, empresa)
                        else:
                            _alertas.append("O xml é de evento")
                            _alertas_local.append("O xml é de evento")

                    elif "<cteProc" in string_xml:
                        xml = tools.xmltodict(string_xml)
                    else:
                        xml = None
                        _alertas.append("O xml é inválido")
                        _alertas_local.append("O xml é inválido")

                    if xml != None:
                        if "nfeProc" in xml and "NFe" in xml["nfeProc"] and "NFe" in xml["nfeProc"] and "infNFeSupl" in xml["nfeProc"]["NFe"] and "urlChave" in xml["nfeProc"]["NFe"]["infNFeSupl"]:
                            xml["modelo"] = "NFC-e" if "nfce" in xml["nfeProc"]["NFe"]["infNFeSupl"]["urlChave"] else ""
                        elif not "modelo" in xml:
                            xml["modelo"] = ""
                        if "nfeProc" in xml:
                            xml = xml["nfeProc"]
                            if "NFe" in xml:
                                xml = xml["NFe"]
                                if "infNFe" in xml:
                                    xml = xml["infNFe"]
                                    canc_arquivo = ("protNFe" in xml and int(xml["protNFe"]["infProt"]["cStat"]) == 101) or self.notaCancelada(chave)
                                else:
                                    _alertas.append("O xml não possui a tag infNFe")
                                    _alertas_local.append("O xml não possui a tag infNFe")
                            else:
                                _alertas.append("O xml não possui a tag NFe")
                                _alertas_local.append("O xml não possui a tag NFe")
                        elif "cteProc" in xml:
                            xml = xml["cteProc"]

                            canc_arquivo = ("protCTe" in xml and int(xml["protCTe"]["infProt"]["cStat"]) == 101) or self.notaCancelada(chave)

                            if "CTe" in xml:
                                xml["modelo"] = "CT-e"
                                xml = xml["CTe"]
                                if "infCte" in xml:
                                    xml = xml["infCte"]
                                else:
                                    _alertas.append("O xml não possui a tag infCte")
                                    _alertas_local.append("O xml não possui a tag infCte")
                            else:
                                _alertas.append("O xml não possui a tag Cte")
                                _alertas_local.append("O xml não possui a tag Cte")
                        else:
                            _alertas.append("O xml não possui a tag nfeProc nem a cteProc")
                            _alertas_local.append("O xml não possui a tag nfeProc nem a cteProc")

                    if xml != None:
                        #######################################################################
                        #######################################################################
                        chave = ler_chave(xml)
                        xml["versao"] = float(xml["@versao"])
                        self.Log(path)
                        # desconhecido = self.notaDesconhecida(chave)

                        if chave in self.notas_canceladas:
                            canc_arquivo = True

                        # Carrega os dados da empresa emitente e destinatário
                        cnpj = None
                        cnpj = get_cnpj(xml["emit"])
                        empresa_emit = None
                        if cnpj != None:
                            empresa_emit = sybase.buscarempresa(cnpj=cnpj)
                            if not validar_empresa_no_escritorio(empresa_emit, xml, "emitente"):
                                empresa_emit = None

                        empresa_dest = None
                        if "dest" in xml:
                            cnpj = None
                            cnpj = get_cnpj(xml["dest"])
                            if cnpj != None:
                                empresa_dest = sybase.buscarempresa(cnpj=cnpj)
                                if not validar_empresa_no_escritorio(empresa_dest, xml, "destin."):
                                    empresa_dest = None

                        empresa_tom = None
                        tomador = None
                        if "toma3" in xml['ide']:
                            tomador = xml['ide']['toma3']['toma']
                        elif "toma4" in xml['ide']:
                            tomador = xml['ide']['toma4']['toma']
                        if tomador != None:
                            cnpj = None
                            if tomador == "0":
                                cnpj = get_cnpj(xml["rem"])
                            elif tomador == "1":
                                cnpj = get_cnpj(xml["exped"])
                            elif tomador == "2":
                                cnpj = get_cnpj(xml["receb"])
                            elif tomador == "3":
                                cnpj = get_cnpj(xml["dest"])
                            elif tomador == "4":
                                cnpj = get_cnpj(xml["ide"]["toma4"])
                            if cnpj != None:
                                empresa_tom = sybase.buscarempresa(cnpj=cnpj)

                            if empresa_tom != None:
                                if not validar_empresa_no_escritorio(empresa_tom, xml, "tomadora"):
                                    empresa_tom = None

                        if empresa == "":
                            if empresa_emit != None:
                                empresa = str(empresa_emit['codigo'])
                            elif empresa_dest != None:
                                empresa = str(empresa_dest['codigo'])
                            elif empresa_tom != None:
                                empresa = str(empresa_tom['codigo'])

                        nota_dominio = self.buscar_nota_dominio(empresa, chave)
                        nota_dominio = self.consultarNFE(chave, empresa)
                        canc_dominio = nota_dominio != None and len(nota_dominio) > 0 and self.inteiro(nota_dominio[0], "situacao") == 2

                        # sit = self.inteiro(nota_dominio[0], "situacao") if len(nota_dominio) > 0 else None


                        # Verificando se a nota está lançada pra empresa correta
                        empresa_certa = True
                        if len(nota_dominio) > 0:
                            def conferir_empresa_certa(empresa):
                                return empresa != None and str(empresa["codigo"]) == str(nota_dominio[0]["codi_emp"])
                            empresa_certa = conferir_empresa_certa(empresa_emit) or conferir_empresa_certa(empresa_dest) or conferir_empresa_certa(empresa_tom)
                            tipo = nota_dominio[0]["tipo"]

                        # Carrega os CFOPs dos produtos da nota
                        cfops = []
                        for nota in nota_dominio:
                            for prod in nota['produtos']:
                                cfop_cod = self.texto(prod, "cfop")
                                if cfop_cod != None and cfop_cod not in cfops:
                                    cfops.append(cfop_cod)

                        # Carrega se a nota é própria ou não
                        #
                        # Quando uma empresa emite uma nota do tipo 0, Entrada, ela é automaticamente própria
                        # https://app.gitkraken.com/glo/board/W8iKZgtLqg4A5yVW/card/XhYYHAV0bQAQzL47
                        xml["ide"]["propria"] = False
                        empresa_process = None
                        # https://app.gitkraken.com/glo/board/W8iKZgtLqg4A5yVW/card/Xmuih_xk8AARjt8Y
                        # if xml['ide']['tpNF'] == "0":
                        #     xml["ide"]["propria"] = True
                        self.logger.log("Verificando se a nota é própria:")
                        if empresa != "":
                            empresa_process = sybase.buscarempresa(codigo=empresa)

                        xml['tipo'] = "nfe"
                        if 'tpNF' in xml['ide'] and xml['ide']['tpNF'] == "0" and empresa_process != None and empresa_process['cnpj'] in chave:
                            xml["ide"]["propria"] = True
                            self.logger.log(f"        tpNF           = {xml['ide']['tpNF']}")
                            self.logger.log(f"        tpNF igual a 0 = {xml['ide']['tpNF'] == '0'}")
                            self.logger.log(f"        cnpj           = {empresa_process['cnpj']}")
                            self.logger.log(f"        cnpj na chave  = {empresa_process['cnpj'] in chave}")
                            self.logger.log(f"    Resultado:")
                        elif 'tpCTe' in xml['ide']:
                            xml['tipo'] = "cte"
                            self.logger.log(f"        CTe é sempre entrada")
                        elif empresa != "":
                            self.logger.log(f"        Empresa foi informada")
                            self.logger.log(f"        empresa          = {empresa}")
                            if len(nota_dominio) > 0:
                                self.logger.log(f"        len(nota_dominio) > 0 = {len(nota_dominio) > 0}")
                                if tipo == "entrada" and empresa_process["cnpj"] in chave:
                                    xml["ide"]["propria"] = True
                                elif tipo == "saida" and empresa_process["cnpj"] not in chave:
                                    xml["ide"]["propria"] = True
                                self.logger.log(f"        tipo           = {tipo}")
                                self.logger.log(f"        é saída        = {tipo == 'saida'}")
                                self.logger.log(f"        cnpj           = {empresa_process['cnpj']}")
                                self.logger.log(f"        cnpj na chave  = {empresa_process['cnpj'] in chave}")
                                self.logger.log(f"    Resultado:")
                            if empresa_process != None:
                                self.logger.log(f"        Empresa está na Domínio")
                                self.logger.log(f"        dominio != None  = {empresa_process != None}")
                                if empresa_dest != None and str(empresa_dest["codigo"]) == empresa:
                                    self.logger.log(f"        Empresa é destinatário")
                                    self.logger.log(f"        dest['codigo'] == empresa = {str(empresa_dest['codigo']) == empresa}")
                                    xml["ide"]["tpNF"] = "0"
                                    if empresa_process["cnpj"] in chave:
                                        xml["ide"]["propria"] = True

                        # Regra alternativa da Daianny de notas próprias
                        # https://trello.com/c/GChBXVFh/356-criar-novo-tipo-de-nota-pr%C3%B3pria-na-malha-antecipada
                        try:
                            xml['propria_daianny'] = False
                            if 'dest' in xml:
                                dest_daianny = ''
                                if 'CNPJ' in xml['dest']:
                                    dest_daianny = xml['dest']['CNPJ']
                                elif 'CPF' in xml['dest']:
                                    dest_daianny = xml['dest']['CPF']
                                if dest_daianny == empresa_process['cnpj']:
                                    if 'det' in xml:
                                        if isinstance(xml['det'], list):
                                            for item in xml['det']:
                                                if item['prod']['CFOP'][0:1] in ['1', '2']:
                                                    xml['propria_daianny'] = True
                                                    break
                                        else:
                                            xml['propria_daianny'] = xml['det']['prod']['CFOP'][0:1] in ['1', '2']
                        except Exception as ex:
                            tools.debug(type(xml['det']), xml['det'])
                            print(ex)
                            tools.exit()

                        self.logger.log(f"        CNPJ na chave")
                        self.logger.log(f"        cnpj             = {empresa_process['cnpj']}")
                        self.logger.log(f"        cnpj na chave    = {empresa_process['cnpj'] in chave}")
                        self.logger.log(f"    Resultado:")
                        self.logger.log(f"        {xml['ide']['propria']}")

                        periodo = periodo_from_xml(xml)

                        processamento_destinatario = {}
                        processamento_emitente = {}
                        processamento_tomador = {}

                        # Verifica se há regras cadastradas para a empresa
                        regrasEmpresa = self.carregarRegrasEmpresa(empresa, especial=especial, dev=dev)

                        self.logger.log("Processando regras:")
                        if len(regrasEmpresa) > 0:
                            for regra in regrasEmpresa:
                                if xml['tipo'] == 'nfe' or ('cte' in regra and  regra['cte']):
                                    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "regras")
                                    filename = regra['arquivo'].split('\\')[-1]

                                    try:
                                        with codecs.open(os.path.join(filepath, filename), "r", "utf-8") as f:
                                            codigo = f.read()
                                        _alertas_local = []
                                        regra = compile(codigo, filename, "exec")
                                        resposta = exec(regra)
                                        self.Log("    {}".format(filename.split(".")[0]))
                                        if len(_alertas_local) > 0:
                                            for alerta in _alertas_local:
                                                self.Log("        {}".format(alerta))
                                            _alertas_local = []
                                        else:
                                            self.Log("        Nenhum problema encontrado")
                                        _local_printed = True
                                        if abortar_missao:
                                            sys.exit()
                                    except Exception as ex:
                                        self.logger.log("")
                                        self.logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                        self.logger.log(f"Erro ao tentar processar o NFE. Favor enviar o xml para o suporte")
                                        self.logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                        self.logger.log(traceback.print_exc())
                                        self.logger.log("")
                                        self.logger.log(type(ex))
                                        self.logger.log(ex.args)
                                        tools.exit()
                        else:
                            filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "regras")

                            for (dirpath, dirnames, filenames) in os.walk(filepath):
                                for filename in filenames:
                                    try:
                                        with codecs.open(os.path.join(filepath, filename), "r", "utf-8") as f:
                                            codigo = f.read()
                                        _alertas_local = []
                                        regra = compile(codigo, filename, "exec")
                                        resposta = exec(regra)
                                        self.Log("    {}".format(filename.split(".")[0]))
                                        if len(_alertas_local) > 0:
                                            for alerta in _alertas_local:
                                                self.Log("        {}".format(alerta))
                                            _alertas_local = []
                                        else:
                                            self.Log("        Nenhum problema encontrado")
                                        _local_printed = True
                                        if abortar_missao:
                                            sys.exit()
                                    except Exception as ex:
                                        self.logger.log("")
                                        self.logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                        self.logger.log(f"Erro ao tentar processar o NFE. Favor enviar o xml para o suporte")
                                        self.logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                        self.logger.log(traceback.print_exc())
                                        self.logger.log("")
                                        self.logger.log(type(ex))
                                        self.logger.log(ex.args)
                                        tools.exit()
                        #######################################################################
                        #######################################################################

                        if "nfeProc" in xml and "NFe" in xml["nfeProc"] and "NFe" in xml["nfeProc"] and "infNFeSupl" in xml["nfeProc"]["NFe"] and "urlChave" in xml["nfeProc"]["NFe"]["infNFeSupl"]:
                            xml["modelo"] = "NFC-e" if "nfce" in xml["nfeProc"]["NFe"]["infNFeSupl"]["urlChave"] else ""
                        elif not "modelo" in xml:
                            xml["modelo"] = ""

                        if chave != "":
                            if empresa_emit != None:
                                emit = {
                                    "codigo": empresa_emit["codigo"],
                                    "cnpj": empresa_emit["cnpj"]
                                }
                            else:
                                emit = None
                            if empresa_dest != None:
                                dest = {
                                    "codigo": empresa_dest["codigo"],
                                    "cnpj": empresa_dest["cnpj"]
                                }
                            else:
                                dest = None
                            if empresa_tom != None:
                                tom = {
                                    "codigo": empresa_tom["codigo"],
                                    "cnpj": empresa_tom["cnpj"]
                                }
                            else:
                                tom = None
                            if nota_dominio != None and len(nota_dominio) > 0:
                                tem_dominio = 1
                            else:
                                tem_dominio = 0

                            if cfops == None:
                                cfops = []

                            def processar_pos(empresa, xml, nota_dominio):
                                periodo = periodo_from_xml(xml)
                                chave = ler_chave(xml)
                                if not empresa["codigo"] in self.alertas:
                                    self.alertas[empresa["codigo"]] = {}
                                if not periodo in self.alertas[empresa["codigo"]]:
                                    self.alertas[empresa["codigo"]][periodo] = {}

                                if xml['tipo'] == 'nfe':
                                    tipo = xml["ide"]["tpNF"]
                                    if nota_dominio != None and len(nota_dominio) > 0:
                                        if nota_dominio[0]['tipo'] == 'entrada':
                                            tipo = "0"
                                        else:
                                            tipo = "1"
                                    elif foldered:
                                        for bloco in path.split("\\"):
                                            if bloco.lower() in ["entrada", "entradas"]:
                                                tipo = "0"
                                            elif bloco.lower() in ["saida", "saidas""saída", "saídas"]:
                                                tipo = "1"
                                elif xml['tipo'] == 'cte':
                                    tipo = 0

                                d = datetime.datetime.now()
                                if "nCT" in xml["ide"]:
                                    numero = xml["ide"]["nCT"]
                                else:
                                    numero = xml["ide"]["nNF"]

                                # Eu estava com dúvida qual o campo do valor, de acordo com a Daianny, é o vTPrest
                                if "vPrest" in xml:
                                    # if xml["vPrest"]["vTPrest"] != xml["vPrest"]["vRec"]:
                                    valor_nota = xml["vPrest"]["vTPrest"]
                                else:
                                    valor_nota = xml["total"]["ICMSTot"]["vNF"]
                                
                                self.alertas[empresa["codigo"]][periodo][chave]["processamentos"] = f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}"
                                self.alertas[empresa["codigo"]][periodo][chave]["numero"] = numero
                                self.alertas[empresa["codigo"]][periodo][chave]["valor"] = valor_nota
                                self.alertas[empresa["codigo"]][periodo][chave]["tipo"] = tipo
                            
                            def processar_opcionais(empresa, processamento, xml, foldered):
                                periodo = periodo_from_xml(xml)
                                chave = ler_chave(xml)
                                processamento[chave] = self.alertas[empresa["codigo"]][periodo][chave]
                                filepath = "C:\\inetpub\\summa\\files\\nfe\\{}-{}.json".format(empresa["codigo"], periodo)
                                self.Log(f"Gravando processamento do da nota:")

                                # _collection = self.mongo.iacon.conferencia_notas_periodos
                                _collection = self.mongo['conferencia_notas_periodos']
                                self.Log("    Processando a opção pack")

                                for _chave in processamento:
                                    if processamento[_chave] != None:
                                        processamento[_chave]['chave'] = _chave
                                        _query = {"$and": [{"periodo": processamento[_chave]['periodo']}, {"chave": _chave}, {"empresa": processamento[_chave]['empresa']}]}

                                        # _id = self.mongo.iacon.conferencia_notas_periodos.replace_one(_query, processamento[_chave], True)
                                        _id = self.mongo['conferencia_notas_periodos'].replace_one(_query, processamento[_chave], True)

                                        if _id.upserted_id != None:
                                            self.documents.append(_id.upserted_id)
                                        else:
                                            self.documents.append(_collection.find(_query).next()['_id'])

                                self.Log("    Processando a opção foldered")
                                if foldered:
                                    blocos = os.path.dirname(path).split("\\")
                                    if len(blocos) > 3 and len(blocos[-3:-2][0].split("-")) > 1:
                                        if int(blocos[-3:-2][0].split('-')[0].strip()) != (empresa['codigo']):
                                            processamento[chave]['alertas'].append(f"A empresa do XML ({empresa['codigo']}) não é a mesma da pasta ({blocos[-3:-2][0].split('-')[0].strip()})")
                                    while not self.checkToUpdateFolderedXML(chave, processamento[chave], path, rootpath=rootpath):
                                        time.sleep(0.5)
                                        pass
                                self.Log("    Fim dos processamentos opcionais.")

                            d = datetime.datetime.now()
                            if tom != None:
                                if not empresa_tom["codigo"] in self.alertas:
                                    self.alertas[empresa_tom["codigo"]] = {}
                                if not periodo in self.alertas[empresa_tom["codigo"]]:
                                    self.alertas[empresa_tom["codigo"]][periodo] = {}

                                _query = {"$and": [{"periodo": periodo}, {"chave": chave}, {"empresa": empresa}]}
                                nota = self.mongo['conferencia_notas_periodos'].find_one(_query)

                                desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                                desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}

                                self.alertas[empresa_tom["codigo"]][periodo][chave] = {
                                    "empresa": empresa,
                                    "arquivo": vars['origin'] if vars['origin'] != '' else arquivo,
                                    "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                                    "periodo": periodo,
                                    "numero": "",
                                    "serie": xml["ide"]["serie"],
                                    "tipo": "",
                                    "emitente": emit,
                                    "destinatario": dest,
                                    "tomador": tom,
                                    "dominio": tem_dominio,
                                    "canc_arquivo": canc_arquivo,
                                    "canc_dominio": canc_dominio,
                                    "propria": xml["ide"]["propria"],
                                    "valor": 0,
                                    "alertas": _alertas,
                                    "cfops": cfops,
                                    "modelo" : xml["modelo"],
                                    "propria_daianny": xml['propria_daianny'],
                                    "desconhecido": desconhecido,
                                    "desconhecido_por": desconhecido_por
                                }
                                processar_pos(empresa_tom, xml, nota_dominio)
                                processar_opcionais(empresa_tom, processamento_tomador, xml, foldered)
                            elif dest != None:
                                if not empresa_dest["codigo"] in self.alertas:
                                    self.alertas[empresa_dest["codigo"]] = {}
                                if not periodo in self.alertas[empresa_dest["codigo"]]:
                                    self.alertas[empresa_dest["codigo"]][periodo] = {}

                                if xml['tipo'] == 'nfe':
                                    tipo = xml["ide"]["tpNF"]
                                    if nota_dominio != None and len(nota_dominio) > 0:
                                        if nota_dominio[0]['tipo'] == 'entrada':
                                            tipo = "0"
                                        else:
                                            tipo = "1"
                                    elif foldered:
                                        for bloco in path.split("\\"):
                                            if bloco.lower() in ["entrada", "entradas"]:
                                                tipo = "0"
                                            elif bloco.lower() in ["saida", "saidas""saída", "saídas"]:
                                                tipo = "1"
                                else:
                                    tipo = 0

                                d = datetime.datetime.now()
                                if "nCT" in xml["ide"]:
                                    numero = xml["ide"]["nCT"]
                                else:
                                    numero = xml["ide"]["nNF"]
                                # Eu estava com dúvida qual o campo do valor, de acordo com a Daianny, é o vTPrest
                                if "vPrest" in xml:
                                    # if xml["vPrest"]["vTPrest"] != xml["vPrest"]["vRec"]:
                                    valor_nota = xml["vPrest"]["vTPrest"]
                                else:
                                    valor_nota = xml["total"]["ICMSTot"]["vNF"]
                                
                                _query = {"$and": [{"periodo": periodo}, {"chave": chave}, {"empresa": empresa}]}
                                nota = self.mongo['conferencia_notas_periodos'].find_one(_query)

                                desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                                desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}

                                self.alertas[empresa_dest["codigo"]][periodo][chave] = {
                                    "empresa": empresa,
                                    "arquivo": vars['origin'] if vars['origin'] != '' else arquivo,
                                    "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                                    "periodo": periodo,
                                    "numero": numero,
                                    "serie": xml["ide"]["serie"],
                                    "tipo": tipo,
                                    "emitente": emit,
                                    "destinatario": dest,
                                    "dominio": tem_dominio,
                                    "canc_arquivo": canc_arquivo,
                                    "canc_dominio": canc_dominio,
                                    "propria": xml["ide"]["propria"],
                                    "valor": valor_nota,
                                    "alertas": _alertas,
                                    "cfops": cfops,
                                    "modelo" : xml["modelo"],
                                    "propria_daianny": xml['propria_daianny'] and tipo == 0,
                                    "desconhecido": desconhecido,
                                    "desconhecido_por": desconhecido_por
                                }

                                processamento_destinatario[chave] = self.alertas[empresa_dest["codigo"]][periodo][chave]
                                filepath = "C:\\inetpub\\summa\\files\\nfe\\{}-{}.json".format(empresa_dest["codigo"], periodo)
                                self.Log("Gravando processamento do destinatário da nota:")

                                # _collection = self.mongo.iacon.conferencia_notas_periodos
                                _collection = self.mongo['conferencia_notas_periodos']
                                self.Log("    Processando a opção pack")

                                for _chave in processamento_destinatario:
                                    if processamento_destinatario[_chave]['destinatario'] != None:
                                        processamento_destinatario[_chave]['chave'] = _chave
                                        emp = empresa if isinstance(empresa, str) else str(empresa['codigo'])
                                        _query = {"$and": [{"periodo": processamento_destinatario[_chave]['periodo']}, {"chave": _chave}, {"empresa": emp}]}

                                        # _id = self.mongo.iacon.conferencia_notas_periodos.replace_one(_query, processamento_destinatario[_chave], True)
                                        _id = self.mongo['conferencia_notas_periodos'].replace_one(_query, processamento_destinatario[_chave], True)

                                        if _id.upserted_id != None:
                                            self.documents.append(_id.upserted_id)
                                        else:
                                            self.documents.append(_collection.find(_query).next()['_id'])

                                self.Log("    Processando a opção foldered")
                                if foldered:
                                    blocos = os.path.dirname(path).split("\\")
                                    if len(blocos) > 3 and len(blocos[-3:-2][0].split("-")) > 1:
                                        if tools.isStringNumber(blocos[-3:-2][0].split('-')[0]):
                                            emp_cod = int(blocos[-3:-2][0].split('-')[0].strip())
                                        else:
                                            emp_cod = int(blocos[-3:-2][0].split('-')[1].strip())
                                        if emp_cod != (empresa_dest['codigo']):
                                            processamento_destinatario[chave]['alertas'].append(f"A empresa do XML ({empresa_dest['codigo']}) não é a mesma da pasta ({blocos[-3:-2][0].split('-')[0].strip()})")
                                    while not self.checkToUpdateFolderedXML(chave, processamento_destinatario[chave], path, rootpath=rootpath):
                                        time.sleep(0.5)
                                        pass
                                self.Log("    Fim dos processamentos opcionais.")

                            elif emit != None:
                                if not empresa_emit["codigo"] in self.alertas:
                                    self.alertas[empresa_emit["codigo"]] = {}
                                if not periodo in self.alertas[empresa_emit["codigo"]]:
                                    self.alertas[empresa_emit["codigo"]][periodo] = {}

                                if "tpCTe" in xml["ide"]:
                                    tipo = xml["ide"]["tpCTe"]
                                else:
                                    tipo = xml["ide"]["tpNF"]
                                if nota_dominio != None and len(nota_dominio) > 0:
                                    if nota_dominio[0]['tipo'] == 'entrada':
                                        tipo = "0"
                                    else:
                                        tipo = "1"
                                elif foldered:
                                    for bloco in path.split("\\"):
                                        if bloco.lower() in ["entrada", "entradas"]:
                                            tipo = "0"
                                        elif bloco.lower() in ["saida", "saidas", "saída", "saídas"]:
                                            tipo = "1"
                                d = datetime.datetime.now()

                                _query = {"$and": [{"periodo": periodo}, {"chave": chave}, {"empresa": empresa}]}
                                nota = self.mongo['conferencia_notas_periodos'].find_one(_query)

                                desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                                desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}
                                
                                if xml["tipo"] == 'cte':
                                    numero = xml["ide"]["nCT"]
                                    valor = xml["infCTeNorm"]["infCarga"]["vCarga"]
                                else:
                                    numero = xml["ide"]["nNF"]
                                    valor = xml["total"]["ICMSTot"]["vNF"]

                                self.alertas[empresa_emit["codigo"]][periodo][chave] = {
                                    "empresa": empresa,
                                    "arquivo": vars['origin'] if vars['origin'] != '' else arquivo,
                                    "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                                    "periodo": periodo,
                                    "numero": numero,
                                    "serie": xml["ide"]["serie"],
                                    "tipo": tipo,
                                    "emitente": emit,
                                    "destinatario": dest,
                                    "dominio": tem_dominio,
                                    "canc_arquivo": canc_arquivo,
                                    "canc_dominio": canc_dominio,
                                    "propria": xml["ide"]["propria"],
                                    "valor": valor,
                                    "alertas": _alertas,
                                    "cfops": cfops,
                                    "modelo" : xml["modelo"],
                                    "propria_daianny": xml['propria_daianny'] and tipo == 0,
                                    "desconhecido": desconhecido,
                                    "desconhecido_por": desconhecido_por
                                }

                                processamento_emitente[chave] = self.alertas[empresa_emit["codigo"]][periodo][chave]
                                filepath = "C:\\inetpub\\summa\\files\\nfe\\{}-{}.json".format(empresa_emit["codigo"], periodo)
                                self.Log("Gravando processamento do emitente da nota:")
                                self.Log("    {}".format(filepath))

                                # _collection = self.mongo.iacon.conferencia_notas_periodos
                                _collection = self.mongo['conferencia_notas_periodos']
                                for chave in processamento_emitente:
                                    if processamento_emitente[chave]['emitente'] != None:
                                        processamento_emitente[chave]['chave'] = chave
                                        if isinstance(empresa, str):
                                            cod_emp = empresa
                                        else:
                                            cod_emp = str(empresa['codigo'])
                                        _query = {"$and": [{"periodo": processamento_emitente[chave]['periodo']}, {"chave": chave}, {"empresa": cod_emp}]}

                                        # _id = self.mongo.iacon.conferencia_notas_periodos.replace_one(_query, processamento_emitente[chave], True)#checked
                                        _id = self.mongo['conferencia_notas_periodos'].replace_one(_query, processamento_emitente[chave], True)#checked

                                        if _id.upserted_id != None:
                                            self.documents.append(_id.upserted_id)
                                        else:
                                            self.documents.append(_collection.find(_query).next()['_id'])

                                if foldered:
                                    blocos = os.path.dirname(path).split("\\")
                                    if len(blocos) > 3 and len(blocos[-3:-2][0].split("-")) > 1:
                                        if blocos[-3:-2][0].split('-')[0].strip() != str(empresa_emit['codigo']):
                                            processamento_emitente[chave]['alertas'].append(f"A empresa do XML ({empresa_emit['codigo']}) não é a mesma da pasta ({blocos[-3:-2][0].split('-')[0].strip()})")
                                    while not self.checkToUpdateFolderedXML(chave, processamento_emitente[chave], path, rootpath=rootpath):
                                        time.sleep(0.5)
                                        pass
                            else:
                                if not "avulsas" in self.alertas:
                                    self.alertas["avulsas"] = {}
                                if not periodo in self.alertas["avulsas"]:
                                    self.alertas["avulsas"][periodo] = {}

                                if "tpCTe" in xml["ide"]:
                                    tipo = xml["ide"]["tpCTe"]
                                else:
                                    tipo = xml["ide"]["tpNF"]
                                if nota_dominio != None and len(nota_dominio) > 0:
                                    if nota_dominio[0]['tipo'] == 'entrada':
                                        tipo = "0"
                                    else:
                                        tipo = "1"
                                elif foldered:
                                    for bloco in path.split("\\"):
                                        if bloco.lower() in ["entrada", "entradas"]:
                                            tipo = "0"
                                        elif bloco.lower() in ["saida", "saidas", "saída", "saídas"]:
                                            tipo = "1"

                                d = datetime.datetime.now()

                                _query = {"$and": [{"periodo": periodo}, {"chave": chave}, {"empresa": empresa}]}
                                nota = self.mongo['conferencia_notas_periodos'].find_one(_query)

                                desconhecido = nota['desconhecido'] if nota != None and 'desconhecido' in nota else False
                                desconhecido_por =  nota['desconhecido_por'] if nota != None and 'desconhecido_por' in nota else {'atualizado_em': '', 'usuario': ''}

                                self.alertas["avulsas"][periodo][chave] = {
                                    "empresa": empresa,
                                    "arquivo": vars['origin'] if vars['origin'] != '' else arquivo,
                                    "periodo": periodo,
                                    "processamento": f"{d.year}-{str(d.month).rjust(2,'0')}-{str(d.day).rjust(2,'0')} {str(d.hour).rjust(2,'0')}:{str(d.minute).rjust(2,'0')}:{str(d.second).rjust(2,'0')}",
                                    "numero": xml["ide"]["nCT"] if "nCT" in xml["ide"] else xml["ide"]["nNF"],
                                    "serie": xml["ide"]["serie"],
                                    "tipo": tipo,
                                    "emitente": emit,
                                    "destinatario": dest,
                                    "dominio": tem_dominio,
                                    "canc_arquivo": canc_arquivo,
                                    "canc_dominio": canc_dominio,
                                    "propria": xml["ide"]["propria"],
                                    "valor": xml["vPrest"]["vTPrest"] if "vPrest" in xml else xml["total"]["ICMSTot"]["vNF"],
                                    "alertas": _alertas,
                                    "cfops": cfops,
                                    "modelo" : xml["modelo"],
                                    "propria_daianny": xml['propria_daianny'] and tipo == 0,
                                    "desconhecido": desconhecido,
                                    "desconhecido_por": desconhecido_por
                                }

            if pack:
                _basepath = os.path.join(rootpath, "summa.json")
                _json = json.dumps(mapper.alertas, ensure_ascii=False)
                with codecs.open(_basepath, "w", "utf-8") as temp:
                    temp.write(_json)

            if not _local_printed:
                if len(_alertas_local) > 0:
                    for alerta in _alertas_local:
                        self.Log("    {}".format(alerta))
                else:
                    self.Log("    Nenhuma problema encontrado")

            if not _local_printed:
                if len(_alertas_local) > 0:
                    for alerta in _alertas_local:
                        self.Log("    {}".format(alerta))
                else:
                    self.Log("    Nenhuma problema encontrado")
        # except Exception as exc:
        #     self.logger.log(exc)
        #     self.logger.log(traceback.print_exc())
        #     tools.exit()

    def apagarNotasForaPeriodoEmpresa(self):
        mongo = self.mongo['conferencia_notas_periodos'].find()
        cont = 0
        for nota in mongo:
            self.logger.log(nota['chave'])
            nota['_id'] = str(nota['_id'])

            if nota['emitente'] != None:
                empresa = sybase.buscarempresa(codigo=nota['emitente']['codigo'])
                saida = "" if empresa['datasaida'] == "" else empresa['datasaida'].split(" ")[0][0:-3]
                entrada = "" if empresa['inicio'] == "" else empresa['inicio'].split(" ")[0][0:-3]
                manter = (entrada == "" or entrada <= nota['periodo']) and (saida == "" or saida >= nota['periodo'])
                if not manter:
                    if entrada == "" and saida == "":
                        self.logger.log(f"    Empresa {empresa['codigo']} entrou em {entrada} e saiu em {saida} mas a nota é de {nota['periodo']}")
                    elif saida == "":
                        self.logger.log(f"    Empresa {empresa['codigo']} entrou em {entrada} mas a nota é de {nota['periodo']}")
                    else:
                        self.logger.log(f"    Empresa {empresa['codigo']} saiu em {saida} mas a nota é de {nota['periodo']}")

                    self.mongo['conferencia_notas_periodos'].update_one(
                        {'chave': nota['chave']}, {'$set': {'emitente': None}})
                    nota['emitente'] = None

            if nota['destinatario'] != None:
                empresa = sybase.buscarempresa(codigo=nota['destinatario']['codigo'])
                saida = "" if empresa['datasaida'] == "" else empresa['datasaida'].split(" ")[0][0:-3]
                entrada = "" if empresa['inicio'] == "" else empresa['inicio'].split(" ")[0][0:-3]
                manter = (entrada == "" or entrada <= nota['periodo']) and (saida == "" or saida >= nota['periodo'])
                if not manter:
                    if entrada == "" and saida == "":
                        self.logger.log(f"    Empresa {empresa['codigo']} entrou em {entrada} e saiu em {saida} mas a nota é de {nota['periodo']}")
                    elif saida == "":
                        self.logger.log(f"    Empresa {empresa['codigo']} entrou em {entrada} mas a nota é de {nota['periodo']}")
                    else:
                        self.logger.log(f"    Empresa {empresa['codigo']} saiu em {saida} mas a nota é de {nota['periodo']}")

                    self.mongo['conferencia_notas_periodos'].update_one(
                        {'chave': nota['chave']}, {'$set': {'destinatario': None}})
                    nota['destinatario'] = None

            if nota['emitente'] == None and nota['destinatario'] == None:
                self.logger.log(f"    Não sobrou emitente nem destinatário válido na nota. Excluindo a nota.")
                self.mongo['conferencia_notas_periodos'].delete_one({'chave': nota['chave']})

    def testar(self):
        pass


def clearStringParam(param):
    if (param[0] == "'" and param[-1:] == "'") or (param[0] == "'" and param[-1:] == "'"):
        param = param[1:-1]
    return param


def printHelp():
    logger.log("╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗")
    logger.log("║ Processa o arquivo ECD indicado. Se o caminho for um diretório, classifica e processa todos os arquivos encontrados.                    ║")
    logger.log("║    Se for marcado apenas para mapear, apenas lê, classifica os dados e mostra-os na tela para conferência com o                         ║")
    logger.log("║    valor total.                                                                                                                         ║")
    logger.log("║                                                                                                                                         ║")
    logger.log("║ Parâmetros:                                                                                                                             ║")
    logger.log("║     -empresa    = Informa a empresa da nota a ser processada                                                                            ║")
    logger.log("║     -pack       = Opcional. Ao finalizar o processamento, cria uma cópia do resultado na pasta.                                         ║")
    logger.log("║     -empacotar  = Idem acima.                                                                                                           ║")
    logger.log("║     -foldered   = Gera os resumos para o Iacon nas pastas informadas.                                                                   ║")
    logger.log("║     -filter     = Filtra as pastas processadas. Exemplo -filter=\"Novas%\" processaria apenas as pastas que iniciam com \"Novas\"           ║")
    logger.log("║     -especial   = Aplica apenas a regra de nota existente na domínio.                                                                   ║")
    # logger.log("     -limpar     = Idem acima.")
    # logger.log("     -map        = Opcional. Pede para que o script apenas faça o mapeamento e mostre os valores para conferência")
    # logger.log("     -mapear     = Idem acima.")
    # logger.log("     -consolidar = Opcional. Executar a consolidação de uma empresa e um ano")
    # logger.log("     -faltantes  = Opcional. Executa a consolidação mas agora para todas as empresas e anos faltantes.")
    # logger.log("     -teste      = Opcional. Faz um teste dos arquivos gerados.")
    # logger.log("     <Mês>       = Opcional mas usado com a tag -map, indica o mês a ser mostrado na tela. Se for omitido, mostra todos os")
    # logger.log("                      meses.")
    # logger.log("     <Empresa>   = Opcional mas usado com a tag -consolidar, indica a empresa a ser consolidada.")
    # logger.log("     <Ano>       = Opcional mas usado com a tag -consolidar e -faltantes, indica o ano a ser consolidado")
    # logger.log("")
    logger.log("║ Forma de uso do script:                                                                                                                 ║")
    logger.log("║     python nfe.py <Caminhos dos arquivos>                                                                                               ║")
    logger.log("║                                                                                                                                         ║")
    logger.log("║ Exemplo:                                                                                                                                ║")
    logger.log("║     python nfe.py \"C:\\Spool\\30769960000181-52300040116-20180622-20181231-G-754575E6130FED2963F7DC9FD288BFADCA6AD94C-1-SPED-ECD.xml\"     ║")
    logger.log("║ ou:                                                                                                                                     ║")
    logger.log("║     python nfe.py \"S:\\\"                                                                                                                 ║")
    logger.log("║ ou:                                                                                                                                     ║")
    logger.log("║     python nfe.py \"S:\\\" -foldered                                                                                                       ║")
    logger.log("║     python nfe.py \"S:\\\" -foldered -filter=\"C:\\Iacon XML\\466\"                                                                                                      ║")
    logger.log("║                                                                                                                                         ║")
    logger.log("╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝")

def validarBloco(pasta, filtro):
    try:
        if filtro == "":
            return True
        elif filtro[0] == "%" and filtro[-1] == "%":
            f = filtro[1:-1]
            return f in pasta
        elif filtro[0] == "%":
            f = filtro[1:]
            d = pasta[-(len(f)):]
            r = f == d
            return r
        elif filtro[-1] == "%":
            f = filtro[0:-1]
            d = pasta[0:(len(f))]
            r = f == d
            return r
        else:
            return pasta == filtro
    except Exception as exc:
        logger.log(traceback.print_exc())
        logger.log(exc)
        tools.exit()

def validarPastaAno(blocos, pini, pfim):
    try:
        date_inicial = pini.split('/')
        date_final = pfim.split('/')

        if date_inicial[1] <= blocos[1] <= date_final[1]:
            return True
        else:
            return False

    except Exception as exc:
        logger.log(traceback.print_exc())
        logger.log(exc)
        tools.exit()


def validarPastaPeriodo(blocos, pini, pfim):
    try:
        date_inicial = pini.split('/')
        date_inicial = date_inicial[1] + date_inicial[0]
        date_final = pfim.split('/')
        date_final = date_final[1] + date_final[0]

        date_file = blocos[1] + blocos[2]

        if date_inicial <= date_file <= date_final:
            return True
        else:
            return False

    except Exception as exc:
        logger.log(traceback.print_exc())
        logger.log(exc)
        tools.exit()


def validarPasta(pasta, filtro, pini, pfim):
    try:
        result = False
        if filtro == "" and pini == "":
            return True

        pasta_blocos = pasta.split("\\")
        if pasta != "" and len(pasta_blocos) > 0:
            # for bloco in pasta_blocos:
            result = validarBloco(pasta_blocos[0], filtro)
            if result and pini != "":
                if len(pasta_blocos) >= 3:
                    result = validarPastaPeriodo(pasta_blocos, pini, pfim)

                return result
        return result
    except Exception as exc:
        logger.log(traceback.print_exc())
        logger.log(exc)
        tools.exit()


def processarArquivo(path, logger, pack="", packname="", empresa="", foldered=False, filtro="", historico="", especial=False, _threaded=0, pini="", pfim="", dev=False, fast=False, force=False, zip_path=""):
    if True:
    # try:
        mapper = Processor(historico, logger)
        mapper.fast = fast
        # tools.CheckFolder(mapper.summadir)
        _basepath = path
        if os.path.isfile(path):
            if path.split(".")[-1].lower() == "xml":
                _basepath = os.path.dirname(path)
                # mapper.carregar_canceladas(_basepath)
                mapper.process(path, pack, _basepath, packname, empresa=empresa, foldered=foldered, especial=especial, dev=dev, zip_path=zip_path)
            elif path.split(".")[-1].lower() in ["zip"]:
                f = tempfile.TemporaryDirectory(dir = tempfile.gettempdir())
                with zipfile.ZipFile(path) as zf:
                    for filename in zf.namelist():
                        zf.extract(filename, f.name)
                        if filename.split(".")[-1].lower() == "xml":
                            tools.log('')
                            tools.log(f'{path}')
                            tools.log(f'    -> {os.path.join(f.name, filename)}')
                            mapper.process(os.path.join(f.name, filename), pack, _basepath, packname, empresa=empresa, foldered=foldered, especial=especial, dev=dev, zip_path=zip_path)
                f.cleanup()
            else:
                mapper.log("    Arquivo não é xml")
                sys.exit(3)
        elif os.path.isdir(path):
            mapper.documents = []
            mapper.carregarHistorico()
            # mapper.carregar_canceladas(path)

            if _threaded > 0:
                threaded.ThreadPooled.configure(
                    max_workers=multiprocessing.cpu_count()*_threaded)

            for (dirpath, dirnames, filenames) in os.walk(path):
                if dirpath in mapper.historico and not force:
                    mapper.Log("        Pasta já processada!")
                elif validarPasta(dirpath.replace(path, "")[1:], filtro, pini, pfim):
                    mapper.Log(f"    {dirpath}")
                    for filename in filenames:
                        if filename.split(".")[-1] == "xml":
                            ###################################################################################
                            # Por causa da exigência de se informar sempre a empresa no chamado:
                            # https://app.gitkraken.com/glo/board/W8iKZgtLqg4A5yVW/card/Xii1CxUCwgAQD8cS
                            ###
                            # Agora a empresa deve obrigatoriamente estar na raiz pesquisada
                            ###################################################################################
                            if len(dirpath.replace(path, '').split('\\')) > 1 and len(dirpath.replace(path, '').split('\\')[1].split(' -')) > 1:
                                empresa = dirpath.replace(path, '').split('\\')[1].split('- ')[-1]
                            mapper.Log("")
                            mapper.Log(
                                "---------------------------------------------------------------------------------------------------")
                            mapper.Log(os.path.join(dirpath, filename))
                            mapper.process(os.path.join(dirpath, filename), pack, _basepath, packname, empresa=empresa, foldered=foldered, especial=especial, dev=dev)

                    mapper.historico[dirpath] = 1
                    mapper.salvarHistorico()
            if pack and len(mapper.documents) > 0:
                now = datetime.datetime.now()
                data = {
                    "caminho": path,
                    "data": f"{now.year}{str(now.month).rjust(2,'0')}{str(now.day).rjust(2,'0')}",
                    "hora": f"{str(now.hour).rjust(2,'0')}{str(now.minute).rjust(2,'0')}{str(now.second).rjust(2,'0')}",
                    "empresa": empresa,
                    "nfe": mapper.documents
                }

                self.mongo['conferencia_notas'].insert_one(data)
            # mapper.apagarHistorico()
            mapper.atualizarCanceladas(path)
        else:
            logger.log("Não é arquivo nem pasta")
    # except Exception as exc:
    #     logger.log('')
    #     logger.log(''.join(traceback.format_exception(etype=type(exc), value=exc, tb=exc.__traceback__)))
    #     tools.exit()


if __name__ == "__main__":
    sn = ".".join(__file__.split(".")[0:len(__file__.split(".")) - 1])
    sn = sn.split("\\")[-1]
    logger = tools.dharmaLogger()

    try:
        args = []
        vars = {}
        vars['command'] = sys.argv[0]
        vars['path'] = ""
        vars['process'] = False
        vars['map'] = False
        vars['mes'] = ""
        vars['consolidar'] = False
        vars['faltantes'] = False
        vars['teste'] = False
        vars['pack'] = False
        vars['foldered'] = False
        vars['especial'] = False
        vars['totais'] = False
        vars['canceladas'] = False
        vars['apagar'] = False
        vars['dev'] = False
        vars['fast'] = False
        vars['force'] = False
        vars['threaded'] = 0
        vars['filter'] = ""
        vars['historico'] = ""
        vars['empresa'] = ""
        vars['packname'] = ""
        vars['ano'] = ""
        vars['pini'] = ""
        vars['pfim'] = ""
        vars['zip_path'] = ""
        vars['origin'] = ""
        for arg in sys.argv[1:]:
            parts = arg.split('=')
            if (len(parts) > 1):
                if parts[0] == '-c':
                    vars['path'] = clearStringParam(parts[1])
                if parts[0] == '-process':
                    vars['process'] = True
                if parts[0] == '-map':
                    vars['map'] = True
                if parts[0] == '-empacotar':
                    vars['pack'] = True
                if parts[0] == '-pack':
                    vars['pack'] = True
                    vars['packname'] = clearStringParam(parts[1])
                if parts[0] in ['empresa','-empresa', '--empresa']:
                    vars['empresa'] = clearStringParam(parts[1])
                if parts[0] in ['zip','-zip', '--zip']:
                    vars['zip_path'] = clearStringParam(parts[1])
                if parts[0] == '-threaded':
                    vars['threaded'] = int(clearStringParam(parts[1]))
                elif (parts[0] == "-filter"):
                    vars['filter'] = parts[1].strip()
                elif (parts[0] == "-historico"):
                    vars['historico'] = parts[1].strip()
                if parts[0] == '-m':
                    vars['mes'] = clearStringParam(parts[1])
                if parts[0] == '-pini':
                    vars['pini'] = clearStringParam(parts[1])
                if parts[0] == '-pfim':
                    vars['pfim'] = clearStringParam(parts[1])
                elif parts[0] == 'log' or parts[0] == '-log':
                    logger.name = clearStringParam(parts[1])
                elif parts[0] == 'uuid' or parts[0] == '-uuid':
                    logger.uuid = clearStringParam(parts[1])
                if parts[0] in ['origin','-origin', '--origin']:
                    vars['origin'] = clearStringParam(parts[1])
            else:
                args.append(parts[0])
                if (parts[0] == "-clear"):
                    vars['path'] = parts[0]
                elif (parts[0] == "-limpar"):
                    vars['path'] = parts[0]
                elif (parts[0] == "-map"):
                    vars['map'] = True
                elif (parts[0] == "-pack"):
                    vars['pack'] = True
                elif (parts[0] == "-empacotar"):
                    vars['pack'] = True
                elif (parts[0] == "-teste"):
                    vars['teste'] = True
                elif (parts[0] == "-mapear"):
                    vars['map'] = True
                elif (parts[0] == "-foldered"):
                    vars['foldered'] = True
                elif (parts[0].lower() in ["canceladas", "-canceladas"]):
                    vars['canceladas'] = True
                elif (parts[0].lower() in ["apagar", "-apagar"]):
                    vars['apagar'] = True
                elif (parts[0] in ["-especial", "especial"]):
                    vars['especial'] = True
                elif (parts[0] in ["-dev", "dev"]):
                    vars['dev'] = True
                elif (parts[0] == "-consolidar"):
                    vars['consolidar'] = True
                elif (parts[0] == "-gerar-totais"):
                    vars['totais'] = True
                elif (parts[0] == "-faltantes"):
                    vars['faltantes'] = True
                elif (vars['map'] and vars['mes'] == ""):
                    vars['mes'] = parts[0]
                elif (vars['consolidar'] and vars['empresa'] == ""):
                    vars['empresa'] = parts[0]
                elif ((vars['consolidar'] or vars['faltantes']) and vars['ano'] == ""):
                    vars['ano'] = parts[0]
                elif parts[0] == 'log' or parts[0] == '-log':
                    logger.name = clearStringParam(parts[1])
                elif (parts[0] == "fast"):
                    vars['fast'] = True
                elif (parts[0] == "force") or (parts[0] == "-force") or (parts[0] == "--force"):
                    vars['force'] = True
                elif (vars['path'] == ""):
                    vars['path'] = parts[0]

        if (vars['path'] == "apagar"):
            printHelp()
        elif ((vars['path'] == "") and not vars['consolidar'] and not vars['map'] and not vars['faltantes'] and not vars['canceladas'] and not vars['apagar'] and not vars['teste'] and not vars['totais']):
            if vars['empresa'] == "":
                logger.log("")
                logger.log("  Obrigatório informar a empresa!")
                logger.log("")
            printHelp()
        elif (vars['consolidar'] and (vars['empresa'] == "" or vars['ano'] == "")):
            printHelp()
        elif (vars['path'] == "?"):
            printHelp()
        else:
            mapper = Processor(vars['historico'], logger)
            mapper.fast = vars['fast']
            if vars['map']:
                mapper.Log("Comando mapear")
                mapper.Log("    path = {}".format(vars['path']))
                mapper.Log("    mes  = {}".format(vars['mes']))
                mapper.mapearArquivo(vars['path'], vars['mes'])
            elif vars['canceladas']:
                mapper.Log("Processando XML de Cancelamentos")
                # mapper.gerarTotais(debug=True, rootpath=vars["path"])
                mapper.atualizarCanceladas(vars["path"].replace("^&", "&"))
            elif vars['apagar']:
                mapper.Log("apagar notas fora do periodo da empresa")
                # mapper.gerarTotais(debug=True, rootpath=vars["path"])
                mapper.apagarNotasForaPeriodoEmpresa()
            elif vars['totais']:
                mapper.Log("Gerando Totais")
                # mapper.gerarTotais(debug=True, rootpath=vars["path"])
                mapper.gerarTotais()
            elif vars['consolidar']:
                mapper.Log("Comando consolidar")
                mapper.Log("    empresa = {}".format(vars['empresa']))
                mapper.Log("    ano     = {}".format(vars['ano']))
                mapper.consolidarEmpresa(vars['empresa'], vars['ano'])
            elif (vars['faltantes']):
                mapper.Log("Comando faltantes")
                mapper.consolidarFaltantes(vars['ano'])
            elif (vars['teste']):
                mapper.Log("Comando teste")
                mapper.testar()
            elif (vars['path'] == "-clear"):
                mapper.Log("Comando clear")
                mapper.clearAll()
            else:
                mapper.Log("Comando processar")
                mapper.Log("    path = {}".format(vars['path']))
                processarArquivo(vars["path"].replace("^&", "&"), logger, vars['pack'], vars['packname'], empresa=vars['empresa'], foldered=vars['foldered'], filtro=vars['filter'], historico=vars['historico'], especial=vars['especial'], _threaded=vars['threaded'], pini=vars['pini'], pfim=vars['pfim'], dev=vars['dev'], fast=vars['fast'], force=vars['force'], zip_path=vars['zip_path'])
            mapper.Log("      ,'.-.'. ")
            mapper.Log("      '\~ o/` ,,")
            mapper.Log("       { @ } f")
            mapper.Log("       /`-'\$ ")
            mapper.Log("      (_/-\_)")
            mapper.Log("    Terminou!!!")
            mapper.Log(
                "#########################################################################################")
    except Exception as ex:
        logger.log(traceback.format_exc())
        tools.exit()