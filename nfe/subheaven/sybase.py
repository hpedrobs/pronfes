# -*- coding: utf-8 -*-
import codecs
import datetime
import json
import os
import sqlanydb
import sys
import unicodedata

dboptions = None


def select(sql, headers=None):
    def criarConf():
        _opt = {
            "host": "",
            "servidor": "",
            "banco": "",
            "uid": "",
            "pwd": ""
        }
        if sys.version_info[0] < 3:
            _json = json.dumps(_opt, encoding="latin1", indent=4)
        else:
            _json = json.dumps(_opt, ensure_ascii=False, indent=4)
        with codecs.open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sybase.conf"), "w", "utf-8") as temp:
            temp.write(_json)
        return _opt

    if os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sybase.conf")):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sybase.conf")) as file:
            dboptions = json.load(file)
    else:
        dboptions = criarConf()

    if dboptions["host"] != "" or dboptions["banco"] != "" or dboptions["uid"] != "":
        # _connection = sqlanydb.connect(host="SRVERP", uid='EXTERNO', pwd='dominio', eng='srvcontabil', dbn='Contabil' )
        _connection = sqlanydb.connect(
            host=dboptions["host"], uid=dboptions["uid"], pwd=dboptions["pwd"], eng=dboptions["servidor"], dbn=dboptions["banco"])
        _cursor = _connection.cursor()
        _cursor.execute(sql)
        _data = _cursor.fetchall()

        if headers == None:
            headers = []
            for header in _cursor.description:
                headers.append(header[0])

        _cursor.close()
        _connection.close()
        if (headers != None):
            _result = []
            for _d in _data:
                _rd = {}
                for i in range(len(headers)):
                    _rd[headers[i]] = _d[i]
                _result.append(_rd)
            return _result
        else:
            return _data
    else:
        print("")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("Arquivo de conexão com o banco de dados não configurado!")
        print("Por favor, altere o arquivo {}\sybase.conf".format(os.path.resolve(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))))
        print("    com os dados necessários e tente novamente")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("")
        return None


def getHeaders(tabela):
    sql = "SELECT column_name FROM systabcol\n"
    sql += "KEY JOIN systab\n"
    sql += "WHERE table_name = '{}'".format(tabela)
    dataset = select(sql)
    columns = []
    for name in dataset:
        columns.append(name['column_name'])
    return columns

# 1 = Lucro Real
# 2 = Simples Nacional
# 3 = Estimativa
# 4 = Pequeno Porte
# 5 = Lucro Presumido
# 6 = Reg. Esp. Trib.
# 7 = Lucro Arbitrado
# 8 = Imune do IRPJ


def buscarempresa(codigo="", nome="", cnpj="", grupo=""):
    _where = ""
    _coma = "WHERE "
    _grupo = ""
    if codigo != "":
        _where = "{} e.codi_emp LIKE '{}'".format(_coma, codigo)
        _coma = " AND "
    if nome != "":
        _where = "{} e.nome_emp LIKE '{}{}'".format(_coma, nome, "%")
        _coma = " AND "
    if cnpj != "":
        _where = "{} e.cgce_emp LIKE '{}{}'".format(_coma, cnpj, "%")
        _coma = " AND "
    if grupo != "":
        _grupo = "JOIN bethadba.usconfempresas AS ue ON (ue.i_empresa = e.codi_emp AND ue.modulos <> '' AND ue.tipo = 3 AND ue.i_confusuario = {})".format(
            grupo)

    # A empresa 881 não estava aparecendo na malha pois o campo uf_leg_emp é de SP, ele tem que verificar o campo uf apenas
    # O campo uf_leg_emp é o do responsável legal
    _sql = "SELECT e.codi_emp, COALESCE(e.nome_emp, '') AS nome, COALESCE(CAST(e.dina_emp AS VARCHAR(10)), '') AS datasaida, COALESCE(e.cgce_emp, '') AS cnpj, COALESCE(e.iest_emp, '') AS ie, COALESCE(e.imun_emp, '') AS im, COALESCE(CAST((SELECT FIRST vi.rfed_par FROM bethadba.EFPARAMETRO_VIGENCIA AS vi WHERE vi.codi_emp = e.codi_emp ORDER BY vi.codi_emp, vi.vigencia_par DESC) AS VARCHAR(20)), '') AS regime, COALESCE(e.esta_emp, '') AS uf, COALESCE((SELECT nome_municipio FROM bethadba.gemunicipio WHERE codigo_municipio = e.codigo_municipio), '') AS mun, COALESCE(e.email_emp, '') AS email, CAST(e.dcad_emp AS varchar(10)) AS inicio, e.stat_emp AS situacao, (SELECT list(usu.i_usuario) FROM bethadba.usconfusuario AS usu JOIN bethadba.usconfempresas AS ue ON (ue.tipo = usu.tipo AND ue.i_confusuario = usu.i_confusuario AND ue.modulos <> '') WHERE usu.i_confusuario in (8,9,66) AND usu.tipo = 3 AND ue.modulos <> '' AND ue.i_empresa = e.codi_emp) AS grupo, (SELECT list(i_confusuario) FROM bethadba.usconfempresas AS ue WHERE ue.modulos <> '' AND ue.tipo = 3 AND ue.i_confusuario IN (8,9,66) AND ue.i_empresa = e.codi_emp) AS grupo_cod, COALESCE(e.i_cnae20, '') AS cnae_cod, cnae = COALESCE((SELECT cnae.descricao FROM bethadba.gecnae20 AS cnae WHERE cnae.i_cnae20 = e.i_cnae20), ''), COALESCE(e.obs_geral, '') AS obs FROM bethadba.geempre AS e {} {} ORDER BY 2".format(_grupo, _where)
    _headers = ["codigo", "nome", "datasaida", "cnpj", "ie", "im", "regime", "uf", "mun",
                "email", "inicio", "situacao", "grupo", "grupo_cod", "cnae_cod", "cnae", "obs"]

    _datalist = select(_sql, _headers)
    if (_datalist != None and len(_datalist) > 0):
        # Podem existir mais de um cadastro de empresa, nesse caso, pegar a mais nova ativa.
        if len(_datalist) > 1:
            for data in _datalist[::-1]:
                if data["situacao"] == "A":
                    return data
        return _datalist[-1]
    else:
        return None


def buscarempresas():
    _where = ""
    _grupo = ""

    _simplesp = "LEFT JOIN bethadba.EFPARAMETRO_VIGENCIA AS p ON (p.codi_emp = e.codi_emp AND p.VIGENCIA_PAR = (SELECT MAX( param2.VIGENCIA_PAR ) FROM bethadba.EFPARAMETRO_VIGENCIA param2 WHERE param2.codi_emp = p.codi_emp))"

    # A empresa 881 não estava aparecendo na malha pois o campo uf_leg_emp é de SP, ele tem que verificar o campo uf apenas
    # O campo uf_leg_emp é o do responsável legal
    _sql = "SELECT e.codi_emp, COALESCE(e.nome_emp, '') AS nome, COALESCE(p.SIMPLESN_ICMS_NORMAL_PAR, '') AS simples, COALESCE(CAST(e.dina_emp AS VARCHAR(10)), '') AS datasaida, COALESCE(e.cgce_emp, '') AS cnpj, COALESCE(e.iest_emp, '') AS ie, COALESCE(e.imun_emp, '') AS im, COALESCE(CAST((SELECT FIRST vi.rfed_par FROM bethadba.EFPARAMETRO_VIGENCIA AS vi WHERE vi.codi_emp = e.codi_emp ORDER BY vi.codi_emp, vi.vigencia_par DESC) AS VARCHAR(20)), '') AS regime, COALESCE(e.esta_emp, '') AS uf, COALESCE((SELECT nome_municipio FROM bethadba.gemunicipio WHERE codigo_municipio = e.codigo_municipio), '') AS mun, COALESCE(e.email_emp, '') AS email, CAST(e.dcad_emp AS varchar(10)) AS inicio, e.stat_emp AS situacao, (SELECT list(usu.i_usuario) FROM bethadba.usconfusuario AS usu JOIN bethadba.usconfempresas AS ue ON (ue.tipo = usu.tipo AND ue.i_confusuario = usu.i_confusuario AND ue.modulos <> '') WHERE usu.i_confusuario in (8,9,66) AND usu.tipo = 3 AND ue.modulos <> '' AND ue.i_empresa = e.codi_emp) AS grupo, (SELECT list(i_confusuario)FROM bethadba.usconfempresas AS ue WHERE ue.modulos <> '' AND ue.tipo = 3 AND ue.i_confusuario IN (8,9,66) AND ue.i_empresa = e.codi_emp) AS grupo_cod, COALESCE(e.i_cnae20, '') AS cnae_cod, cnae = COALESCE((SELECT cnae.descricao FROM bethadba.gecnae20 AS cnae WHERE cnae.i_cnae20 = e.i_cnae20), ''), COALESCE(e.obs_geral, '') AS obs FROM bethadba.geempre AS e {} {} {} ORDER BY 1, 3, 4".format(_simplesp, _grupo, _where)

    _headers = ["codigo", "nome", "simples", "datasaida", "cnpj", "ie", "im", "regime", "uf",
                "mun", "email", "inicio", "situacao", "grupo", "grupo_cod", "cnae_cod", "cnae", "obs"]

    _datalist = select(_sql, _headers)
    return _datalist


def buscarempresasativas():
    _where = "WHERE situacao = 'A'"
    _grupo = ""

    _simplesp = "LEFT JOIN bethadba.EFPARAMETRO_VIGENCIA AS p ON (p.codi_emp = e.codi_emp AND p.VIGENCIA_PAR = (SELECT MAX( param2.VIGENCIA_PAR ) FROM bethadba.EFPARAMETRO_VIGENCIA param2 WHERE param2.codi_emp = p.codi_emp))"

    # A empresa 881 não estava aparecendo na malha pois o campo uf_leg_emp é de SP, ele tem que verificar o campo uf apenas
    # O campo uf_leg_emp é o do responsável legal
    _sql = "SELECT e.codi_emp, COALESCE(e.nome_emp, '') AS nome, COALESCE(p.SIMPLESN_ICMS_NORMAL_PAR, '') AS simples, COALESCE(CAST(e.dina_emp AS VARCHAR(10)), '') AS datasaida, COALESCE(e.cgce_emp, '') AS cnpj, COALESCE(e.iest_emp, '') AS ie, COALESCE(e.imun_emp, '') AS im, COALESCE(CAST((SELECT FIRST vi.rfed_par FROM bethadba.EFPARAMETRO_VIGENCIA AS vi WHERE vi.codi_emp = e.codi_emp ORDER BY vi.codi_emp, vi.vigencia_par DESC) AS VARCHAR(20)), '') AS regime, COALESCE(e.esta_emp, '') AS uf, COALESCE((SELECT nome_municipio FROM bethadba.gemunicipio WHERE codigo_municipio = e.codigo_municipio), '') AS mun, COALESCE(e.email_emp, '') AS email, CAST(e.dcad_emp AS varchar(10)) AS inicio, e.stat_emp AS situacao, (SELECT list(usu.i_usuario) FROM bethadba.usconfusuario AS usu JOIN bethadba.usconfempresas AS ue ON (ue.tipo = usu.tipo AND ue.i_confusuario = usu.i_confusuario AND ue.modulos <> '') WHERE usu.i_confusuario in (8,9,66) AND usu.tipo = 3 AND ue.modulos <> '' AND ue.i_empresa = e.codi_emp) AS grupo, (SELECT list(i_confusuario)FROM bethadba.usconfempresas AS ue WHERE ue.modulos <> '' AND ue.tipo = 3 AND ue.i_confusuario IN (8,9,66) AND ue.i_empresa = e.codi_emp) AS grupo_cod, COALESCE(e.i_cnae20, '') AS cnae_cod, cnae = COALESCE((SELECT cnae.descricao FROM bethadba.gecnae20 AS cnae WHERE cnae.i_cnae20 = e.i_cnae20), ''), COALESCE(e.obs_geral, '') AS obs FROM bethadba.geempre AS e {} {} {} ORDER BY 1, 3, 4".format(_simplesp, _grupo, _where)

    _headers = ["codigo", "nome", "simples", "datasaida", "cnpj", "ie", "im", "regime_cod", "uf",
                "mun", "email", "inicio", "situacao", "grupo", "grupo_cod", "cnae_cod", "cnae", "obs"]

    _datalist = select(_sql, _headers)
    return _datalist


def consultarEntradaBase(chave, empresa):
    headers = getHeaders("efentradas")
    sql = "SELECT nfe.{}, nfe.situacao_ent AS situacao, acuvig.SIMPLESN_CREDITO_PRESUMIDO_ACU AS tem_simples\n".format(
        ", nfe.".join(headers))
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
    sql += "GROUP BY nfe.{}, acuvig.SIMPLESN_CREDITO_PRESUMIDO_ACU\n".format(
        ", nfe.".join(headers))
    headers.append("situacao")
    headers.append("tem_simples")
    dataset = select(sql, headers)

    return dataset


def consultarAliquotasEntrada(empresa, codigo):
    headers = getHeaders("efimpent")
    sql = "SELECT imp.{}\n".format(", imp.".join(headers))
    sql += "FROM bethadba.efentradas as nfe , bethadba.efimpent AS imp\n"
    sql += "WHERE nfe.codi_emp = imp.codi_emp\n"
    sql += "  AND nfe.codi_ent = imp.codi_ent\n"
    sql += "  AND imp.codi_imp = 1\n"
    sql += "  AND nfe.codi_emp = {}\n".format(empresa)
    sql += "  AND nfe.codi_ent = {}\n".format(codigo)
    dataset = select(sql, headers)

    return dataset


def consultarProdutosEntrada(empresa, codigo):
    headers = getHeaders("efmvepro")
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
    dataset = select(sql, headers)

    return dataset


def consultarSimplesEntrada(chave, empresa):
    headers = getHeaders("EFMVEPRO_ICMS_SIMPLES_NACIONAL")
    sql = "SELECT simp.{}\n".format(", simp.".join(headers))
    sql += "FROM bethadba.efentradas as nfe , bethadba.EFMVEPRO_ICMS_SIMPLES_NACIONAL AS simp\n"
    sql += "WHERE nfe.codi_emp = simp.codi_emp\n"
    sql += "  AND nfe.codi_ent = simp.codi_ent\n"
    if empresa != 0 and empresa != "":
        sql += "  AND nfe.codi_emp = '{}'".format(empresa)
    sql += "  AND nfe.chave_nfe_ent = '{}'\n".format(chave)
    dataset = select(sql, headers)

    return dataset


def consultarSaidaBase(chave, empresa):
    headers = getHeaders("efsaidas")
    sql = "SELECT nfe.{}, nfe.situacao_sai as situacao, 'N' AS tem_simples\n".format(
        ", nfe.".join(headers))
    sql += "FROM bethadba.efsaidas as nfe\n"
    sql += "WHERE nfe.chave_nfe_sai = '{}'".format(chave)
    if empresa != 0 and empresa != "":
        sql += "  AND nfe.codi_emp = '{}'".format(empresa)
    sql += "GROUP BY nfe.{}\n".format(", nfe.".join(headers))

    headers.append("situacao")
    headers.append("tem_simples")
    dataset = select(sql, headers)

    return dataset


def consultarAliquotasSaida(empresa, codigo):
    headers = getHeaders("efimpsai")
    sql = "SELECT imp.{}\n".format(", imp.".join(headers))
    sql += "FROM bethadba.efsaidas as nfe , bethadba.efimpsai AS imp\n"
    sql += "WHERE nfe.codi_emp = imp.codi_emp\n"
    sql += "  AND nfe.codi_sai = imp.codi_sai\n"
    sql += "  AND imp.codi_imp = 1\n"
    sql += "  AND nfe.codi_emp = {}\n".format(empresa)
    sql += "  AND nfe.codi_sai = {}\n".format(codigo)
    dataset = select(sql, headers)

    return dataset


def consultarProdutosSaida(empresa, codigo):
    headers = getHeaders("efmvspro")
    sql = "SELECT prod.{}\n".format(", prod.".join(headers))
    sql += "FROM bethadba.efsaidas as nfe, bethadba.efmvspro AS prod\n"
    sql += "WHERE nfe.codi_emp = prod.codi_emp\n"
    sql += "  AND nfe.codi_sai = prod.codi_sai\n"
    sql += "  AND nfe.codi_emp = {}\n".format(empresa)
    sql += "  AND nfe.codi_sai = {}\n".format(codigo)
    dataset = select(sql, headers)

    return dataset


def consultarSimplesSaida(empresa, codigo):
    headers = getHeaders("EFMVSPRO_SIMPLES_NACIONAL_MONOFASICO")
    sql = "SELECT simp.{}\n".format(", simp.".join(headers))
    sql += "FROM bethadba.efsaidas as nfe, bethadba.EFMVSPRO_SIMPLES_NACIONAL_MONOFASICO AS simp\n"
    sql += "WHERE nfe.codi_emp = simp.codi_emp\n"
    sql += "  AND nfe.codi_sai = simp.codi_sai\n"
    sql += "  AND nfe.codi_emp = {}\n".format(empresa)
    sql += "  AND nfe.codi_sai = {}\n".format(codigo)
    dataset = select(sql, headers)

    return dataset


def buscarnotadominio(chave, empresa, short=False):
    nfe = consultarEntradaBase(chave, empresa)
    if len(nfe) > 0:
        for segmento in nfe:
            segmento["tipo"] = "entrada"
            segmento["aliquotas"] = consultarAliquotasEntrada(
                segmento["codi_emp"], segmento["codi_ent"])
            segmento["produtos"] = consultarProdutosEntrada(
                segmento["codi_emp"], segmento["codi_ent"])
            segmento["simples"] = consultarSimplesEntrada(
                segmento["chave_nfe_ent"], empresa)
    else:
        nfe = consultarSaidaBase(chave, empresa)
        if len(nfe) > 0:
            for segmento in nfe:
                segmento["tipo"] = "saida"
                segmento["aliquotas"] = consultarAliquotasSaida(
                    segmento["codi_emp"], segmento["codi_sai"])
                segmento["produtos"] = consultarProdutosSaida(
                    segmento["codi_emp"], segmento["codi_sai"])
                segmento["simples"] = consultarSimplesSaida(
                    segmento["codi_emp"], segmento["codi_sai"])

    if short:
        novo = []
        for segmento in nfe:
            novo_seg = {
                "empresa": segmento["codi_emp"],
                "numero": segmento["nume_sai"] if "nume_sai" in segmento else segmento["nume_ent"],
                "serie": segmento["seri_sai"] if "seri_sai" in segmento else segmento["seri_ent"],
                "data": segmento["dsai_sai"] if "dsai_sai" in segmento else segmento["dent_ent"],
                "codi_nat": segmento["codi_nat"],
                "valor_contabil": segmento["vcon_sai"] if "vcon_sai" in segmento else segmento["vcon_ent"],
                "valor_produtos": segmento["vprod_sai"] if "vprod_sai" in segmento else segmento["vprod_ent"],
                "tipo": segmento["tipo"],
                "situacao": segmento["situacao_sai"] if "situacao_sai" in segmento else segmento["situacao_ent"],
                "aliquotas": [],
                "produtos": [],
            }

            for aliq in segmento['aliquotas']:
                novo_seg['aliquotas'].append({
                    "sequencial": aliq["sequ_isa"] if "sequ_isa" in aliq else aliq["sequ_ien"],
                    "valor_contabil": aliq["vcon_isa"] if "vcon_isa" in aliq else aliq["vcon_ien"],
                    "bc": aliq["bcal_isa"] if "bcal_isa" in aliq else aliq["bcal_ien"],
                    "aliquota": aliq["aliq_isa"] if "aliq_isa" in aliq else aliq["aliq_ien"],
                    "valor": aliq["vlor_isa"] if "vlor_isa" in aliq else aliq["vlor_ien"],
                })

            for prod in segmento['produtos']:
                novo_seg['produtos'].append({
                    "codigo": (prod["codi_pdi"] if "codi_pdi" in prod else prod["codi_pdi"]).strip(),
                    "cfop": prod["cfop_msp"] if "cfop_msp" in prod else prod["cfop_mep"],
                    "cst": prod["cst_msp"] if "cst_msp" in prod else prod["cst_mep"],
                    "numero": prod["nume_msp"] if "nume_msp" in prod else prod["nume_mep"],
                    "quantidade": prod["qtde_msp"] if "qtde_msp" in prod else prod["qtde_mep"],
                    "valor": prod["vpro_msp"] if "vpro_msp" in prod else prod["vpro_mep"],
                    "desconto": prod["vdes_msp"] if "vdes_msp" in prod else prod["vdes_mep"],
                    "valor_icms": prod["valor_icms_msp"] if "valor_icms_msp" in prod else prod["valor_icms_mep"],
                })

            novo.append(novo_seg)
        nfe = novo
    return nfe


def buscarnotadominio_deprecated(chave, empresa=""):
    chnfe = chave.lower().replace("nfe", "")
    sql = ""
    sql += "SELECT nfs.codi_emp AS codi_emp, nfs.nume_sai AS numero, nfs.seri_sai AS serie, SUM(nfs.vprod_sai) AS vprod, SUM(nfs.vcon_sai) AS contabil, SUM(nfs.ipi_sai) AS ipi,\n"
    sql += "      nfs.situacao_sai AS situacao, COALESCE(SUM(imp.vlor_isa),0) AS valor_imposto\n"
    sql += "FROM bethadba.efsaidas nfs\n"
    sql += "     LEFT OUTER JOIN bethadba.efimpsai AS imp\n"
    sql += "               ON    imp.codi_emp = nfs.codi_emp\n"
    sql += "                 AND imp.codi_sai = nfs.codi_sai\n"
    sql += "WHERE ( imp.codi_imp = 1 OR imp.codi_imp IS NULL )\n"
    sql += "  AND nfs.chave_nfe_sai LIKE '{}%'\n".format(chave)
    if empresa != "":
        sql += "  AND nfs.codi_emp = {}\n".format(empresa)
    sql += "GROUP BY codi_emp, numero, serie, situacao\n"
    sql += "UNION\n"
    sql += "SELECT nfs.codi_emp AS codi_emp, nfs.nume_ent AS numero, nfs.seri_ent AS serie, SUM(nfs.vprod_ent) AS vprod, SUM(nfs.vcon_ent) AS contabil, SUM(nfs.ipi_ent) AS ipi,\n"
    sql += "      nfs.situacao_ent AS situacao, COALESCE(SUM(imp.vlor_ien),0) AS valor_imposto\n"
    sql += "FROM bethadba.efentradas nfs\n"
    sql += "     LEFT OUTER JOIN bethadba.efimpent AS imp\n"
    sql += "               ON    imp.codi_emp = nfs.codi_emp\n"
    sql += "                 AND imp.codi_ent = nfs.codi_ent\n"
    sql += "WHERE ( imp.codi_imp = 1 OR imp.codi_imp IS NULL )\n"
    sql += "  AND nfs.chave_nfe_ent LIKE '{}%'\n".format(chave)
    if empresa != "":
        sql += "  AND nfs.codi_emp = {}\n".format(empresa)
    sql += "GROUP BY codi_emp, numero, serie, situacao"

    consulta = select(sql, ["empresa", "numero", "serie", "valor_produtos",
                            "valor_contabil", "valor_ipi", "situacao", "imposto"])
    if consulta != None and len(consulta) > 0:
        return consulta[0]
    else:
        return None


def buscarnotamalha(chave):
    if chave.strip() != "":
        _sql = "SELECT nfs.codi_emp AS codi_emp, nfs.nume_sai AS numero, nfs.seri_sai AS serie, SUM(nfs.vprod_sai) AS vprod,\n"
        _sql += "      nfs.situacao_sai AS situacao, COALESCE(SUM(imp.vlor_isa),0) AS valor_imposto\n"
        _sql += "FROM bethadba.efsaidas nfs\n"
        _sql += "     LEFT OUTER JOIN bethadba.efimpsai AS imp\n"
        _sql += "               ON    imp.codi_emp = nfs.codi_emp\n"
        _sql += "                 AND imp.codi_sai = nfs.codi_sai\n"
        _sql += "WHERE ( imp.codi_imp = 1 OR imp.codi_imp IS NULL )\n"
        _sql += "  AND nfs.chave_nfe_sai LIKE '{}{}'\n".format(chave, "%")
        _sql += "GROUP BY codi_emp, numero, serie, situacao"
        _sql += "\nUNION\n"
        _sql += "SELECT nfs.codi_emp AS codi_emp, nfs.nume_ent AS numero, nfs.seri_ent AS serie, SUM(nfs.vprod_ent) AS vprod,\n"
        _sql += "      nfs.situacao_ent AS situacao, COALESCE(SUM(imp.vlor_ien),0) AS valor_imposto\n"
        _sql += "FROM bethadba.efentradas nfs\n"
        _sql += "     LEFT OUTER JOIN bethadba.efimpent AS imp\n"
        _sql += "               ON    imp.codi_emp = nfs.codi_emp\n"
        _sql += "                 AND imp.codi_ent = nfs.codi_ent\n"
        _sql += "WHERE ( imp.codi_imp = 1 OR imp.codi_imp IS NULL )\n"
        _sql += "  AND nfs.chave_nfe_ent LIKE '{}{}'\n".format(chave, "%")
        _sql += "GROUP BY codi_emp, numero, serie, situacao"
        _headers = ["empresa", "numero", "serie",
                    "valor", "situacao", "valor_imposto"]

        _datalist = select(_sql, _headers)
        if (len(_datalist) > 0):
            return _datalist[0]
        else:
            return None
    else:
        return None


def optsimples(empresa, mes_inicial=1, ano_inicial=2017, mes_final=0, ano_final=0):
    _m = mes_inicial
    _a = ano_inicial
    _vigencias = {}
    _now = datetime.datetime.now()
    if _m == 0:
        _m = _now.month
    if _a == 0:
        _a = _now.year - 2
    if mes_final == 0:
        mes_final = _now.month
    if ano_final == 0:
        ano_final = _now.year
    while _a < ano_final or (_a == ano_final and _m <= mes_final):
        _sql = ""
        _sql += "SELECT vigencia = '{}-{}-01',\n".format(
            _a, str(_m).rjust(2, "0"))
        _sql += "       opt_simples = (SELECT EFPARAMETRO_VIGENCIA.SIMPLESN_OPTANTE_PAR\n"
        _sql += "                     FROM bethadba.EFPARAMETRO_VIGENCIA\n"
        _sql += "                    WHERE bethadba.EFPARAMETRO_VIGENCIA.CODI_EMP = emp.codi_emp\n"
        _sql += "                      AND bethadba.EFPARAMETRO_VIGENCIA.VIGENCIA_PAR = (SELECT max(P2.VIGENCIA_PAR )\n"
        _sql += "                                                                          FROM bethadba.EFPARAMETRO_VIGENCIA AS P2\n"
        _sql += "                                                                         WHERE P2.codi_emp = emp.codi_emp\n"
        _sql += "                                                                           AND P2.VIGENCIA_PAR <= vigencia ))\n"
        _sql += "  FROM bethadba.geempre AS emp\n"
        _sql += " WHERE emp.stat_emp = 'A' /* empresas ativas */\n"
        _sql += "   AND ( ( emp.tins_emp = 1 AND SUBSTR(emp.cgce_emp,9,4) = '0001' ) /* só empresas matrizes - inscrição CNPJ */\n"
        _sql += "         OR emp.tins_emp <> 1 /* inscrições que não são CNPJ também buscar, pois não tem filial. */ )\n"
        _sql += "   AND opt_simples = 'S' /* apenas optante do simples nacional */\n"
        _sql += "   AND emp.codi_emp = {}\n".format(empresa)
        _sql += "ORDER BY emp.codi_emp\n"
        _r = select(_sql, ["vigencia", "simples"])

        if len(_r) > 0:
            _r = _r[0]
            _vigencias[_r["vigencia"].replace(
                "-01", "")] = _r["simples"] == "S"
        else:
            _vigencias["{}-{}".format(_a, str(_m).rjust(2, "0"))] = False

        _m += 1
        while _m > 12:
            _m -= 12
            _a += 1

    return _vigencias


def consultarvigencias(empresa, mes_inicial=1, ano_inicial=0, mes_final=0, ano_final=0):
    _now = datetime.datetime.now()
    if mes_inicial == 0:
        mes_inicial = _now.month
    if ano_inicial == 0:
        ano_inicial = _now.year - 2

    if (ano_inicial < 2010):
        print("Ano inicial inválido:")
        print("    {}".format(ano_inicial))
        return []
    else:
        _sql = "SELECT vigencia_par, rfed_par FROM bethadba.EFPARAMETRO_VIGENCIA WHERE codi_emp = {} ORDER BY vigencia_par".format(
            empresa)
        _headers = ["vig", "regime"]
        _datalist = select(_sql, _headers)
        return _datalist


def buscarvigencia(empresa, mes, ano):
    _sql = ""
    _sql += "SELECT TOP 1 vigencia_par, rfed_par, simplesn_optante_par\n"
    _sql += "FROM bethadba.EFPARAMETRO_VIGENCIA\n"
    _sql += f"WHERE codi_emp = {empresa}\n"
    _sql += f"AND vigencia_par <= '{ano}-{str(mes).rjust(2, '0')}-01'\n"
    _sql += "ORDER BY vigencia_par DESC\n"
    _headers = ["vig", "regime", "optante"]
    _datalist = select(_sql, _headers)
    if len(_datalist) > 0:
        return _datalist[0]
    else:
        return None


def consultarempresasvigenciasbase(empresa="", mes_inicial=1, ano_inicial=0, mes_final=0, ano_final=0):
    _where = ""
    _grupo = ""
    now = datetime.datetime.now()
    if ano_inicial == 0:
        ano_inicial = now.year - 1
    if ano_final == 0:
        ano_final = now.year
    if mes_final == 0:
        mes_final = now.month
    _em = mes_final
    _ey = ano_final

    if empresa != "":
        _where = "WHERE e.codi_emp = {}\n".format(empresa)

    # _simplesp = "LEFT JOIN bethadba.EFPARAMETRO_VIGENCIA AS p ON (p.codi_emp = e.codi_emp AND p.VIGENCIA_PAR = (SELECT MAX( param2.VIGENCIA_PAR ) FROM bethadba.EFPARAMETRO_VIGENCIA param2 WHERE param2.codi_emp = p.codi_emp))"

    # A empresa 881 não estava aparecendo na malha pois o campo uf_leg_emp é de SP, ele tem que verificar o campo uf apenas
    # O campo uf_leg_emp é o do responsável legal
    # _sql = "SELECT e.codi_emp, COALESCE(e.nome_emp, '') AS nome, COALESCE(p.SIMPLESN_ICMS_NORMAL_PAR, '') AS simples, COALESCE(e.cgce_emp, '') AS cnpj, COALESCE(e.iest_emp, '') AS ie, COALESCE(e.imun_emp, '') AS im, COALESCE(CAST((SELECT FIRST vi.rfed_par FROM bethadba.EFPARAMETRO_VIGENCIA AS vi WHERE vi.codi_emp = e.codi_emp ORDER BY vi.codi_emp, vi.vigencia_par DESC) AS VARCHAR(20)), '') AS regime, COALESCE(e.esta_emp, '') AS uf, COALESCE((SELECT nome_municipio FROM bethadba.gemunicipio WHERE codigo_municipio = e.codigo_municipio), '') AS mun, COALESCE(e.email_emp, '') AS email, CAST(e.dcad_emp AS varchar(10)) AS inicio, e.stat_emp AS situacao, (SELECT list(usu.i_usuario) FROM bethadba.usconfusuario AS usu JOIN bethadba.usconfempresas AS ue ON (ue.tipo = usu.tipo AND ue.i_confusuario = usu.i_confusuario AND ue.modulos <> '') WHERE usu.i_confusuario in (8,9,66) AND usu.tipo = 3 AND ue.modulos <> '' AND ue.i_empresa = e.codi_emp) AS grupo, (SELECT list(i_confusuario)FROM bethadba.usconfempresas AS ue WHERE ue.modulos <> '' AND ue.tipo = 3 AND ue.i_confusuario IN (8,9,66) AND ue.i_empresa = e.codi_emp) AS grupo_cod, COALESCE(e.i_cnae20, '') AS cnae_cod, cnae = COALESCE((SELECT cnae.descricao FROM bethadba.gecnae20 AS cnae WHERE cnae.i_cnae20 = e.i_cnae20), ''), COALESCE(e.obs_geral, '') AS obs FROM bethadba.geempre AS e {} {} {} ORDER BY 1, 2, 3, 4".format(_simplesp, _grupo, _where)
    _sql = ""
    _sql += "SELECT e.codi_emp, \n"
    _sql += "       COALESCE(e.nome_emp, '') AS nome, \n"
    _sql += "       COALESCE(p.SIMPLESN_ICMS_NORMAL_PAR, '') AS simples, \n"
    _sql += "       COALESCE(e.cgce_emp, '') AS cnpj, \n"
    _sql += "       COALESCE(e.iest_emp, '') AS ie, \n"
    _sql += "       COALESCE(e.imun_emp, '') AS im, \n"
    _sql += "       COALESCE(CAST((\n"
    _sql += "             SELECT FIRST vi.rfed_par \n"
    _sql += "             FROM bethadba.EFPARAMETRO_VIGENCIA AS vi \n"
    _sql += "             WHERE vi.codi_emp = e.codi_emp \n"
    _sql += "             ORDER BY vi.codi_emp, vi.vigencia_par DESC) AS VARCHAR(20)), '') AS regime, \n"
    _sql += "       COALESCE(e.esta_emp, '') AS uf, \n"
    _sql += "       COALESCE((\n"
    _sql += "             SELECT nome_municipio \n"
    _sql += "             FROM bethadba.gemunicipio \n"
    _sql += "             WHERE codigo_municipio = e.codigo_municipio), '') AS mun, \n"
    _sql += "       COALESCE(e.email_emp, '') AS email, \n"
    _sql += "       CAST(e.dcad_emp AS varchar(10)) AS inicio, \n"
    _sql += "       CAST(e.dina_emp AS varchar(10)) AS saida, \n"
    _sql += "       e.stat_emp AS situacao, \n"
    _sql += "       (SELECT list(usu.i_usuario) \n"
    _sql += "        FROM bethadba.usconfusuario AS usu \n"
    _sql += "        JOIN bethadba.usconfempresas AS ue ON (ue.tipo = usu.tipo AND ue.i_confusuario = usu.i_confusuario AND ue.modulos <> '') \n"
    _sql += "        WHERE usu.i_confusuario in (8,9,66) \n"
    _sql += "          AND usu.tipo = 3 \n"
    _sql += "          AND ue.modulos <> '' \n"
    _sql += "          AND ue.i_empresa = e.codi_emp) AS grupo, \n"
    _sql += "       (SELECT list(i_confusuario)\n"
    _sql += "        FROM bethadba.usconfempresas AS ue \n"
    _sql += "        WHERE ue.modulos <> '' \n"
    _sql += "          AND ue.tipo = 3 \n"
    _sql += "          AND ue.i_confusuario IN (8,9,66) AND ue.i_empresa = e.codi_emp) AS grupo_cod, \n"
    _sql += "       COALESCE(e.i_cnae20, '') AS cnae_cod, \n"
    _sql += "       cnae = COALESCE((\n"
    _sql += "                    SELECT cnae.descricao \n"
    _sql += "                    FROM bethadba.gecnae20 AS cnae \n"
    _sql += "                    WHERE cnae.i_cnae20 = e.i_cnae20), ''), \n"
    _sql += "       COALESCE(e.fone_leg_emp, '') AS fone, \n"
    _sql += "       COALESCE(e.obs_geral, '') AS obs \n"
    _sql += "FROM bethadba.geempre AS e \n"
    _sql += "LEFT JOIN bethadba.EFPARAMETRO_VIGENCIA AS p ON (p.codi_emp = e.codi_emp AND p.VIGENCIA_PAR = (\n"
    _sql += "       SELECT MAX( param2.VIGENCIA_PAR ) \n"
    _sql += "       FROM bethadba.EFPARAMETRO_VIGENCIA param2 \n"
    _sql += "       WHERE param2.codi_emp = p.codi_emp))   \n"
    #_sql += "WHERE situacao = 'A'\n"
    _sql += "{}".format(_where)
    _sql += "ORDER BY 1, 2, 3, 4\n"

    _headers = ["codigo", "nome", "simples", "cnpj", "ie", "im", "regime_cod", "uf", "mun", "email",
                "inicio", "saida", "situacao", "grupo", "grupo_cod", "cnae_cod", "cnae", "fone", "obs"]

    _datalist = select(_sql, _headers)
    for _d in range(len(_datalist)):
        _datalist[_d]["vigencia"] = {}
        _vigencias = consultarvigencias(
            _datalist[_d]["codigo"], mes_inicial, ano_inicial, mes_final, ano_final)
        #_optsimples = optsimples(_datalist[_d]["codigo"], mes_inicial, ano_inicial, mes_final, ano_final)
        #_datalist[_d]["simples"] = _optsimples
        _am = mes_inicial
        _ay = ano_inicial
        while (_ay < _ey or (_ay == _ey and _am <= _em)):
            _p = "{}-{}-01".format(_ay, "{:02}".format(_am))
            if (_vigencias != None and len(_vigencias) > 0):
                _regime = _vigencias[0]["regime"]
                for _vigencia in _vigencias:
                    if (_p <= _vigencia["vig"]):
                        _regime = _vigencia["regime"]
                    else:
                        break
                _p = "{}{}".format(_ay, "{:02}".format(_am))
                _datalist[_d]["vigencia"][_p] = _regime
            else:
                _p = "{}{}".format(_ay, "{:02}".format(_am))
                _datalist[_d]["vigencia"][_p] = ""
            _am += 1
            if (_am > 12):
                _am = 1
                _ay += 1
    return _datalist
