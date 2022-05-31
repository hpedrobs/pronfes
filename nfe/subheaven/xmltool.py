# -*- coding: utf-8 -*-
import codecs
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
import unicodedata

def xmltodict(param):
    def cleartagname(tag):
        result = tag
        search_results = re.finditer(r'\{.*?\}', tag)
        for item in search_results:
            result = result.replace(item.group(0), "")
        return result

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
                    print("Erro ao lidar com o tipo de atributo do XML. Verificar")
                    sys.exit(9)

            return result

    def validatexml(param):
        valid = True
        valid = valid and param != ""
        valid = valid and "<" in param
        valid = valid and ">" in param
        return valid

    valid_data_initialized = False
    try:
        if len(param) < 240 and os.path.isfile(param) and param.split('.')[-1].lower() == "xml":
            with codecs.open(param, 'rb', 'latin1') as file:
                param = file.read()

        newparam = ""
        debug = 0
        for k in param:
            debug += 1
            if not valid_data_initialized and k == "<":
                valid_data_initialized = True
            if valid_data_initialized and k != "\n" and k != "\t" and ord(k) > 26:
                newparam = f"{newparam}{k}"
        param = newparam
        while " <" in param:
            param = param.replace(" <", "<")
        while "> " in param:
            param = param.replace("> ", ">")
        if validatexml(param):
            root = ET.fromstring(param)
        else:
            print("        XML inválido!")
            root = None

        data = {}
        if root != None:
            data[cleartagname(root.tag)] = nodetodict(root)
        return data
    except Exception as e:
        print(f"    {e}")
        return None

def getFromXML(xml, attr = None, chain = None):
    if attr != None:
        if attr in xml:
            return xml[attr]
        else:
            return None
    elif chain != None:
        base = xml
        for level in chain:
            if base != None and level in base:
                base = base[level]
            else:
                return None
        return base
    else:
        return xml