# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)))))

import res.input
import stringtool

def input_text(prompt, default = "", obrigatorio = False):
    result = res.input.input_default(prompt, default).strip()
    if obrigatorio:
        while result == "":
            result = res.input.input_default(prompt, default).strip()
    return result

def input_number(prompt, default = "", obrigatorio = False):
    if default != "":
        default = str(default)
    result = res.input.input_default(prompt, default).strip()
    while not stringtool.is_number_or_empty(result):
        print("Valor tem que ser um número:")
        result = res.input.input_default(prompt, default).strip()
    if obrigatorio:
        ok = False
        while result == "":
            result = res.input.input_default(prompt, default).strip()
    return result