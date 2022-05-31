# -*- coding: utf-8 -*-
import os
import sys

def limpar_string(text, allowed):
    r = ""
    for k in text:
        if k in allowed:
            r += k
    return r

def apenas_numeros(text):
    return limpar_string(text, '0123456789')

def is_numeric(string):
    try:
        int(string)
        return True
    except ValueError:
        return False

def is_number_or_empty(string):
    return string == "" or is_numeric(string)
    