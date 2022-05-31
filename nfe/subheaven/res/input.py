# -*- coding: utf-8 -*-
import os

def input_default(prompt, default):
    bck = chr(8) * len(default)
    ret = input(prompt + default + bck)
    return ret or default