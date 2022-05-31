# -*- coding: utf-8 -*-
import os

def check_folder(path):
    if (not os.path.isdir(path)):
        os.makedirs(path)

def file_exists(filepath):
    return os.path.isfile(filepath)