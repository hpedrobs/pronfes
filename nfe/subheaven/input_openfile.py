# -*- coding: utf-8 -*-
import os
import sys
from tkinter import Tk
from tkinter.filedialog import askopenfilename

class input_openfile(object):
    selected_path = ""
    directory_path = None
    root = None

    def __ini__(self):
        pass

    def run(self):
        Tk().withdraw()
        filename = askopenfilename()
        return filename