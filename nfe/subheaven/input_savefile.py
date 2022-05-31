# -*- coding: utf-8 -*-
import os
from tkinter import Tk
from tkinter.filedialog import asksaveasfilename

class input_savefile(object):
    selected_path = ""
    directory_path = None
    root = None

    def __ini__(self):
        pass

    def run(self):
        Tk().withdraw()
        filename = asksaveasfilename()
        return filename