# -*- coding: utf-8 -*-
import os
from tkinter import filedialog
from tkinter import *

class input_directory(object):
    selected_path = ""
    directory_path = None
    root = None

    def __ini__(self):
        pass

    def browse_button(self):
        self.selected_path = filedialog.askdirectory()
        self.directory_path.set(self.selected_path)
        self.root.destroy()

    def run(self):
        Tk().withdraw()
        directory = filedialog.askdirectory()
        return directory

    # def run(self):
        # print(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'iacon.ico'))
        # self.root = Tk()
        # self.root.geometry("200x100")
        # self.root.iconbitmap(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'iacon.ico'))
        # self.root.title("Iacon")
        # self.directory_path = StringVar()
        # lbl1 = Label(master=self.root,textvariable=self.directory_path)
        # lbl1.grid(row=0, column=1)
        # button2 = Button(text="Selecione uma pasta", command=self.browse_button)
        # button2.grid(row=0, column=3)

        # mainloop()
        # return self.selected_path