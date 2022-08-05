import argparse
import cmd
import contextlib
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Tuple

import pysqlar
import prompt_toolkit as ptk
from fs import open_fs
from prompt_toolkit import PromptSession


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str)
    return parser.parse_args()


class PyDatabaseEngine(contextlib.ContextDecorator):
    def __init__(self, filename):
        super().__init__()
        self.conn = None
        self.file_archive = None
        self.filename = filename
        self.temp_dir = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.filename)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.file_archive = pysqlar.SQLiteArchive(self.filename)
        self.file_archive.extractall(self.temp_dir.name)
        print(os.listdir(self.temp_dir.name))

    def __exit__(self, exc_type, exc, exc_tb):
        self.conn.close()
        self.file_archive.close()
        self.temp_dir.cleanup()

    def open_database(self, filename):
        temp_dir: tempfile.TemporaryDirectory
        with tempfile.TemporaryDirectory() as temp_dir:
            self.file_archive.extractall(temp_dir)
            print(os.listdir)

    def exec_file(self, filename):
        with fs.open('hello.py') as f:
            code = f.read()
            mod = compile(code, f.name,  'exec')
            _globals = _locals  = dict()
            exec(mod, _globals, _locals)
             

if __name__ == '__main__':
    args = get_args()
    with PyDatabaseEngine(args.filename):
        pass
    #db = open_database(args.filename)
