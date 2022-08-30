#!env/bin/python
from asyncio.format_helpers import extract_stack
from datetime import datetime
import fnmatch
import math
import os
import re
import sqlite3
from collections import namedtuple
from glob import glob
from pathlib import Path

import click
import fs.errors as fse
import pysqlar
from traitlets import default


class SQLARFileInfo:
    def __init__(self, name, mode, mtime, sz, is_dir, is_sym) -> None:
        self.name = name
        self.mode = mode
        self.mtime = mtime
        #self.mtime = datetime.fromtimestamp(mtime)
        self.sz = sz
        self.is_dir = is_dir
        self.is_sym = is_sym
        self.display_format = r'{_type:5} {name} {mode:5} {mtime:19} {sz:>20} '

    @property
    def wid(self):
        try:
            wid, _ = os.get_terminal_size()
        except:
            wid = 80
        return wid

    @property
    def fn_size(self):
        return self.wid - 5 - 5 - 19 - 20 - 5

    @property
    def _name(self):
        text = self.name
        w = self.fn_size
        fmt = '{{text:{w}}}'.format(w=w)
        txt = fmt.format(text=text)
        if len(txt) > w:
            w -= 3 # for ellipses
            mid = math.floor(len(txt) / 2)
            remove_chrs = len(txt) - w
            sub_left = math.floor(remove_chrs / 2)
            sub_right = (remove_chrs - sub_left)
            left = txt[0:mid-sub_left]
            right = txt[mid+sub_right:]
            txt = f'{left}...{right}'
        return txt

    @property
    def f_type(self):
        return ['FILE', 'DIR', 'SYM'][self.is_dir | self.is_sym << 1]

    def __str__(self) -> str:
        if not self.fn_size <= 0:
            return self.display_format.format(_type=self.f_type,
                name=self._name, mode=str(self.mode), mtime=str(self.mtime),
                sz=str(self.sz))


def write(arch, filename, arch_filename, is_dir=False, data=None, mode='wb'):
    def _try_write(arch, filename, arch_filename, is_dir=False, data=None, mode=mode):
        if is_dir:
            arch.sql(f"INSERT INTO sqlar (name, mode, mtime, sz) VALUES (?, ?, ?, ?)", 
                arch_filename, 0o777, int(datetime.utcnow().timestamp()), 0)
        elif data != None:
            arch.writestr(arch_filename, data, mode=mode)
        else:
            arch.write(filename, arch_filename)
    try:
        _try_write(arch, filename, arch_filename, is_dir, data, mode)
    except sqlite3.IntegrityError:
        delete_file(arch, arch_filename)
        _try_write(arch, filename, arch_filename, is_dir, data, mode)


def delete_file(archive, file):
    archive.sql(f"DELETE FROM sqlar WHERE name=?", file)


def _make_archive(archive, files):
    with pysqlar.SQLiteArchive(archive, mode='rwc') as new_arch:
        try:
            file: Path
            for pattern in files:
                for file in [Path(p) for p in glob(str(pattern), recursive=True)]:
                    stat = file.stat()
                    npath = os.path.normpath(str(file))
                    prefix = re.match('^([.][.]/)+', npath)
                    archive_filename = str(file)
                    if prefix:
                        archive_filename = npath[len(prefix.group(0)):]
                    f_info = SQLARFileInfo(archive_filename, stat.st_mode, stat.st_mtime, stat.st_size, file.is_dir())
                    print(str(f_info))
                    write(new_arch, str(file), archive_filename, file.is_dir())
        finally:
            new_arch.close()


def extract_dir(name):
    path = Path(name)
    path.mkdir(parents=True, exist_ok=True)


def _extract_symlink(archive, name):
    # Symbolic link
    file = Path(name)
    extract_dir(str(file.parent))
    sympath = archive.read(name).decode()
    file.unlink(missing_ok=True)
    file.symlink_to(sympath)


def _extract_files(archive, files):
    with pysqlar.SQLiteArchive(archive, mode='ro') as arch:
        try:
            file: SQLARFileInfo
            for file in find_files(arch, files):
                print(str(file))
                if file.is_dir:
                    # Directory
                    extract_dir(file.name)
                elif file.sz == -1:
                    _extract_symlink(arch, file.name)
                elif file.sz == 0:
                    # Empty file
                    Path(file.name).touch()
                else:
                    arch.extract(file.name)     
        finally:
            arch.close()


def _list(archive, patterns=[]):
    with pysqlar.SQLiteArchive(archive, mode='ro') as arch:
        for file in find_files(arch, patterns):
            print(str(file))
            
def path_is_dir(archive, path):
    data = archive.sql("SELECT data FROM sqlar WHERE name=? AND data IS NULL", path)
    return len(data) > 0

def get_sqlarinfo(archive: pysqlar.SQLiteArchive, *file) -> SQLARFileInfo:
    """
    Create a SQLARInfo object from file details
    :param archive: A SQLiteArchive object (from pysqlar)
    """
    is_dir = False
    is_sym = False
    if file[3] == -1:
        is_sym = True
    elif file[3] == 0:
        # Check if it's a directory
        is_dir = path_is_dir(archive, file[0])
    sqlarinfo = SQLARFileInfo(*file, is_dir, is_sym)
    return sqlarinfo


def find_files(archive, patterns, from_root=False):
    if type(patterns) is tuple:
        patterns = list(patterns)
    if type(patterns) is not list:
        patterns = [patterns]
    if len(patterns) == 0:
        patterns = ['*']
    for file in archive.infolist():
        for pattern in patterns:
            reobj = re.compile(('^' if from_root else '') + fnmatch.translate(str(pattern)))
            if reobj.match(file[0]):
                yield get_sqlarinfo(archive, *file)


def get_path_info(archive, path):
    if len(path) == 0:
        return None
    elif path == '/':
        return SQLARFileInfo('', 0o777, int(datetime.utcnow().timestamp()), 
            0, True, False)
    else:
        # if path.startswith('/'):
        #     path = path[1:]
        file = archive.getinfo(path)
        if file == None:
            return None
        return get_sqlarinfo(archive, *file)


@click.command()
@click.option('-l', 'command', flag_value='list')
@click.option('-x', 'command', flag_value='extract')
@click.option('-w', 'width', default=80)
@click.argument('archive', required=True, type=click.Path(path_type=Path, exists=False))
@click.argument('files', required=False, type=click.Path(path_type=Path), nargs=-1)
def cli(command, width, archive, files):
    global console_width
    console_width = width
    if command == None:
        if len(files) == 0:
            raise click.UsageError("No filenames provided.")
    if command == None:
        # Archive files
        _make_archive(archive, files)
    elif command == 'extract':
        _extract_files(archive, files)
    elif command == 'list':
        _list(archive, files)


if __name__ == '__main__':
    cli()
