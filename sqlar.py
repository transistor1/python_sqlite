#!python
from asyncio.format_helpers import extract_stack
from datetime import datetime
import fnmatch
import os
import re
import sqlite3
from collections import namedtuple
from glob import glob
from pathlib import Path

import click
import pysqlar
from traitlets import default


SQLARInfo = namedtuple('SQLARInfo', 'name mode mtime sz is_dir is_sym')
#class SQLARInfo():

console_width = 80


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


def write(arch, filename, arch_filename, is_dir=False):
    def _try_write():
        if is_dir:
            arch.sql(f"INSERT INTO sqlar (name, mode, mtime, sz) VALUES (?, ?, ?, ?)", 
                arch_filename, 0o777, int(datetime.utcnow().timestamp()), 0)
        else:
            arch.write(filename, arch_filename)
    try:
        _try_write()
    except sqlite3.IntegrityError:
        delete_file(arch, arch_filename)
        _try_write()


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
                    f_info = SQLARInfo(archive_filename, stat.st_mode, stat.st_mtime, stat.st_size, file.is_dir())
                    _print_file(f_info)
                    write(new_arch, str(file), archive_filename, file.is_dir())
        finally:
            new_arch.close()


def extract_dir(name):
    path = Path(name)
    path.mkdir(parents=True, exist_ok=True)


def get_data(archive, name):
    return archive.sql("SELECT data FROM sqlar WHERE name=?", name)[0][0]


def extract_symlink(archive, name):
    # Symbolic link
    file = Path(name)
    extract_dir(str(file.parent))
    sympath = get_data(archive, name)
    #print(name, type(name), sympath, type(sympath))
    file.unlink(missing_ok=True)
    file.symlink_to(sympath)


def _extract_files(archive, files):
    with pysqlar.SQLiteArchive(archive, mode='ro') as arch:
        try:
            file: SQLARInfo
            for file in find_files(arch, files):
                _print_file(file)
                if file.is_dir:
                    # Directory
                    extract_dir(file.name)
                elif file.sz == -1:
                    extract_symlink(arch, file.name)
                elif file.sz == 0:
                    # Empty file
                    Path(file.name).touch()
                else:
                    arch.extract(file.name)     
        finally:
            arch.close()


def _sz(text, w):
    fmt = '{{text:{w}}}'.format(w=w)
    txt = fmt.format(text=text)
    if len(txt) > w:
        mid = int(w / 2)
        txt = txt[0:mid-1] + '...' + txt[-mid+2:]
    return txt
    

def _print_file(file):
    # try:
    #     wid, _ = os.get_terminal_size()
    # except OSError:
    #     wid = 80
    wid = console_width
    fn_size = wid - 5 - 15 - 20 - 5 - 5
    if not fn_size <= 0:
        f_type = ['FILE', 'DIR', 'SYM'][file.is_dir | file.is_sym << 1]
        print(f'{f_type:5}{_sz(file.name, fn_size)}{file.mode:5}{file.mtime:15}{file.sz:20}')


def _list(archive, patterns=[]):
    with pysqlar.SQLiteArchive(archive, mode='ro') as arch:
        for file in find_files(arch, patterns):
            _print_file(file)
            

def get_sqlarinfo(archive: pysqlar.SQLiteArchive, *file):
    """
    Create a SQLARInfo object from file details
    :param archive: A SQLiteArchive object (from pysqlar)
    """
    is_dir = False
    is_sym = False
    if file[3] == -1:
        is_sym = True
    elif file[3] == 0:
        data = get_data(archive, file[0])
        if data == None:
            is_dir = True
    sqlarinfo = SQLARInfo(*file, is_dir, is_sym)
    return sqlarinfo


def find_files(archive, patterns):
    if len(patterns) == 0:
        patterns = ['*']
    for file in archive.infolist():
        for pattern in patterns:
            reobj = re.compile(fnmatch.translate(str(pattern)))
            if reobj.match(file[0]):
                yield get_sqlarinfo(archive, *file)


if __name__ == '__main__':
    cli()
