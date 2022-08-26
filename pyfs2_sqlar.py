from inspect import trace
import io
import logging
import os
import sys

from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path
import traceback

import fs # ResourceType
import fs.errors as fse # ResouceNotFound
import fs.base as fsb # FS
import fs.info as fsi # Info
import sqlar
from pysqlar import SQLiteArchive


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger(__name__)


def getsplit(text, max, start=0):
    spl = text.find('\n', start)
    if spl == -1:
        r = text[start:][:max]
        return len(r), start + len(r), r
    r = text[start:spl][:max]
    return len(r), start + len(r), r


class SQLARFS(fsb.FS):
    def __init__(self, filename=None):
        super().__init__()
        self.filename = filename
        self._file = None

    @property
    def file(self):
        if not self._file:
            self._file = SQLiteArchive(self.filename, mode="rwc")
        return self._file

    def _path_exists(self, path):
        path_obj = sqlar.get_path_info(self.file, path)
        if path_obj == None:
            return False
        return True
    
    def close(self):
        self.file.close()

    def getinfo(self, path, namespaces=None):
        namespaces = namespaces or ()
        path_obj = sqlar.get_path_info(self.file, path)
        if path_obj == None:
            raise fse.ResourceNotFound(path)
        resource_type = [fs.ResourceType.file, fs.ResourceType.directory, fs.ResourceType.symlink] \
                            [path_obj.is_sym << 1 | path_obj.is_dir]
        logger.debug(f'PATHINFO: {str(path_obj)}')
        info = {"basic": {"name": path_obj.name, "is_dir": path_obj.is_dir}}
        if "details" in namespaces:
            info["details"] = {
                "_write": ["accessed", "modified"],
                "type": int(resource_type),
                "size": path_obj.sz,
                "accessed": 0,
                "modified": path_obj.mtime,
                "created": 0
            }
        return fsi.Info(info)

    def listdir(self, path):
        pass

    def makedir(self, path, permissions=None, recreate=False):
        sqlar.write(self.file, path, path, True)

    def openbin(self, path, mode="r", buffering=-1, **options):
        if len(mode) > 2:
            raise ValueError()
        if len(mode) == 1:
            mode += 'b'
        _mode, _type = mode[0:2]
        if _mode not in ['r', 'w', 'x'] or _type != 'b':
            raise ValueError()
        path_info = sqlar.get_path_info(self.file, path)
        if path_info and path_info.is_dir:
            raise fse.FileExpected(path)
        file_obj = None
        if _mode == 'r':
            if path_info == None:
                raise fse.ResourceNotFound(path)
            file_obj = SQLARFileWriter(self.file, path, 'rb')
        elif _mode == 'w':
            # Check to see if parent directories exist:
            full_path = Path(path)
            while str(full_path.parent) != '.':
                parent = full_path.parent
                path_info = sqlar.get_path_info(self.file, str(parent))
                if path_info == None or not path_info.is_dir:
                    raise fse.ResourceNotFound(path)
                full_path = parent
            file_obj = SQLARFileWriter(self.file, path, 'wb')
        elif _mode == 'x':
            if path_info:
                raise fse.FileExists(path)
            file_obj = SQLARFileWriter(self.file, path, 'xb')
        return file_obj

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass    


class SQLARFileWriter(io.BytesIO):
    def __init__(self, filename, path, mode='wb'):
        super().__init__()
        self.mode = mode
        if len(self.mode) == 1: self.mode += 'b'
        self._mode = mode[0]
        self._type = 'b'
        if isinstance(filename, SQLiteArchive):
            self.sqlite_archive = filename
        else:
            self.sqlite_archive = SQLiteArchive(filename, mode='rwc')
        self.path = path
        self._fileinfo = sqlar.get_path_info(self.sqlite_archive, path)
        if mode[0] == 'r':
            self.write(self.sqlite_archive.read(self.path))
            self.seek(0)
        self._pos = None

    def write(self, _buffer):
        sqlar.write(self.sqlite_archive, '', self.path, data=_buffer)
        return len(_buffer)

    def read(self, _size = None):
        data = self.sqlite_archive.read(self.path)[self._pos:_size]
        self._pos = (self._pos or 0) + len(data)
        return data

    # def writelines(self, _lines):
    #     return super().writelines(_lines)

    # def readinto(self, _buffer):
    #     return super().readinto(_buffer)

    def readable(self) -> bool:
        return self._mode == 'r'

    def writable(self) -> bool:
        return self._mode in ['w', 'x']
