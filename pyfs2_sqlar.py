from inspect import trace
import io
import logging
import os
import sys

from collections import namedtuple
from contextlib import contextmanager
import traceback

import fs # ResourceType
import fs.errors as fse # ResouceNotFound
import fs.base as fsb # FS
import fs.info as fsi # Info
import sqlar
from pysqlar import SQLiteArchive


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger(__name__)


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


class SQLARFileWriter(io.RawIOBase):
    def __init__(self, filename, path, mode='wb'):
        super().__init__()
        if len(mode) == 1:
            mode += 'b'
        self.mode = mode
        if isinstance(filename, SQLiteArchive):
            self.file = filename
        else:
            self.file = SQLiteArchive(filename, mode='rwc')
        self.path = path
        self._pos = 0
        self._fileinfo = sqlar.get_path_info(self.file, path)
    
    def fileno(self) -> int:
        raise io.UnsupportedOperation

    def readinto(self, _buffer):
        if self._pos >= self._fileinfo.sz:
            return 0
            raise io.BlockingIOError
        file_data = self.file.read(self.path)
        if file_data == None:
            raise fse.ResourceNotFound(self.path)
        _buffer[0:len(file_data)] = file_data
        self._pos = len(file_data)
        return self._pos

    def writable(self) -> bool:
        return self.mode[0] in ['w','x']
            
    def readable(self) -> bool:
        return self.mode[0] == 'r'
    
    def write(self, bytes_):
        self.file.writestr(self.path, bytes_, overwrite=True)
        return len(bytes_)

    def close(self) -> None:
        super().close()
        pass
