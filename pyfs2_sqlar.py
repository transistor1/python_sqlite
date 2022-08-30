from inspect import trace
import io
import logging
import os
import sys

from collections import namedtuple
from contextlib import contextmanager
from pathlib import PurePosixPath
import traceback

import fs # ResourceType
import fs.errors as fse # ResouceNotFound
import fs.base as fsb # FS
import fs.info as fsi # Info
import fs.path as fsp
import fs.subfs as sfs
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
    def __init__(self, filename=None, root = '/'):
        super().__init__()
        self.filename = filename
        self._file = None
        self._root = root
        self._closed = False
        self.invalid_path_chars = '\\:@\n\0'

    def _tr_path(self, path):
        path = fsp.normpath(fsp.abspath(path))
        path = fsp.combine(self._root, path)
        return path

    @property
    def file(self):
        if not self._file:
            self._file = SQLiteArchive(self.filename, mode="rwc")
        return self._file

    def _path_exists(self, path):
        path = self._tr_path(path)
        path_obj = sqlar.get_path_info(self.file, path)
        if path_obj == None:
            return False
        return True
    
    def _check_invalid(self, path):
        for ch in self.invalid_path_chars:
            if ch in path:
                return True
        return False

    def _validate_intermediate_paths(self, path):
        full_path = self._tr_path(path)
        while fsp.dirname(full_path) != '/':
            parent = fsp.dirname(full_path)
            path_info = sqlar.get_path_info(self.file, str(parent))
            if path_info == None or not path_info.is_dir:
                raise fse.ResourceNotFound(path)
            full_path = parent

    def close(self):
        self.file.close()
        self._closed = True

    def isclosed(self):
        return self._closed

    def getmeta(self, namespace='standard'):
        if namespace != 'standard':
            return {}
        meta = {'case_insensitive': False,
                'invalid_path_chars': self.invalid_path_chars,
                'max_path_length': None,
                'max_sys_path_length': None,
                'network': False,
                'read_only': False,
                'rename': False}
        return meta

    def getinfo(self, path, namespaces=None):
        path = self._tr_path(path)
        namespaces = namespaces or ()
        path_obj = sqlar.get_path_info(self.file, path)
        if path_obj == None:
            raise fse.ResourceNotFound(path)
        resource_type = [fs.ResourceType.file, fs.ResourceType.directory, fs.ResourceType.symlink] \
                            [path_obj.is_sym << 1 | path_obj.is_dir]
        logger.debug(f'PATHINFO: {str(path_obj)}')
        #I think "name" needs to be just the filename, not full path
        #info = {"basic": {"name": path_obj.name, "is_dir": path_obj.is_dir}}
        info = {"basic": {"name": fsp.basename(path_obj.name), "is_dir": path_obj.is_dir}} 
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

    def scandir(self, path, namespaces=None, page=None):
        path = self._tr_path(path)
        # Make sure path exists; getinfo will
        # throw ResourceNotFound if it doesn't.
        dirinfo = self.getinfo(path)
        if not dirinfo.is_dir:
            raise fse.DirectoryExpected(path)
        pattern = fsp.combine(path, '*')
        files = sqlar.find_files(self.file, pattern, True)
        start, end = None, None
        def _getfiles(_files):
            for _file in _files:
                if (fsp.dirname(_file.name) or '/') == path and _file.name != '/':
                    yield _file
        #files = iter([file.name for file in files if (fsp.dirname(file.name) or '/') == path and file.name != '/'])
        files = _getfiles(files)
        if page != None:
            start, end = page
            # For paging
            page = []
            try:
                # for idx in range(0, start or len(files)):
                #     next(files)
                # for idx in range(start, end or len(files)):
                #     page.append(next(files))
                try:
                    while True:
                        if (start or 0) > 0:
                            next(files)
                            start -= 1
                        else:
                            break
                except StopIteration:
                    pass                
                try:
                    for _ in range(0, end):
                        page.append(next(files))
                except StopIteration:
                    pass
                files = page
            except StopIteration:
                pass
        for file in files:
            yield self.getinfo(file.name)


    def listdir(self, path):
        path = self._tr_path(path)
        # Make sure path exists; getinfo will
        # throw ResourceNotFound if it doesn't.
        dirinfo = self.getinfo(path)
        if not dirinfo.is_dir:
            raise fse.DirectoryExpected(path)
        pattern = fsp.combine(path, '*')
        files = sqlar.find_files(self.file, pattern, True)
        files = [file.name for file in files if (fsp.dirname(file.name) or '/') == path]
        files = [fsp.relativefrom(path, file) for file in files]
        return files

    def makedir(self, path, permissions=None, recreate=False):
        path = self._tr_path(path)
        self._validate_intermediate_paths(path)
        if self.exists(path):
            if not recreate:
                raise fse.DirectoryExists(path)
            else:
                return sfs.SubFS(self, path)
        sqlar.write(self.file, path, path, True)
        return sfs.SubFS(self, path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        if self._closed:
            raise fse.FilesystemClosed(path)
        if self._check_invalid(path):
            raise fse.InvalidCharsInPath(path)
        path = self._tr_path(path)
        if len(mode) > 3 or 't' in mode:
            raise ValueError()
        if mode[-1] != 'b':
            mode += 'b'
        _mode = mode[:-1]
        _type = mode[-1]
        if _type != 'b':
            raise ValueError()
        if _mode[0] not in ['a', 'r', 'w', 'x'] or _type != 'b':
            raise ValueError()
        path_info = sqlar.get_path_info(self.file, path)
        if path_info and path_info.is_dir:
            raise fse.FileExpected(path)
        file_obj = None
        if _mode[0] == 'r':
            if path_info == None:
                raise fse.ResourceNotFound(path)
            file_obj = SQLARFileWriter(self.file, path, 'rb')
        elif _mode[0] in ['a', 'w']:
            # Check to see if parent directories exist:
            self._validate_intermediate_paths(path)
            file_obj = SQLARFileWriter(self.file, path, mode)
        elif _mode == 'x':
            if path_info:
                raise fse.FileExists(path)
            file_obj = SQLARFileWriter(self.file, path, mode)
        return file_obj

    def remove(self, path):
        path = self._tr_path(path)
        info = self.getinfo(path)
        if info == None:
            raise fse.ResourceNotFound(path)
        elif info.is_dir:
            raise fse.FileExpected(path)
        sqlar.delete_file(self.file, path)

    def removedir(self, path):
        path = self._tr_path(path)
        if path == '/':
            raise fse.RemoveRootError(path)
        info = self.getinfo(path)
        if info == None:
            raise fse.ResourceNotFound(path)
        elif not info.is_dir:
            raise fse.DirectoryExpected(path)
        files = self.listdir(path)
        if len(files) > 0:
            raise fse.DirectoryNotEmpty(path)
        sqlar.delete_file(self.file, path)

    def setinfo(self, path, info):
        pass    


class SQLARFileWriter(io.RawIOBase):
    def __init__(self, archive_filename, internal_filename_path, mode='wb'):
        super().__init__()
        self._pos = 0
        self._buffer = None
        if len(mode) <= 2 and mode[-1] != 'b':
            mode += 'b'
        self.mode = mode
        if isinstance(archive_filename, SQLiteArchive):
            self.sqlite_archive = archive_filename
        else:
            self.sqlite_archive = SQLiteArchive(archive_filename, mode='rwc')
        self.internal_filename_path = internal_filename_path
        self._fileinfo = sqlar.get_path_info(self.sqlite_archive, internal_filename_path)
        if self._mode[0] == 'r':
            self._write(self.sqlite_archive.read(self.internal_filename_path))
            self.seek(0)
        if self._mode == 'w':
            # "touch" the file
            self._write(b'')
        
    @property
    def _mode(self):
        return self.mode[:-1]

    @property
    def _type(self):
        return self.mode[-1]

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, _offset, _whence=0):
        if _whence == 0:
            self._pos = _offset
        elif _whence == 1:
            # From current pos
            self._pos += _offset
        elif _whence == 2:
            # From EOF, offset should be negative:
            data = self.read()
            self._pos = len(data) + _offset

    def truncate(self, _size):
        #return super().truncate(_size)
        new_data = self.read(_size)
        self.seek(0)
        self._write(new_data)

    def write(self, _buffer):
        if 'r' in self.mode and '+' not in self.mode:
            raise OSError()
        return self._write(_buffer)

    def _write(self, _buffer):
        if self._pos == 0:
            sqlar.write(self.sqlite_archive, '', self.internal_filename_path, data=_buffer, mode=self.mode)
        else:
            sqlar.write(self.sqlite_archive, '', self.internal_filename_path, data=_buffer, mode='ab')
        self._pos += len(_buffer)
        return len(_buffer)

    def writelines(self, _lines):
        for line in _lines:
            self._write(line)

    def readline(self, _size = None):
        _bytes_read = b''
        _size = _size or -1
        while True:
            ch = self.read(1)
            _bytes_read += ch
            if ch == b'\n':
                _size -= 1
            if ch == b'\n' or ch == b'' or (_size or -1) == 0:
                break
        return _bytes_read

    def _iter_readline(self):
        while True:
            data = self.readline()
            if data == b'':
                break
            yield data

    def __iter__(self):
        yield from self._iter_readline()

    def readlines(self, _hint=-1):
        # hint values of 0 or less, as well as None, are treated as no hint.
        _hint = _hint or -1
        _tot_len = 0
        lines = []
        while True:
            line = self.readline(1)
            _tot_len += len(line)
            if line == b'':
                break
            lines.append(line)
            if _hint > 0 and _tot_len > _hint:
                break
        return lines

    def read(self, _size = None):
        if 'w' in self.mode and '+' not in self.mode:
            raise OSError()
        if self._pos == 0:
            # So we only have 1 trip to the database
            data = self.sqlite_archive.read(self.internal_filename_path)
            self._buffer = io.BytesIO()
            self._buffer.write(data)
            self._buffer.seek(0)
        data = self._buffer.read(_size)
        self._pos = (self._pos or 0) + len(data)
        return data

    def readinto(self, _buffer: bytearray):
        _buffer.extend(self.read())

    def readable(self) -> bool:
        return self._mode == 'r'

    def writable(self) -> bool:
        return self._mode in ['a', 'w', 'x']
