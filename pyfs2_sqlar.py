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
import fs.mode as fsm
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
            path_info = self._get_sqlar_path_info(parent)
            if not path_info.is_dir:
                raise fse.ResourceNotFound(path)
            full_path = parent

    def close(self):
        if not self._closed:
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

    def _get_sqlar_path_info(self, path):
        """Get a SQLARFileInfo object

        :param path: Path of the file or directory

        :raises ResourceNotFound: if file or directory doesn't exist
        """
        path_obj = sqlar.get_path_info(self.file, path)
        if path_obj == None:
            raise fse.ResourceNotFound(path)
        return path_obj

    def getinfo(self, path, namespaces=None):
        path = self._tr_path(path)
        namespaces = namespaces or ()
        path_obj = self._get_sqlar_path_info(path) # Raises ResourceNotFound if non-existent
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
        mode_obj = fsm.Mode(mode)
        mode_obj.validate(_valid_chars=frozenset("rwxab+"))
        path = self._tr_path(path)
        path_info = None
        try:
            path_info = self._get_sqlar_path_info(path)
        except fse.ResourceNotFound:
            if mode_obj.reading:
                raise
        if path_info and mode_obj.exclusive:
            raise fse.FileExists(path)
        if path_info and path_info.is_dir:
            raise fse.FileExpected(path)
        # Check to see if parent directories exist:
        self._validate_intermediate_paths(path)
        file_obj = SQLARFileWriter(self.file, path, mode)
        return file_obj

    def remove(self, path):
        _orig_path = path
        path = self._tr_path(path)
        try:
            info = self.getinfo(path)
        except fse.ResourceNotFound:
            raise fse.ResourceNotFound(_orig_path) from None
        if info.is_dir:
            raise fse.FileExpected(path)
        sqlar.delete_file(self.file, path)

    def removedir(self, path):
        path = self._tr_path(path)
        if path == '/':
            raise fse.RemoveRootError(path)
        info = self.getinfo(path)
        if not info.is_dir:
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
        self._buffer = io.BytesIO()
        self._flush_pos = 0
        self._mode = fsm.Mode(mode)
        self._closed = False
        self.sqlite_archive = None
        if isinstance(archive_filename, SQLiteArchive):
            self.sqlite_archive = archive_filename
        else:
            self.sqlite_archive = SQLiteArchive(archive_filename, mode='rwc')
        self.internal_filename_path = internal_filename_path
        self._init_buffer()
        if self._mode.create and not self._mode.appending:
            # overwrite the file
            self.seek(0, io.SEEK_SET)
            self.truncate(0)
            self.flush()
            
    @property
    def mode(self):
        return self._mode.to_platform_bin()

    def _init_buffer(self):
        # So we only have 1 trip to the database
        data = self.sqlite_archive.read(self.internal_filename_path)
        if data != None:
            self._buffer.write(data)
            if not self._mode.appending:
                self._buffer.seek(0)
        if self._mode.appending:
            self._buffer.seek(0, io.SEEK_END)
            self._flush_pos = self.tell()

    def seekable(self):
        return True

    def _validate_seekable(self):
        if not self.seekable():
            raise OSError

    def tell(self):
        self._validate_seekable()
        return self._buffer.tell()

    def seek(self, _offset, _whence=0):
        self._validate_seekable()
        return self._buffer.seek(_offset, _whence)

    def truncate(self, _size):
        self._validate_seekable()
        new_file_size = self._buffer.truncate(_size)
        old_file_size = len(self._buffer.getbuffer())
        if new_file_size > old_file_size:
            pos = self.tell()
            self.seek(old_file_size)
            self.write(b'\0' * (new_file_size - old_file_size))
            self.seek(pos)
            #self._buffer.getbuffer()[pos:new_file_size] = b'\0' * (new_file_size - pos + 1)
        return new_file_size

    def write(self, _buffer):
        if not self.writable():
            raise OSError()
        return self._write_buf(_buffer)

    def _write_buf(self, _buffer):
        return self._buffer.write(_buffer)

    def close(self):
        if not self._closed:
            if self.writable():
                self.flush()
            self._closed = True
    
    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        self._closed = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #return super().__exit__(exc_type, exc_val, exc_tb)
        self.close()

    def flush(self):
        if self.writable():
            data = self._buffer.getbuffer().tobytes()
            sqlar.write(self.sqlite_archive, '', self.internal_filename_path, data=data, mode=str(self._mode), cursor_pos=self.tell())
            self._flush_pos = self.tell()

    def writelines(self, _lines):
        if not self.writable():
            raise OSError()
        self._buffer.writelines(_lines)

    def readline(self, _size = None):
       return self._buffer.readline(_size)

    def __iter__(self):
        yield from self._buffer.__iter__()

    def readlines(self, _hint=-1):
        if not self.readable():
            raise OSError()
        return self._buffer.readlines(_hint)

    def read(self, _size = None):
        if not self.readable():
            raise OSError()
        data = self._buffer.read(_size)
        return data

    def readinto(self, _buffer: bytearray):
        if not self.readable():
            raise OSError()
        return self._buffer.readinto(_buffer)

    def readable(self) -> bool:
        return self._mode.reading

    def writable(self) -> bool:
        return self._mode.writing
