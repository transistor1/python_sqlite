import logging
import sys
from collections import namedtuple

import pysqlar
from fs import ResourceType
from fs.base import FS
from fs.errors import ResourceNotFound
from fs.info import Info
from pysqlar import SQLiteArchive

from sqlar import find_files, SQLARInfo


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger(__name__)


class SQLARFS(FS):
    def __init__(self, filename):
        super().__init__()
        self.file = SQLiteArchive(filename)
    
    def close(self):
        self.file.close()

    def getinfo(self, path, namespaces=None):
        namespaces = namespaces or ()
        cursor = self.file._conn.cursor()
        pathinfo = self.file.getinfo(path)
        is_dir = False
        resource_type = ResourceType.file
        if not pathinfo:
            cursor.execute('SELECT ? as name, '
                           '0 as mode, '
                           'max(s.mtime) as mtime, '
                           'sum(s.sz) as sz '
                           'FROM sqlar s '
                           'WHERE name LIKE ?;', (path, f'{path}%',))
            pathinfo = cursor.fetchall()
            if len(pathinfo) > 0:
                resource_type = ResourceType.directory
                is_dir = True
            else:
                raise ResourceNotFound
        pathinfo = list(pathinfo) + [is_dir]
        logger.debug(f'*** PATHINFO: {str(pathinfo)}')
        path_obj = SQLARPathInfo(*pathinfo)
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
        return Info(info)

    def listdir(self, path):
        pass

    def makedir(self, path, permissions=None, recreate=False):
        pass

    def openbin(self, path, mode="r", buffering=-1, **options):
        pass

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass    
