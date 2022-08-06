import io
import logging
import sys

from collections import namedtuple

import fs # ResourceType
import fs.errors as fse # ResouceNotFound
import fs.base as fsb # FS
import fs.info as fsi # Info
from pysqlar import SQLiteArchive
from sqlar import get_path_info, SQLARFileInfo


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger(__name__)


class SQLARFS(fsb.FS):
    def __init__(self, filename=None):
        super().__init__()
        self.file = SQLiteArchive(filename)
    
    def close(self):
        self.file.close()

    def getinfo(self, path, namespaces=None):
        namespaces = namespaces or ()
        path_obj = get_path_info(self.file, path)
        if path_obj == None:
            raise fse.ResourceNotFound
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
        pass

    def openbin(self, path, mode="r", buffering=-1, **options):
        pass

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass    
