import unittest
from pathlib import Path
from fs.test import FSTestCases
from pyfs2_sqlar import SQLARFS


class TestSQLARFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        # Return an instance of your FS object here
        arc = Path('./test.sqlar')
        if arc.exists():
            arc.unlink(missing_ok=True)
        return SQLARFS(str(arc))


unittest.main()
