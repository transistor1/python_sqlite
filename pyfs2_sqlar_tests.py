import unittest
from fs.test import FSTestCases
from pyfs2_sqlar import SQLARFS


class TestSQLARFS(FSTestCases, unittest.TestCase):

    def make_fs(self):
        # Return an instance of your FS object here
        return SQLARFS('tmp/archive.sqlar')

unittest.main()
