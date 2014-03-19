import types

from achus import exception
from achus import test
from achus import utils


class UtilTest(test.TestCase):
    def test_import_class(self):
        self.assertEqual(test.TestCase,
                         utils.import_class("achus.test.TestCase"))

    def test_cannot_import_class(self):
        self.assertRaises(exception.ClassNotFound,
                          utils.import_class,
                          "achus.tests.FooBar")

    def test_cannot_import_module_when_importing_class(self):
        self.assertRaises(ImportError,
                          utils.import_class,
                          "foo.bar.baz:FooBar")

    def test_cannot_import_module(self):
        self.assertRaises(ImportError,
                          utils.import_module,
                          "foo . bar . baz")

    def test_import_module(self):
        self.assertIsInstance(utils.import_module("os.path"),
                              types.ModuleType)
