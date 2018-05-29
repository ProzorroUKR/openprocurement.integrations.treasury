# -*- coding: utf-8 -*-
import unittest

from openprocurement.integrations.treasury.tests import utils


def suite():
    suite = unittest.TestSuite()
    suite.addTest(utils.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
