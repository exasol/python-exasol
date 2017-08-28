'''Test the frame work functionality'''

import unittest

class SelfTest(unittest.TestCase):
    def test_import(self):
        try:
            import exasol
        except Exception as e:
            self.fail(str(e))


if __name__ == '__main__':
    unittest.main(verbosity=2)

# vim: ts=4:sts=4:sw=4:et:fdm=indent
