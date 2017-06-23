import os
from distutils.core import setup
setup(name = 'exasol',
      version = '6.0.1',
      description = 'EXASolution Python Package',
      long_description = open(os.path.join(os.path.dirname(__file__), 'README.txt')).read(),
      author = 'EXASOL AG',
      author_email = 'support@exasol.com',
      url = 'http://www.exasol.com/',
      py_modules = ['exasol'],
      scripts = ['exaoutput.py'],
)
