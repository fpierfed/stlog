#!/usr/bin/env python
from distutils.core import setup
import glob



NAME = 'stlog'
DESC = 'STScI Logging Module'
AUTHOR_NAME = 'Francesco Pierfederici'
AUTHOR_EMAIL = 'fpierfed@stsci.edu'

SCRIPTS = glob.glob('bin/*.py')

LICENSE = 'BSD'
VERSION = '0.1'


setup(name = NAME,
      description = DESC,
      author = AUTHOR_NAME,
      author_email = AUTHOR_EMAIL,
      license = LICENSE,
      version = VERSION,

      scripts = SCRIPTS,
      packages = [NAME, ],
      package_dir = {NAME: NAME},
)

