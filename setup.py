#!/usr/bin/python
#
# Copyright 2017 Ben Walsh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import subprocess
import numpy as np

from setuptools import setup, Extension

# can't import because we don't have shared library built yet.
exec(open('camog/_version.py').read())  # pylint: disable=exec-used

ext_modules = [Extension('camog._cfastcsv',
                         ['src/fastcsv.c',
                          'src/mtq.c',
                          'src/pyfastcsv.c'],
                         include_dirs=['gensrc', np.get_include()])]

try:
    long_description \
        = subprocess.Popen(['pandoc', '-t', 'rst', '-o', '-', 'README.md'],
                           stdout=subprocess.PIPE).communicate()[0].decode('utf-8')
except OSError:
    long_description = open('README.md').read()

mydir = os.path.dirname(os.path.abspath(__file__))
generate_py = os.path.join(mydir, 'generator', 'generate.py')
if os.path.exists(generate_py):
    destdir = os.path.join(mydir, 'gensrc')
    try:
        os.makedirs(destdir)
    except OSError:
        pass
    subprocess.check_call([sys.executable, generate_py], cwd=destdir)

setup(name='camog',
      version=__version__,
      url='https://github.com/walshb/camog',
      license='Apache License 2.0',
      author='Ben Walsh',
      author_email='b@wumpster.com',
      description='csv file reader',
      long_description=long_description,
      keywords='csv reader',
      platforms='any',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      tests_require=['pytest'],
      packages=['camog'],
      ext_modules=ext_modules)
