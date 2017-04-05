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

import numpy as np

from setuptools import setup, Extension

ext_modules = [Extension('camog._cfastcsv',
                         ['src/fastcsv.c',
                          'src/pyfastcsv.c'],
                         include_dirs=['gensrc', np.get_include()])]


setup(name="camog",
      packages=["camog"],
      ext_modules=ext_modules)
