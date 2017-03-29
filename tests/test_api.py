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

import os
import tempfile
import shutil

import numpy as np

import camog

def test_load():
    dirname = tempfile.mkdtemp()
    fname = os.path.join(dirname, 'test.csv')

    with open(fname, 'wb') as fp:
        fp.write('abc,def,ghi\n123,456,789\n')

    headers, data = camog.load(fname)

    shutil.rmtree(dirname)

    assert headers == ['abc', 'def', 'ghi']
    assert np.all(data[0] == np.array([123]))
    assert np.all(data[1] == np.array([456]))
    assert np.all(data[2] == np.array([789]))
