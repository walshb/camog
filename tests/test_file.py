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
import pytest
import shutil

import numpy as np

import camog._cfastcsv as cfastcsv

import _testhelper

def test_simple_file():
    data = 'abc,def,ghi\n123,456,789\n'

    with _testhelper.TempCsvFile(data) as fname:
        headers, data = cfastcsv.parse_file(fname, ',', 4, 0, 1)

    assert headers == ['abc', 'def', 'ghi']
    assert np.all(data[0] == np.array([123]))
    assert np.all(data[1] == np.array([456]))
    assert np.all(data[2] == np.array([789]))


def test_missing_file():
    fname = '/no_such_file.txt'

    assert not os.path.exists(fname)

    with pytest.raises(IOError):
        cfastcsv.parse_file(fname, ',', 4, 0, 1)
