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

import pytest

import numpy as np

import camog

import _testhelper as th

def test_load():
    data = 'abc,def,ghi\n123,456,789\n'

    with th.TempCsvFile(data) as fname:
        headers, data = camog.load(fname)

    assert headers == ['abc', 'def', 'ghi']
    assert np.all(data[0] == np.array([123]))
    assert np.all(data[1] == np.array([456]))
    assert np.all(data[2] == np.array([789]))


def test_load_invalid_nthreads():
    data = 'abc,def,ghi\n123,456,789\n'

    with pytest.raises(ValueError):
        with th.TempCsvFile(data) as fname:
            camog.load(fname, nthreads=0)


def test_load_invalid_fname():
    data = 'abc,def,ghi\n123,456,789\n'

    with pytest.raises(ValueError):
        camog.load(object())


def test_load_invalid_separator():
    data = 'abc,def,ghi\n123,456,789\n'

    with pytest.raises(ValueError):
        with th.TempCsvFile(data) as fname:
            camog.load(fname, sep=object())
