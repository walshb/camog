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
        headers, cols = camog.load(fname)

    assert headers == ['abc', 'def', 'ghi']
    assert np.all(cols[0] == np.array([123]))
    assert np.all(cols[1] == np.array([456]))
    assert np.all(cols[2] == np.array([789]))


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


def test_load_bad_utf8():
    bdata = b'\xbf'

    with th.TempCsvFile(data=None, bdata=bdata) as fname:
        headers, _ = camog.load(fname)

    assert isinstance(headers[0], bytes)
    assert headers[0] == bdata


def test_col_to_type_str():
    data = 'abc,def,ghi\n123,456,789\n'

    with th.TempCsvFile(data) as fname:
        headers, cols = camog.load(fname, col_to_type={'abc': str})

    assert headers == ['abc', 'def', 'ghi']
    assert cols[0].dtype.kind == 'S'
    assert np.all(cols[0] == np.array([b'123']))
    assert cols[1].dtype.kind == 'i'
    assert np.all(cols[1] == np.array([456]))
    assert cols[2].dtype.kind == 'i'
    assert np.all(cols[2] == np.array([789]))


def test_col_to_type_int():
    data = 'abc,def,ghi\n123,456,789\n'

    with th.TempCsvFile(data) as fname:
        headers, cols = camog.load(fname, col_to_type={1: str})

    assert headers == ['abc', 'def', 'ghi']
    assert cols[0].dtype.kind == 'i'
    assert np.all(cols[0] == np.array([123]))
    assert cols[1].dtype.kind == 'S'
    assert np.all(cols[1] == np.array([b'456']))
    assert cols[2].dtype.kind == 'i'
    assert np.all(cols[2] == np.array([789]))


def test_col_to_type_want_float():
    data = 'abc,def,ghi\naaaaaaaa,bbbbbbbb,123.0\n'

    with th.TempCsvFile(data) as fname:
        headers, cols = camog.load(fname, missing_float_val=np.nan,
                                   col_to_type={0: float, 1: float})

    assert headers == ['abc', 'def', 'ghi']
    assert cols[0].dtype.kind == 'f'
    assert np.isnan(cols[0][0])
    assert cols[1].dtype.kind == 'f'
    assert np.isnan(cols[1][0])
    assert cols[2].dtype.kind == 'f'
    assert np.all(cols[2] == np.array([123.0]))
