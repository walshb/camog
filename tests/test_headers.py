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

import camog

def _do_parse_headers(csv_str, sep=',', nthreads=4):
    return camog.parse_csv(csv_str, sep, nthreads, 0, 1)[0]


def _do_parse_both(csv_str, sep=',', nthreads=4):
    return camog.parse_csv(csv_str, sep, nthreads, 0, 1)


def test_headers1():
    res = _do_parse_headers('123,456,789')

    assert isinstance(res, list)
    assert res == ['123', '456', '789']


def test_empty_headers1():
    res = _do_parse_headers(',123,')

    assert isinstance(res, list)
    assert res == ['', '123', '']


def test_headers_data():
    headers, data = _do_parse_both('123,456,789\n123,456,789\n')

    assert headers == ['123', '456', '789']
    assert np.all(data[0] == np.array([123]))
    assert np.all(data[1] == np.array([456]))
    assert np.all(data[2] == np.array([789]))


def test_headers_data2():
    headers, data = _do_parse_both('123,456,789\n\n123,456,789\n')

    assert headers == ['123', '456', '789']
    assert np.all(data[0] == np.array([0, 123]))
    assert np.all(data[1] == np.array([0, 456]))
    assert np.all(data[2] == np.array([0, 789]))
