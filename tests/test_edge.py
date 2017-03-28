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


def _do_parse_csv(csv_str, sep=',', nthreads=4):
    return camog.parse_csv(csv_str, sep, nthreads)[1]


def test_empty():
    res = _do_parse_csv('\n', ',', 2)

    # If ',,' is 3 columns and ',' is 2 columns then '' is 1 column.
    assert len(res) == 1


def test_empty_first_line():
    res = _do_parse_csv('\na', ',', 2)

    assert len(res) == 1

    assert res[0].dtype == 'S1'
    assert np.all(res[0] == np.array(['', 'a']))


def test_empty_quotes():
    res = _do_parse_csv('""\n', ',', 2)

    assert len(res) == 1
    assert res[0].dtype == 'S1'
    assert np.all(res[0] == np.array(['']))


def test_empty_first_line2():
    res = _do_parse_csv('\n,a,a\n', ',', 2)

    assert len(res) == 3

    assert res[0].dtype == int
    assert np.all(res[0] == np.array([0, 0]))
    assert res[1].dtype == 'S1'
    assert np.all(res[1] == np.array(['', 'a']))
    assert res[2].dtype == 'S1'
    assert np.all(res[2] == np.array(['', 'a']))


def test_empty_line():
    res = _do_parse_csv(',\n\n,1\n', ',', 2)

    assert len(res) == 2

    assert res[0].dtype == int
    assert np.all(res[0] == np.array([0, 0, 0]))
    assert res[1].dtype == int
    assert np.all(res[1] == np.array([0, 0, 1]))


if __name__ == '__main__':
    test_empty_line()

