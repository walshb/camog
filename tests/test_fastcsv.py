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


def test_fastcsv1():
    res = _do_parse_csv("123,456.234,327.0", nthreads=1)

    assert len(res) == 3
    assert np.allclose(res[0], np.array([123.0]))
    assert np.allclose(res[1], np.array([456.234]))
    assert np.allclose(res[2], np.array([327.0]))


def test_fastcsv2():
    lines = ['123,456.234,327.0',
             '123.0,456.0,789.0',
             '456.0,789.0,123.0']

    res = _do_parse_csv('\n'.join(lines))

    assert len(res) == 3
    assert np.allclose(res[0], np.array([123.0, 123.0, 456.0]))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0]))
    assert np.allclose(res[2], np.array([327.0, 789.0, 123.0]))


def test_fastcsv3():
    lines = ['123,456.234,blah',
             'abc,456.0,foo',
             '456.0,789.0,bar']

    res = _do_parse_csv('\n'.join(lines))

    assert len(res) == 3
    assert res[0].dtype == '|S5' or res[0].dtype == object
    assert np.all(res[0] == np.array(['123', 'abc', '456.0']))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0]))
    assert res[2].dtype == '|S4' or res[2].dtype == object
    assert np.all(res[2] == np.array(['blah', 'foo', 'bar']))


def test_fastcsv4():
    lines = ['123,456.234,blah',
             'abc,456.0,foo',
             '456.0,789.0,bar',
             '']

    res = _do_parse_csv('\n'.join(lines))

    assert len(res) == 3
    assert res[0].dtype == '|S5' or res[0].dtype == object
    assert np.all(res[0] == np.array(['123', 'abc', '456.0']))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0]))
    assert res[2].dtype == '|S4' or res[2].dtype == object
    assert np.all(res[2] == np.array(['blah', 'foo', 'bar']))


def test_fastcsv5():
    n = 10

    lines = ['123,456.234,blah',
             'abc,456.0,foo',
             '456.0,789.0,bar'] * n

    res = _do_parse_csv('\n'.join(lines))

    assert len(res) == 3
    assert res[0].dtype == '|S5' or res[0].dtype == object
    assert np.all(res[0] == np.array(['123', 'abc', '456.0'] * n))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0] * n))
    assert res[2].dtype == '|S4' or res[2].dtype == object
    assert np.all(res[2] == np.array(['blah', 'foo', 'bar'] * n))


def test_quoting1():
    lines = ['123,456.234,"blah"',
             '"abcdef",456.0,"foo"',
             '456.0,789.0,"bar"']

    res = _do_parse_csv('\n'.join(lines))

    assert len(res) == 3
    assert res[0].dtype == '|S6' or res[0].dtype == object
    assert np.all(res[0] == np.array(['123', 'abcdef', '456.0']))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0]))
    assert res[2].dtype == '|S4' or res[2].dtype == object
    assert np.all(res[2] == np.array(['blah', 'foo', 'bar']))


def test_quoting_stringappend():
    lines = ['123,456.234,"blah"',
             'ab"cd"ef,456.0,"foo"',
             '456.0,789.0,"bar"']

    res = _do_parse_csv('\n'.join(lines))

    assert len(res) == 3
    assert res[0].dtype == '|S8' or res[0].dtype == object
    assert np.all(res[0] == np.array(['123', 'ab"cd"ef', '456.0']))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0]))
    assert res[2].dtype == '|S4' or res[2].dtype == object
    assert np.all(res[2] == np.array(['blah', 'foo', 'bar']))


def test_quoting_newline():
    lines = ['123,456.234,"blah"',
             'abcdef,456.0,"longtextwithnewline\nabc"',
             '456.0,789.0,"bar"']

    res = _do_parse_csv('\n'.join(lines), ',', 1)

    assert len(res) == 3
    assert res[0].dtype == '|S6' or res[0].dtype == object
    assert np.all(res[0] == np.array(['123', 'abcdef', '456.0']))
    assert np.allclose(res[1], np.array([456.234, 456.0, 789.0]))
    assert res[2].dtype == '|S23' or res[2].dtype == object
    assert np.all(res[2] == np.array(['blah', 'longtextwithnewline\nabc', 'bar']))


def test_ragged_right_doubles():
    lines = ['1,22',
             '333,4444',
             '55555,6,77',
             '888,9999',
             '22,333,1',
             '4444,55555']

    res = _do_parse_csv('\n'.join(lines), ',', 2)

    assert len(res) == 3
    assert np.all(res[0] == np.array([1.0, 333.0, 55555.0, 888.0, 22.0, 4444.0]))
    assert np.all(res[1] == np.array([22.0, 4444.0, 6.0, 9999.0, 333.0, 55555.0]))
    assert np.all(res[2] == np.array([0.0, 0.0, 77.0, 0.0, 1.0, 0.0]))


def test_ragged_right_doubles_late():
    lines = ['1,22',
             '333,4444',
             '55555,6',
             '888,9999',
             '22,333',
             '4444,55555,1']

    res = _do_parse_csv('\n'.join(lines), ',', 2)

    assert len(res) == 3
    assert np.all(res[0] == np.array([1.0, 333.0, 55555.0, 888.0, 22.0, 4444.0]))
    assert np.all(res[1] == np.array([22.0, 4444.0, 6.0, 9999.0, 333.0, 55555.0]))
    assert np.all(res[2] == np.array([0.0, 0.0, 00, 0.0, 0.0, 1.0]))


def test_ragged_right_strings():
    lines = ['a,bb',
             'ccc,dddd',
             'eeeee,f,gg',
             'hhh,iiii',
             'bb,ccc,a',
             'dddd,eeeee']

    res = _do_parse_csv('\n'.join(lines), ',', 2)

    assert len(res) == 3
    assert np.all(res[0] == np.array(['a', 'ccc', 'eeeee', 'hhh', 'bb', 'dddd']))
    assert np.all(res[1] == np.array(['bb', 'dddd', 'f', 'iiii', 'ccc', 'eeeee']))
    assert np.all(res[2] == np.array(['', '', 'gg', '', 'a', '']))


if __name__ == '__main__':
    test_fastcsv1()
