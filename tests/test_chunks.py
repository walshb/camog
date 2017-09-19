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

import camog._cfastcsv as cfastcsv

import _testhelper as th

def _do_parse_csv(csv_str, nthreads):
    return cfastcsv.parse_csv(csv_str, ',', nthreads)[1]


def test_empty_chunk():
    csv_str = ''' \n'''

    res = _do_parse_csv(csv_str, 2)


def test_fixup_numbers():
    csv_str = '''"aaaaaaaaaaaaaaaa
"

,1
,2
,3
,4
,5
,6
,7
,8
,9
'''

    res = _do_parse_csv(csv_str, 3)

    assert len(res) == 2
    assert np.all(res[0] == th.array(['aaaaaaaaaaaaaaaa\n', '', '', '', '',
                                      '', '', '', '', '', '']))
    assert np.all(res[1] == th.array([0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))


def test_fixup_memory():
    csv_str = '''"0,0,0,0,0,0,0,0,
","1,1,1,1,1,1,1
"

1
2
3
4
9,9
'''

    n = len(csv_str)
    str1 = csv_str[n // 3:n * 2 // 3]
    str2 = csv_str[n * 2 // 3:]
    assert '\n' in str1 and '"' in str1 and ',' in str1
    assert '\n' in str2 and '"' in str2 and ',' in str2

    res = _do_parse_csv(csv_str, 3)

    assert len(res) == 2
    assert np.all(res[0] == th.array(['0,0,0,0,0,0,0,0,\n', '', '1', '2', '3', '4', '9']))
    assert np.all(res[1] == th.array(['1,1,1,1,1,1,1\n', '', '', '', '', '', '9']))


def test_fixup_memory_2():
    csv_str = '''"0,0,0,0,0,0,0,0,
"
"1,1,1,1,1,1,1
"

1
2
3
4
9,9
'''

    n = len(csv_str)
    str1 = csv_str[n // 3:n * 2 // 3]
    str2 = csv_str[n * 2 // 3:]
    assert '\n' in str1 and '"' in str1 and ',' in str1
    assert '\n' in str2 and '"' in str2 and ',' in str2

    res = _do_parse_csv(csv_str, 3)

    assert len(res) == 2
    assert np.all(res[0] == th.array(['0,0,0,0,0,0,0,0,\n', '1,1,1,1,1,1,1\n', '', '1', '2', '3', '4', '9']))
    assert np.all(res[1] == th.array([0, 0, 0, 0, 0, 0, 0, 9]))


def test_empty_middle_chunk():
    csv_str = '''a12,456.0
a12
'''

    n = len(csv_str)
    str1 = csv_str[n // 3:n * 2 // 3]
    str2 = csv_str[n * 2 // 3:]
    assert '\n' not in str1
    assert str2.startswith('\n')

    res = _do_parse_csv(csv_str, nthreads=3)

    assert len(res) == 2
    assert np.all(res[0] == th.array(['a12', 'a12']))
    assert np.all(res[1] == th.array([456.0, 0.0]))


def test_chunk_boundary():
    csv_str = '''a123456789012
a12,a123'''

    n = len(csv_str)
    str1 = csv_str[n // 3:n * 2 // 3]
    str2 = csv_str[n * 2 // 3:]
    assert str1.endswith('\n')
    assert '\n' not in str2

    res = _do_parse_csv(csv_str, nthreads=3)

    assert len(res) == 2
    assert np.all(res[0] == th.array(['a123456789012', 'a12']))
    assert np.all(res[1] == th.array(['', 'a123']))


def test_empty_chunk_in_string():
    csv_str = '''a1234,"a12

345"
'''

    n = len(csv_str)
    str1 = csv_str[n // 3:n * 2 // 3]
    str2 = csv_str[n * 2 // 3:]

    assert '"' in str1
    assert str1.endswith('\n')
    assert str2.startswith('\n')

    res = _do_parse_csv(csv_str, nthreads=3)

    assert len(res) == 2
    assert np.all(res[0] == th.array(['a1234']))
    assert np.all(res[1] == th.array(['a12\n\n345']))


def test_change_type():
    csv_str = '''.0,"12
a123456789012
a1234"
12,"
a1234
a123"'''

    res = _do_parse_csv(csv_str, nthreads=3)

    assert np.all(res[0] == th.array([0.0, 12.0]))
    assert np.all(res[1] == th.array(['12\na123456789012\na1234', '\na1234\na123']))
