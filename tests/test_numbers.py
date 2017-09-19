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

import csv
import cStringIO
import logging

import numpy as np

import camog._cfastcsv as cfastcsv

_logger = logging.getLogger(__name__)

_maxlen = 6
_chars = '1.+-e '


def _divmod(n, m):
    return n // m, n % m


def _equal(v1, v2):
    """Same type as well."""
    if isinstance(v2, float) and np.isinf(v2):
        return True
    return isinstance(v1, type(v2)) and v1 == v2


def _parse(s, excel_quotes=True):
    res = cfastcsv.parse_csv(s, ',', 1, 1 if excel_quotes else 0)
    return res[1][0][0]


def _py_csv_parse(s, excel_quotes=True):
    reader = csv.reader(cStringIO.StringIO(s), dialect='excel')
    val = list(reader)[0][0]

    if not excel_quotes and s and s[0] == '"':
        return val

    if val == ' ' * len(val):
        return 0

    try:
        vf = float(val)
    except ValueError:
        return val

    try:
        vi = int(val)
    except ValueError:
        return vf

    return vi


def _assert_parse_same(s):
    res = _parse(s, True)
    py_res = _py_csv_parse(s, True)
    assert _equal(res, py_res), '%r => %r vs %r' % (s, res, py_res)

    res = _parse(s, False)
    py_res = _py_csv_parse(s, False)
    assert _equal(res, py_res), '%r => %r vs %r' % (s, res, py_res)


def test_py_csv_parse_1():
    s = '"123""456"789'
    assert _equal(_parse(s, True), '123"456789')
    assert _equal(_parse(s, False), '123"456789')
    assert _equal(_py_csv_parse(s, True), '123"456789')
    assert _equal(_py_csv_parse(s, False), '123"456789')


def test_py_csv_parse_2():
    s = '"123456"789'
    assert _equal(_parse(s, True), 123456789)
    assert _equal(_parse(s, False), '123456789')
    assert _equal(_py_csv_parse(s, True), 123456789)
    assert _equal(_py_csv_parse(s, False), '123456789')


def test_sign_1():
    s = '+123'
    assert _equal(_parse(s, True), 123)


def test_funny_plus():
    _assert_parse_same('+ ')


def test_missing_expo():
    assert _equal(_parse('1e', True), '1e')


def test_expo():
    for num_sign in ('', '+', '-'):
        for expo_sign in ('', '+', '-'):
            _assert_parse_same('%s1e%s2' % (num_sign, expo_sign))


def test_rounding():
    _assert_parse_same('.11111')


def test_negative_expo_1():
    _assert_parse_same('1e-300')


def test_negative_expo_2():
    _assert_parse_same('1e-320')


def test_negative_expo_3():
    _assert_parse_same('1.1111e-304')


def test_negative_expo_4():
    _assert_parse_same('1.1111e-315')


def test_float_neg_zero():
    csv_str = '''-0
1.0
'''

    res = cfastcsv.parse_csv(csv_str, ',', 1)[1]

    assert np.all(res[0] == np.array([-0.0, 1.0]))


def test_int_neg_zero():
    csv_str = '''-0
1
'''

    res = cfastcsv.parse_csv(csv_str, ',', 1)[1]

    assert np.all(res[0] == np.array([0, 1]))


def test_missing_int_val():
    csv_str = '\n'

    res = cfastcsv.parse_csv(csv_str, ',', 1, 0, 0, 123, 0.0)[1]

    assert res[0].dtype == int
    assert len(res[0]) == 1
    assert np.all(res[0] == np.array([123]))


def test_missing_int_val_2():
    csv_str = '1\n1\n1,3\n'

    res = cfastcsv.parse_csv(csv_str, ',', 1, 0, 0, 123, 0.0)[1]

    assert res[0].dtype == int
    assert res[1].dtype == int
    assert len(res[0]) == 3
    assert len(res[1]) == 3
    assert np.all(res[0] == np.array([1, 1, 1]))
    assert np.all(res[1] == np.array([123, 123, 3]))


def test_missing_float_val():
    csv_str = '\n12.'

    res = cfastcsv.parse_csv(csv_str, ',', 1, 0, 0, 0, 456.0)[1]

    assert res[0].dtype == float
    assert len(res[0]) == 2
    assert np.all(res[0] == np.array([456.0, 12.0]))


def test_missing_float_val2():
    csv_str = '1\n1\n1,3.0\n'

    res = cfastcsv.parse_csv(csv_str, ',', 1, 0, 0, 0, 456.0)[1]

    assert res[0].dtype == int
    assert res[1].dtype == float
    assert len(res[0]) == 3
    assert len(res[1]) == 3
    assert np.all(res[0] == np.array([1, 1, 1]))
    assert np.all(res[1] == np.array([456.0, 456.0, 3.0]))


def test_nan():
    csv_str = 'nan'

    res = cfastcsv.parse_csv(csv_str, ',', 1, 0, 0, 0, 456.0)[1]

    assert res[0].dtype == float
    assert np.all(np.isnan(res[0]))


def test_exact():
    csv_str = '1.0000000000000007'

    res = cfastcsv.parse_csv(csv_str, ',', 1, 0, 0)[1]

    assert res[0].dtype == float
    assert res[0][0] == 1.0000000000000007


def _do_parse(i):
    v = i
    v, n = _divmod(v, _maxlen)
    n += 1
    s = []
    for j in xrange(n):
        v, k = _divmod(v, len(_chars))
        s.append(_chars[k])

    csv_str = ''.join(s)

    _logger.debug('%r', csv_str)

    _assert_parse_same(csv_str)


def test_combinations():
    n = _maxlen * len(_chars) ** _maxlen
    _logger.info('n = %s', n)
    for i in xrange(n):
        _do_parse(i)
