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

logging.basicConfig(level=logging.DEBUG)

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
    assert _equal(_parse(s, True), _py_csv_parse(s, True))
    assert _equal(_parse(s, False), _py_csv_parse(s, False))


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
    _logger.debug('n = %s', n)
    for i in xrange(n):
        _do_parse(i)
