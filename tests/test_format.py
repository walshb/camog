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

import camog

_logger = logging.getLogger(__name__)

_maxlen = 6
_chars = 'a1 ",\n'


def _divmod(n, m):
    return n // m, n % m


def _parse_py_csv(csv_str):
    fp = cStringIO.StringIO(csv_str)
    reader = csv.reader(fp, dialect='excel')
    rows = list(reader)

    ncols = max([len(row) for row in rows])

    cols = [[] for i in xrange(ncols)]
    for row in rows:
        for j in xrange(ncols):
            if j < len(row):
                cols[j].append(row[j])
            else:
                cols[j].append('')

    return cols


def _to_ints(strs):
    return [0 if s == ' ' * len(s) else int(s) for s in strs]


def _cols_equal(cols1, cols2):
    if len(cols1) != len(cols2):
        return False

    for col1, col2 in zip(cols1, cols2):
        if col1.dtype == int:
            if not np.allclose(col1, np.array(_to_ints(col2))):
                return False
        else:
            if not np.all(col1 == np.array(col2)):
                return False

    return True


def _do_parse(i):
    v = i
    v, n = _divmod(v, _maxlen)
    n += 1
    s = []
    for j in xrange(n):
        v, k = _divmod(v, len(_chars))
        s.append(_chars[k])

    csv_str = ''.join(s) + '\n'

    _logger.debug('%r', csv_str)

    res = camog.parse_csv(csv_str, ',', 2)[1]

    py_res = _parse_py_csv(csv_str)

    _logger.debug('%r => %r vs %r', csv_str, res, py_res)

    if csv_str != '\n' * len(csv_str):
        assert _cols_equal(res, py_res)


def test_combinations():
    n = _maxlen * len(_chars) ** _maxlen
    _logger.debug('n = %s', n)
    for i in xrange(n):
        _do_parse(i)
