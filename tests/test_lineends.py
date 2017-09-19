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

def _do_parse_both(csv_str, nheaders=0, excel_quotes=False):
    return cfastcsv.parse_csv(csv_str, ',', 1, 1 if excel_quotes else 0, nheaders)


def test_number_cr_quotes():
    csv_str = ('123,456,789\r\n'
               '"123\r","456\r","78\r9"\n')

    headers, data = _do_parse_both(csv_str, nheaders=0, excel_quotes=True)

    assert np.all(data[0] == th.array(['123', '123']))
    assert np.all(data[1] == th.array(['456', '456']))
    assert np.all(data[2] == th.array(['789', '789']))


def test_number_cr_quotes():
    csv_str = ('123,456,789\r\n'
               '"123\r","456\r","78\r9"\n')

    headers, data = _do_parse_both(csv_str, nheaders=0, excel_quotes=True)

    assert data[0].dtype == '|S3'
    assert data[1].dtype == '|S3'
    assert data[2].dtype == '|S3'

    assert np.all(data[0] == th.array(['123', '123']))
    assert np.all(data[1] == th.array(['456', '456']))
    assert np.all(data[2] == th.array(['789', '789']))


def test_many_crs():
    csv_str = ('123,456,789\r\n'
               '"123\r\r\r","456\r\r\r","78\r\r\r9"\n'
               '123\r\r\r,456\r\r\r,78\r\r\r9\n')

    headers, data = _do_parse_both(csv_str, nheaders=0, excel_quotes=True)

    assert data[0].dtype == '|S3'
    assert data[1].dtype == '|S3'
    assert data[2].dtype == '|S3'

    assert np.all(data[0] == th.array(['123', '123', '123']))
    assert np.all(data[1] == th.array(['456', '456', '456']))
    assert np.all(data[2] == th.array(['789', '789', '789']))


def test_many_crs_noquotes():
    csv_str = ('123,456,789\r\n'
               '123\r\r\r,456\r\r\r,78\r\r\r9\n')

    headers, data = _do_parse_both(csv_str, nheaders=0, excel_quotes=True)

    assert data[0].dtype == '|S3'
    assert data[1].dtype == '|S3'
    assert data[2].dtype == '|S3'

    assert np.all(data[0] == th.array(['123', '123']))
    assert np.all(data[1] == th.array(['456', '456']))
    assert np.all(data[2] == th.array(['789', '789']))


def test_many_crs_headers():
    csv_str = ('\r\rabc\r\r,"\r\r,def,\r\r","ghi"\r\n'
               '123,456,789\r\n'
               '123\r\r\r,456\r\r\r,78\r\r\r9\n')

    headers, data = _do_parse_both(csv_str, nheaders=1, excel_quotes=True)

    assert headers == ['abc', ',def,', 'ghi']

    assert data[0].dtype == '|S3'
    assert data[1].dtype == '|S3'
    assert data[2].dtype == '|S3'

    assert np.all(data[0] == th.array(['123', '123']))
    assert np.all(data[1] == th.array(['456', '456']))
    assert np.all(data[2] == th.array(['789', '789']))
