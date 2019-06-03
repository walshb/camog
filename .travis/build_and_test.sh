#!/bin/sh
#
# Copyright 2019 Ben Walsh
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

set -eux

PYTHON=${PYTHON:-python}

cd $(dirname "$0")/..

TOPDIR=$(/bin/pwd)

clean() {
    cd $TOPDIR

    rm -rf $(find camog -name '*.so' -print) \
       $(find . -name '__pycache__' -print) \
       gensrc build dist .cache ctests/build
}

build_test() {
    clean

    make -C ctests test

    clean

    $PYTHON setup.py build

    PYTHONPATH=$(echo $TOPDIR/build/lib*)
    export PYTHONPATH

    cd $TOPDIR/tests
    $PYTHON -m pytest -sv .
}

CFLAGS='-Wall -Werror -Wsign-compare -Wstrict-prototypes -Wstrict-aliasing=0 -Werror=declaration-after-statement'
export CFLAGS

build_test

CFLAGS="$CFLAGS -DNO_LONG_DOUBLE"
export CFLAGS

build_test

clean
