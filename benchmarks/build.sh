#!/bin/sh
#
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

set -eux

MYDIR=$(dirname $(readlink -f "$0"))

TOPDIR=$MYDIR/..

PYTHON=${PYTHON:-python}

PYTHONVER=$($PYTHON -c 'import sys; print(sys.version[:3])')

PKGDIR=$TOPDIR/extern_pkgs

mkdir -p $PKGDIR/build

PYTHONPATH=$PKGDIR/lib/python${PYTHONVER}/site-packages
export PYTHONPATH

if ! [ -d $PKGDIR/build/paratext ]
then
    cd $PKGDIR/build
    git clone https://github.com/wiseio/paratext
fi

cd /
if ! $PYTHON -c 'import paratext'
then
    cd $PKGDIR/build/paratext/python
    which swig
    # fix bug
    sed -e 's|splitunc|splitdrive|g' -i paratext/core.py
    $PYTHON setup.py build install --prefix=$PKGDIR
fi

if ! [ -d $PKGDIR/build/pandas ]
then
    cd $PKGDIR/build
    git clone https://github.com/pandas-dev/pandas
fi

cd /
if ! $PYTHON -c 'import pandas'
then
    cd $PKGDIR/build/pandas
    $PYTHON setup.py build install --prefix=$PKGDIR
fi
