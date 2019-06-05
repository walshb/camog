#!/bin/sh

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

SWIGVER='3.0.12'

TOPDIR=$(/bin/pwd)

PKGDIR=$TOPDIR/extern_pkgs

mkdir -p $PKGDIR/build

if ! [ -f $PKGDIR/build/swig-${SWIGVER}.tar.gz ]
then
    cd $PKGDIR/build
    curl -LO http://prdownloads.sourceforge.net/swig/swig-${SWIGVER}.tar.gz
fi

if ! [ -d $PKGDIR/swig ]
then
    cd $PKGDIR/build
    tar xf swig-${SWIGVER}.tar.gz
    cd swig-${SWIGVER}
    ./configure --prefix=$PKGDIR/swig
    make
    make install
fi
