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

DESTDIR=$(/bin/pwd)

cd $(dirname "$0")

PYTHON=${PYTHON:-python}

$PYTHON ./powers.py >$DESTDIR/powers.h
$PYTHON ./parser.py --type-stage --nextchar=NEXTCHAR_NOQUOTES --label-prefix=noquotes --outfilename=$DESTDIR/parser.h
$PYTHON ./parser.py --type-stage --nextchar=NEXTCHAR_INQUOTES --label-prefix=inquotes --outfilename=$DESTDIR/parser_inquotes.h
$PYTHON ./parser.py --nextchar=NEXTCHAR2_NOQUOTES --label-prefix=noquotes --outfilename=$DESTDIR/parser2.h
$PYTHON ./parser.py --nextchar=NEXTCHAR2_INQUOTES --label-prefix=inquotes --outfilename=$DESTDIR/parser2_inquotes.h
