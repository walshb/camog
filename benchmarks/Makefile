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

.PHONY: run build

PYTHON ?= python

run:	build
	PYTHONVER=$$($(PYTHON) -c 'import sys; print(sys.version[:3])'); cd ..; export PYTHONPATH=$$(/bin/pwd)/$$(echo build/lib*-$${PYTHONVER}):$$(/bin/pwd)/extern_pkgs/lib/python$${PYTHONVER}/site-packages; $(PYTHON) $(CURDIR)/many_doubles.py -n 20000000 --nthreads=4

build:
	PYTHON=$(PYTHON) ./build.sh
