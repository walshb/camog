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

.PHONY:	all clean test benchmark

PYTHON ?= python
CFLAGS ?= -Wall -Werror -Wsign-compare -Wstrict-prototypes -Wstrict-aliasing=0 -Werror=declaration-after-statement

all:
	CFLAGS='$(CFLAGS)' $(PYTHON) setup.py build

parser:
	mkdir -p gensrc
	cd gensrc; $(PYTHON) $(CURDIR)/generator/generate.py

clean:
	cd camog; rm -rf $$(find . -name '*.so')
	rm -rf $$(find . -name '__pycache__' -print) gensrc build dist .cache *.egg-info

test:	all
	export PYTHONPATH=$$(echo $(CURDIR)/build/lib*); cd tests; $(PYTHON) -m pytest -sv test_fastcsv.py test_headers.py test_edge.py test_file.py test_api.py test_chunks.py test_lineends.py test_numbers.py test_format.py

benchmark:	all
	export PYTHONPATH=$$(echo $(CURDIR)/build/lib*); cd benchmarks; ./many_doubles.py --names=camog -n 20000000 --nthreads=4

sdist:
	$(PYTHON) setup.py sdist
	MYTMP=$$(mktemp -d); cd $$MYTMP; tar xvf $(CURDIR)/dist/*.tar.gz; cd camog-*; python setup.py build; rm -r $$MYTMP
