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

.PHONY:	all clean setup install test testdoc valgrind

all:	setup
	cd build; R CMD build camog
	cd build; R CMD check -l $(CURDIR)/build/libs/ camog_*.tar.gz

clean:
	rm -rf build

setup:
	mkdir -p build/libs
	cp -rv camog build/
	cp ../../src/fastcsv.c ../../src/*.h build/camog/src/
	cp ../../LICENSE build/camog/
	cd build/camog/src; $(CURDIR)/../../generator/generate.py

test:
	R_LIBS_USER=$(CURDIR)/build/libs Rscript test.R

testdoc:
	R_LIBS_USER=$(CURDIR)/build/libs Rscript testdoc.R | cat

valgrind:
	make clean
	make 'LTO=-O0 -g3'
	R_LIBS_USER=$(CURDIR)/build/libs R -d "valgrind --tool=memcheck --leak-check=full" --vanilla <test.R
