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

CC := gcc
CFLAGS := -DDEBUG_SERIAL -O2 -Werror -I../src -I../gensrc

.PHONY:	all clean parser

all: build/test_floats

build/test_floats: build/test_floats.o build/fastcsv.o
	$(CC) $^ -lpthread -o $@

build/fastcsv.o: ../src/fastcsv.c parser build
	$(CC) $(CFLAGS) -c -o $@ $<

build/%.o: %.c build
	$(CC) $(CFLAGS) -c -o $@ $<

parser:
	cd ..; make parser

build:
	mkdir build

clean:
	rm -rf build
