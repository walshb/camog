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
CFLAGS := -Werror -O0 -g3 -I../src -I../gensrc

.PHONY:	clean parser setupfuzz fuzz

csvread: csvread.o fastcsv.o
	$(CC) $^ -lpthread -o $@

csvread.o: csvread.c

fastcsv.o: ../src/fastcsv.c parser
	$(CC) $(CFLAGS) -c -o $@ $<

parser:
	cd ..; make parser

clean:
	rm -f *.o csvread

setupfuzz:
	sudo sh -c 'cd /sys/devices/system/cpu; echo performance | tee cpu*/cpufreq/scaling_governor'
	sudo sh -c 'mkdir -p /mnt/afl; mount -t tmpfs -o size=1G tmpfs /mnt/afl'

fuzz:
	make clean
	make csvread CC=$(AFL_DIR)/afl-gcc
	mkdir -p /mnt/afl/testcase_dir
	/bin/echo -e 'abc,def,ghi\n123,456.0,foo' >/mnt/afl/testcase_dir/test.csv
	$(AFL_DIR)/afl-fuzz -i /mnt/afl/testcase_dir -o /mnt/afl/findings_dir -- $(CURDIR)/csvread @@