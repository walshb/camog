/*
 * Copyright 2017 Ben Walsh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "fastcsv.h"

static void *
afl_add_column(FastCsvResult *res, int col_type, size_t nrows, size_t width)
{
    void *arr;

    fprintf(stderr, "Adding column.\n");

    switch (col_type) {
    case COL_TYPE_INT:
        return malloc(nrows * sizeof(uint64_t));
    case COL_TYPE_DOUBLE:
        return malloc(nrows * sizeof(double));
    default:
        return malloc(nrows * width);
    }
    return NULL;
}

static int
afl_add_header(FastCsvResult *res, const uchar *str, size_t len)
{
    return 0;
}

static int
afl_parse_csv(const uchar *csv_buf, size_t buf_len, uchar sep, int nthreads,
              int flags, int nheaders)
{
    FastCsvInput input;
    FastCsvResult result;

    input.csv_buf = csv_buf;
    input.buf_len = buf_len;
    input.sep = sep;
    input.nthreads = nthreads;
    input.flags = flags;
    input.nheaders = nheaders;

    result.add_header = &afl_add_header;
    result.add_column = &afl_add_column;

    parse_csv(&input, &result);

    return 0;
}

int main(int argc, char *argv[])
{
    void *filedata;
    int fd;
    struct stat stat_buf;

    if (argc != 2) {
        fprintf(stderr, "Usage: csvread filename\n");
        return 1;
    }

    if ((fd = open(argv[1], O_RDONLY)) < 0) {
        fprintf(stderr, "%s: could not open\n", argv[1]);
        return 1;
    }

    fstat(fd, &stat_buf);

    if ((filedata = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
        close(fd);
        fprintf(stderr, "%s: mmap failed\n", argv[1]);
        return 1;
    }

    afl_parse_csv(filedata, stat_buf.st_size, ',', 2, 0, 1);

    munmap(filedata, stat_buf.st_size);

    close(fd);

    return 0;
}
