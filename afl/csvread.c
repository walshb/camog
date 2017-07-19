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
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "fastcsv.h"

#define BUF_SIZE (8 * 1024 * 1024)

typedef struct {
    FastCsvResult r;
    void *buf;
    void *buf_last;
    ssize_t nrows;
    int nthreads;
} AflFastCsvResult;

static void *
afl_add_column(FastCsvResult *res, int col_type, size_t nrows, size_t width)
{
    void *arr;
    AflFastCsvResult *aflres = (AflFastCsvResult *)res;

    if (aflres->nrows >= 0 && nrows != aflres->nrows) {
        fprintf(stderr, "nrows changed from %zd to %zu\n", aflres->nrows, nrows);
        abort();
    }

    switch (col_type) {
    case COL_TYPE_INT:
        width = sizeof(uint64_t);
        break;
    case COL_TYPE_DOUBLE:
        width = sizeof(double);
        break;
    }

    if ((aflres->buf_last - aflres->buf + nrows * width) > BUF_SIZE) {
        fprintf(stderr, "Past buf limit.\n");
        exit(1);
    }

    fprintf(stderr, "nthreads %d adding column %zd * %zd\n",
            aflres->nthreads, nrows, width);

    arr = aflres->buf_last;
    aflres->buf_last += nrows * width;

    return arr;
}

static int
afl_add_header(FastCsvResult *res, const uchar *str, size_t len)
{
    return 0;
}

static AflFastCsvResult *
afl_parse_csv(const uchar *csv_buf, size_t buf_len, uchar sep, int nthreads,
              int flags, int nheaders, AflFastCsvResult *result)
{
    FastCsvInput input;

    input.csv_buf = csv_buf;
    input.buf_len = buf_len;
    input.sep = sep;
    input.nthreads = nthreads;
    input.flags = flags;
    input.nheaders = nheaders;

    result->r.add_header = &afl_add_header;
    result->r.add_column = &afl_add_column;
    result->buf = malloc(BUF_SIZE);
    result->buf_last = result->buf;
    result->nrows = -1;
    result->nthreads = nthreads;

    parse_csv(&input, (FastCsvResult *)result);

    return result;
}

int main(int argc, char *argv[])
{
    void *filedata;
    int fd;
    struct stat stat_buf;
    AflFastCsvResult result1, result2;
    size_t len1, len2;

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

    afl_parse_csv(filedata, stat_buf.st_size, ',', 3, 0, 1, &result1);

    afl_parse_csv(filedata, stat_buf.st_size, ',', 1, 0, 1, &result2);

    len1 = result1.buf_last - result1.buf;
    len2 = result2.buf_last - result2.buf;
    if (len1 != len2 || memcmp(result1.buf, result2.buf, len1) != 0) {
        fprintf(stderr, "len1 = %zd len2 = %zd\n", len1, len2);
        fprintf(stderr, "different result with threads\n");
        abort();
    }

    munmap(filedata, stat_buf.st_size);

    close(fd);

    return 0;
}
