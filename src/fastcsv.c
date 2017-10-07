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

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <math.h>

#include "osx_pthread_barrier.h"

#include "fastcsv.h"

#include "powers.h"

#define LINKED_MAX 1024

#define TO_DOUBLE(V, D) (((V) & ~((uint64_t)15)) / (D) + ((V) & ((uint64_t)15)) / (D))

typedef uint32_t width_t;

typedef struct linked_link_s {
    uchar *ptr;
    uchar data[LINKED_MAX];
    struct linked_link_s *next;
} LinkedLink;

typedef struct {
    LinkedLink *first;
    LinkedLink *last;
    int own_data;
    uchar *first_data;
} LinkedBuf;

typedef struct {
    size_t len;
    uchar *data;
} ArrayBuf;

typedef struct {
    width_t width;
    int first_row;
    int type;
    uchar *arr_ptr;
} Column;

typedef struct {
    int chunk_idx;
    const uchar *buf;
    const uchar *buf_end;
    const uchar *soft_end;
    const uchar *found_end;
    ArrayBuf columns;
    LinkedBuf offset_buf;
    int ncols;
    int nrows;
} Chunk;

typedef struct {
    int nchunks;
    Chunk *all_chunks;
    Chunk *bigchunk;  /* for quoted string fixup */
    pthread_barrier_t barrier1;
    pthread_barrier_t barrier2;
    int flags;
    int *str_idxs;
    int n_str_cols;
    FastCsvResult *result;
    uchar sep;
    int64_t missing_int_val;
    double missing_float_val;
} ThreadCommon;

typedef struct {
    Chunk *chunk;
    ThreadCommon *common;
} ThreadData;

#define MAXLINE 256

#define CHUNK_COLUMN(C, I) ((Column *)((C)->columns.data))[I]

#define NEXTCHAR_NOQUOTES(L) \
    do {                     \
        ++p;                 \
        if (p >= buf_end) {  \
            goto L;          \
        }                    \
        c = *p;              \
    } while (0)

#define NEXTCHAR_INQUOTES(L)                    \
    do {                                        \
        ++p;                                    \
        if (p >= buf_end) {                     \
            goto L;                             \
        }                                       \
        c = *p;                                 \
        if (c == '"' && nquotes == 1) {         \
            ++cellp;                            \
            ++p;                                \
            if (p >= buf_end) {                 \
                goto L;                         \
            }                                   \
            c = *p;                             \
            if (c != '"') {                     \
                ++nquotes;                      \
            }                                   \
        }                                       \
    } while (0)

#define NEXTCHAR2_INQUOTES(L) NEXTCHAR_INQUOTES(L)

#define NEXTCHAR2_NOQUOTES(L) NEXTCHAR_NOQUOTES(L)

#define LINKED_INIT(B, T)                                               \
    do {                                                                \
        LinkedLink *link = (LinkedLink *)malloc(sizeof(LinkedLink));    \
        link->ptr = link->data;                                         \
        link->next = NULL;                                              \
        (B)->own_data = 1;                                              \
        (B)->first = (B)->last = link;                                  \
        (B)->first_data = link->data;                                   \
    } while (0)

#define LINKED_PUT(B, T, D)                                             \
    do {                                                                \
        LinkedLink *link = (B)->last;                                   \
        if (link->ptr >= link->data + LINKED_MAX) {                     \
            LinkedLink *new_link = (LinkedLink *)malloc(sizeof(LinkedLink)); \
            new_link->ptr = new_link->data;                             \
            new_link->next = NULL;                                      \
            (B)->last = link->next = new_link;                          \
            link = new_link;                                            \
        }                                                               \
        *((T *)link->ptr) = D;                                          \
        link->ptr += sizeof(T);                                         \
    } while (0)

#define LINKED_NEXT(L, P, T)                    \
    do {                                        \
        P += sizeof(T);                         \
        if ((P) >= (L)->data + LINKED_MAX) {    \
            L = (L)->next;                      \
            P = (L)->data;                      \
        }                                       \
    } while (0)

#define COLUMN_INIT(C, R, T)                    \
    do {                                        \
        (C)->width = 0;                         \
        (C)->first_row = R;                     \
        (C)->type = T;                          \
    } while (0)

int
array_buf_init(ArrayBuf *arr_buf)
{
    arr_buf->len = 256 * sizeof(Column);
    arr_buf->data = (uchar *)malloc(arr_buf->len);
    return 0;
}

int array_buf_free(ArrayBuf *arr_buf)
{
    free(arr_buf->data);
    return 0;
}

uchar *
array_buf_enlarge(ArrayBuf *arr_buf, size_t n)
{
    if (arr_buf->len <= n) {
        arr_buf->len *= 2;  /* assume only adding small number */
        arr_buf->data = (uchar *)realloc(arr_buf->data, arr_buf->len);
    }

    return arr_buf->data;
}

static int
linked_free(LinkedBuf *linked) {
    LinkedLink *link;

    if (!linked->own_data) {
        return 0;
    }
    link = linked->first;
    while (link != NULL) {
        LinkedLink *next_link = link->next;
        free(link);
        link = next_link;
    }
    return 0;
}

static int
chunk_free(Chunk *chunk) {
    linked_free(&chunk->offset_buf);
    array_buf_free(&chunk->columns);

    return 0;
}

static int
fill_arrays(ThreadCommon *common, Chunk *chunk)
{
    const uchar *p;
    const uchar *buf_end;
    int col_idx;
    int row_idx;
    const uchar sep = common->sep;

    if (chunk->nrows == 0) {
        return 0;
    }

    buf_end = chunk->buf_end;
    p = chunk->buf;
    row_idx = 0;
    col_idx = 0;
    for (row_idx = 0; row_idx < chunk->nrows; ) {
        int nquotes = 0;
        const uchar *cellp;
        int col_type;
        uchar c;
        Column *column = &CHUNK_COLUMN(chunk, col_idx);
        double val;
        uchar *dest, *q;
        int digit;
        int64_t value = 0;
        int expo = 0;
        int fracexpo = 0;
        int exposign = 1;

        if (p >= buf_end) {
            goto comma;
        }

        cellp = p;

        col_type = column->type;

        c = *p;

        if (col_type != COL_TYPE_STRING) {
            if (c == '"') {
                ++nquotes;
                ++cellp;
                NEXTCHAR2_INQUOTES(goodend);

#include "parser2_inquotes.h"

            } else {

#include "parser2.h"

            }

            if (c == '\r') {
                NEXTCHAR2_INQUOTES(goodend);
            }

        goodend:

            if (col_type == COL_TYPE_INT32) {
                dest = column->arr_ptr + row_idx * sizeof(int32_t);
                if (p == cellp || fracexpo != 0) {
                    value = common->missing_int_val;
                }
                *((int32_t *)dest) = value;
            } else if (col_type == COL_TYPE_INT64) {
                dest = column->arr_ptr + row_idx * sizeof(int64_t);
                if (p == cellp || fracexpo != 0) {
                    value = common->missing_int_val;
                }
                *((int64_t *)dest) = value;
            } else {
                dest = column->arr_ptr + row_idx * sizeof(double);
                if (p == cellp) {
                    val = common->missing_float_val;
                } else if (expo == INT_MIN) {
                    val = 0.0 / 0.0;  /* NaN */
                } else {
                    expo = expo * exposign - fracexpo;
                    if (expo >= 0) {
                        if (expo > 309) {
                            expo = 309;
                        }
                        val = (long double)value * powers[expo];
                    } else {
                        /* more accurate to divide by precise value */
                        expo = -expo;
                        if (expo > 308) {
                            if (expo > 340) {
                                expo = 340;
                            }
                            val = (long double)value / 1.0e308 / powers[expo - 308];
                        } else {
                            val = (long double)value / powers[expo];
                        }
                    }
                }
                *((double *)dest) = val;
            }
        } else {  /* string */

            dest = column->arr_ptr + row_idx * column->width;
            q = dest;

            if (c == '"') {
                ++nquotes;
                ++cellp;
                /* c is opening quote here */
                while (1) {
                    ++p;
                    if (p >= buf_end) {
                        goto atstringend;
                    }
                    c = *p;
                    if (c == '\r') {
                        continue;
                    }
                    if (c == '"') {
                        ++p;
                        if (p >= buf_end) {
                            goto atstringend;
                        }
                        c = *p;
                        if (c != '"') {
                            break;
                        }
                    }
                    *q++ = c;
                }
            }
            /* c is non-quoted char here */
            while (1) {
                if (c == sep || c == '\n') {
                    goto atstringend;
                }
                if (c != '\r') {
                    *q++ = c;
                }
                ++p;
                if (p >= buf_end) {
                    goto atstringend;
                }
                c = *p;
            }
        atstringend:
            if ((q - dest) < column->width) {
                memset(q, 0, column->width - (q - dest));
            }
        }

    bad:
    badend:
    comma:

        if (p >= buf_end || c == '\n') {
            for (col_idx++; col_idx < chunk->ncols; col_idx++) {
                Column *column = &CHUNK_COLUMN(chunk, col_idx);
                int col_type = column->type;
                uchar *dest;
                if (col_type == COL_TYPE_INT32) {
                    dest = column->arr_ptr + row_idx * sizeof(int32_t);
                    *((int32_t *)dest) = common->missing_int_val;
                } else if (col_type == COL_TYPE_INT64) {
                    dest = column->arr_ptr + row_idx * sizeof(int64_t);
                    *((int64_t *)dest) = common->missing_int_val;
                } else if (col_type == COL_TYPE_DOUBLE) {
                    dest = column->arr_ptr + row_idx * sizeof(double);
                    *((double *)dest) = common->missing_float_val;
                } else {
                    dest = column->arr_ptr + row_idx * column->width;
                    memset(dest, 0, column->width);
                }
            }
            col_idx = 0;
            row_idx++;
        } else {
            col_idx++;
        }
        p++;
    }

    return 0;
}

static int
allocate_arrays(ThreadCommon *common)
{
    Chunk *chunks = common->all_chunks;
    int nchunks = common->nchunks;
    int i;
    int col_idx;
    int ncols = 0;
    int nrows = 0;
    int n_str_cols = 0;

    for (i = 0; i < nchunks; i++) {
        if (chunks[i].ncols > ncols) {
            ncols = chunks[i].ncols;
        }
        nrows += chunks[i].nrows;
    }

    common->str_idxs = (int *)malloc(ncols * sizeof(int));

    for (col_idx = 0; col_idx < ncols; col_idx++) {
        uchar *xs;
        int col_type;
        width_t width;

        col_type = COL_TYPE_INT32;
        width = 1;  /* numpy has minimum string len of 1 */
        for (i = 0; i < nchunks; i++) {
            Column *column;
            if (col_idx >= chunks[i].ncols) {
                continue;
            }
            column = &CHUNK_COLUMN(&chunks[i], col_idx);
            if (column->type > col_type) {  /* "supertype" */
                col_type = column->type;
            }
            if (column->width > width) {
                width = column->width;
            }
        }

        if (common->result->fix_column_type != NULL) {
            col_type = common->result->fix_column_type(common->result, col_idx, col_type);
        }

        /* make the column the same type in each chunk */
        for (i = 0; i < nchunks; i++) {
            Column *column;
            array_buf_enlarge(&chunks[i].columns, ncols * sizeof(Column));
            column = &CHUNK_COLUMN(&chunks[i], col_idx);
            if (col_idx >= chunks[i].ncols) {
                column->first_row = 0;
            }
            column->type = col_type;
            column->width = width;
        }

        if (col_type == COL_TYPE_INT32) {
            xs = (uchar *)common->result->add_column(common->result, col_type, nrows, 0);
            for (i = 0; i < nchunks; i++) {
                CHUNK_COLUMN(&chunks[i], col_idx).arr_ptr = xs;
                xs += chunks[i].nrows * sizeof(int32_t);
            }
        } if (col_type == COL_TYPE_INT64) {
            xs = (uchar *)common->result->add_column(common->result, col_type, nrows, 0);
            for (i = 0; i < nchunks; i++) {
                CHUNK_COLUMN(&chunks[i], col_idx).arr_ptr = xs;
                xs += chunks[i].nrows * sizeof(int64_t);
            }
        } else if (col_type == COL_TYPE_DOUBLE) {
            xs = (uchar *)common->result->add_column(common->result, col_type, nrows, 0);
            for (i = 0; i < nchunks; i++) {
                CHUNK_COLUMN(&chunks[i], col_idx).arr_ptr = xs;
                xs += chunks[i].nrows * sizeof(double);
            }
        } else if (col_type == COL_TYPE_STRING) {
#if NUMPY_STRING_OBJECT
            arr = PyArray_SimpleNew(1, dims, NPY_OBJECT);
            xs = (uchar *)PyArray_DATA((PyArrayObject *)arr);
            for (i = 0; i < nrows; i++) {
                /* PyString_FromStringAndSize is not thread-safe. */
                ((PyObject **)xs)[i] = PyString_FromStringAndSize(NULL, width);
            }
            for (i = 0; i < nchunks; i++) {
                CHUNK_COLUMN(&chunks[i], col_idx).arr_ptr = xs;
                xs += chunks[i].nrows * sizeof(PyObject *);
            }
#else
            xs = (uchar *)common->result->add_column(common->result, col_type, nrows, width);
            for (i = 0; i < nchunks; i++) {
                CHUNK_COLUMN(&chunks[i], col_idx).arr_ptr = xs;
                xs += chunks[i].nrows * width;
            }
#endif
            common->str_idxs[n_str_cols] = col_idx;
            n_str_cols++;
        }
    }

    /* Give each chunk the same number of columns */
    for (i = 0; i < nchunks; i++) {
        chunks[i].ncols = ncols;
    }

    common->n_str_cols = n_str_cols;

    return 0;
}

static int
parse_stage1(ThreadCommon *common, Chunk *chunk)
{
    const uchar *buf = chunk->buf;
    const uchar *buf_end = chunk->buf_end;
    const uchar *soft_end = chunk->soft_end;  /* can go past this in middle of line */
    const uchar sep = common->sep;
    LinkedBuf *offset_buf = &chunk->offset_buf;
    Column *columns = &CHUNK_COLUMN(chunk, 0);
    const uchar *p = buf;
    const uchar *cellp = NULL;
    const uchar *rowp = buf;
    int col_idx = 0, row_idx = -1;  /* currently not parsing a row */
    int ncols = 0;
    uchar c = 0;

    LINKED_INIT(offset_buf, width_t);

    if (chunk->chunk_idx > 0) {
        while (1) {
            if (p >= buf_end) {
                chunk->buf = p;
                goto finished;  /* just exit nicely */
            }
            c = *p++;
            if (c == '\n') {
                break;
            }
        }
        chunk->buf = p;
        if (p > soft_end) {  /* newline (ie. p-1) >= soft_end */
            goto finished;
        }
    }

    rowp = p;
    row_idx = 0;
    while (1) {
        /* Start of cell */

        width_t width = 0;
        int nquotes = 0;
        int col_type;
        int digit;
#ifdef WITH_NUMIDX
        size_t numidx;
#endif

        if (col_idx >= ncols) {
            ncols++;
            columns = (Column *)array_buf_enlarge(&chunk->columns, ncols * sizeof(Column));
            COLUMN_INIT(&CHUNK_COLUMN(chunk, col_idx), row_idx, COL_TYPE_INT64);
        }
        if (p >= buf_end) {
            goto athardend;
        }

        cellp = p;

        col_type = CHUNK_COLUMN(chunk, col_idx).type;

        c = *p;

        if (col_type == COL_TYPE_STRING) {
            goto parsestring;
        }

        if (c == '"') {
            ++nquotes;
            ++cellp;
            if (!(common->flags & FLAG_EXCEL_QUOTES)) {
                goto bad;
            }
            NEXTCHAR_INQUOTES(goodend);

#include "parser_inquotes.h"

        } else {

#include "parser.h"

        }

        if (nquotes != 1) {  /* c not in quotes */
            if (c == '\r') {
                ++cellp;  /* make width smaller */
                NEXTCHAR_NOQUOTES(goodend);
            }

            if (c == sep || c == '\n') {
                goto comma;
            }
        }
    bad:
        columns[col_idx].type = COL_TYPE_STRING;
        goto atstringbegin;

    badend:
        columns[col_idx].type = COL_TYPE_STRING;
        goto comma;

    parsestring:
        if (c == '"') {
            ++cellp;
            ++nquotes;
        }

    atstringbegin:
        if (nquotes == 1) {
            while (1) {
                if (c == '\r') {
                    ++cellp;  /* make width smaller */
                }
                ++p;
                if (p >= buf_end) {
                    goto comma;
                }
                c = *p;
                if (c == '"') {
                    ++cellp;
                    ++p;
                    if (p >= buf_end) {
                        goto comma;
                    }
                    c = *p;
                    if (c != '"') {
                        ++nquotes;
                        break;
                    }
                }
            }
        }

        /* c is first char of string here */
        while (1) {
            if (c == sep || c == '\n') {
                break;
            }
            if (c == '\r') {
                ++cellp;  /* make width smaller */
            }
            ++p;
            if (p >= buf_end) {
                break;
            }
            c = *p;
        }

    goodend:
    comma:
        width = (width_t)(p - cellp);
        if (width > columns[col_idx].width) {
            columns[col_idx].width = width;
        }

        if (p >= buf_end) {
            goto athardend;
        }

        if (c == '\n') {
            LINKED_PUT(offset_buf, width_t, (width_t)(p - rowp + 1));

            if (p >= soft_end) {  /* newline (ie. p) >= soft_end */
                /* break out before we get set for new row */
                goto finished;
            }

            col_idx = 0;
            row_idx++;
            rowp = p + 1;
        } else {
            col_idx++;
        }
        p++;
    }

 athardend:

    /* pretend separator for consistency */
    LINKED_PUT(offset_buf, width_t, (width_t)(p - rowp + 1));

    /* empty last line? */
    if (p == rowp) {
        row_idx--;
    }

 finished:

    chunk->ncols = ncols;
    chunk->nrows = row_idx + 1;
    chunk->found_end = p;

    return 0;
}

static int
fixup_parse(ThreadCommon *common)
{
    Chunk *bigchunk;
    LinkedLink *offset_link;
    uchar *offset_ptr;
    const uchar *rowp;
    int first_row;
    int i, previ;

    previ = 0;
    for (i = 1; ; i++) {
        if (i >= common->nchunks) {
            return 0;
        }
        if (common->all_chunks[i].nrows == 0) {
            continue;  /* empty chunk */
        }
        if (common->all_chunks[previ].found_end + 1 != common->all_chunks[i].buf) {
            break;
        }
        previ = i;
    }

    bigchunk = (Chunk *)malloc(sizeof(Chunk));
    common->bigchunk = bigchunk;

    /* fix everything from chunk[i] onward */
    bigchunk->chunk_idx = i;
    bigchunk->buf = common->all_chunks[previ].found_end;
    bigchunk->soft_end = common->all_chunks[previ].buf_end;
    bigchunk->buf_end = common->all_chunks[previ].buf_end;
    array_buf_init(&bigchunk->columns);

    parse_stage1(common, bigchunk);

    offset_link = bigchunk->offset_buf.first;
    offset_ptr = offset_link->data;
    rowp = bigchunk->buf;

    first_row = 0;
    for ( ; i < common->nchunks; i++) {
        Chunk *chunk = &common->all_chunks[i];
        int nrows = 0, ncols = 0;
        int col_idx;

        linked_free(&chunk->offset_buf);

        chunk->buf = rowp;
        chunk->offset_buf.own_data = 0;
        chunk->offset_buf.first = offset_link;
        chunk->offset_buf.first_data = offset_ptr;

        while (rowp < chunk->soft_end) {
            rowp += *((width_t *)offset_ptr);
            LINKED_NEXT(offset_link, offset_ptr, width_t);
            nrows++;
        }

        chunk->nrows = nrows;

        for (col_idx = 0; col_idx < bigchunk->ncols; col_idx++) {
            Column *bigcolumn = &CHUNK_COLUMN(bigchunk, col_idx);
            Column *column;

            if (bigcolumn->first_row >= first_row + nrows) {
                break;
            }

            array_buf_enlarge(&chunk->columns, (col_idx + 1) * sizeof(Column));

            column = &CHUNK_COLUMN(chunk, col_idx);

            column->type = bigcolumn->type;
            column->width = bigcolumn->width;
            if (bigcolumn->first_row > first_row) {
                column->first_row = bigcolumn->first_row - first_row;
            } else {
                column->first_row = 0;
            }

            ncols++;
        }

        chunk->ncols = ncols;
        first_row += nrows;
    }

    return 0;
}

static void *
parse_thread(void *data)
{
    ThreadData *thread_data = (ThreadData *)data;

    ThreadCommon *common = thread_data->common;

#ifndef DEBUG_SERIAL
    Chunk *chunk = thread_data->chunk;
    parse_stage1(common, chunk);
#ifdef DEBUG_COUNT
    return NULL;
#endif
#else
    int i;
#endif

    if (pthread_barrier_wait(&common->barrier1) == PTHREAD_BARRIER_SERIAL_THREAD) {
        /* single-threaded here */
#ifdef DEBUG_SERIAL
        for (i = 0; i < common->nchunks; i++) {
            parse_stage1(common, &common->all_chunks[i]);
        }
#ifdef DEBUG_COUNT
        return NULL;
#endif
#endif
        fixup_parse(common);
        allocate_arrays(common);
#ifdef DEBUG_SERIAL
        for (i = 0; i < common->nchunks; i++) {
            fill_arrays(common, &common->all_chunks[i]);
        }
#endif
    }

    pthread_barrier_wait(&common->barrier2);

#ifndef DEBUG_SERIAL
    fill_arrays(common, chunk);
#endif

    return NULL;
}

static const uchar *
parse_headers(ThreadCommon *common, const uchar *csv_buf, const uchar *buf_end)
{
    const uchar *p = csv_buf;
    uchar *cellbuf, *new_cellbuf, *q;
    size_t cell_space;
    uchar sep = common->sep;
    uchar c = 0;

    cell_space = 256;
    cellbuf = (uchar *)malloc(cell_space);

    while (c != '\n') {
        q = cellbuf;
        if (p >= buf_end) {
            goto atstringend;
        }
        c = *p;
        if (c == '"') {
            /* c is opening quote here */
            while (1) {
                ++p;
                if (p >= buf_end) {
                    goto atstringend;
                }
                c = *p;
                if (c == '\r') {
                    continue;
                }
                if (c == '"') {
                    ++p;
                    if (p >= buf_end) {
                        goto atstringend;
                    }
                    c = *p;
                    if (c != '"') {
                        break;
                    }
                }
                *q++ = c;
                if (q >= cellbuf + cell_space) {
                    cell_space *= 2;
                    new_cellbuf = realloc(cellbuf, cell_space);
                    q = new_cellbuf + (q - cellbuf);
                    cellbuf = new_cellbuf;
                }
            }
        }
        /* c is non-quoted char here */
        while (c != sep && c != '\n') {
            if (c != '\r') {
                *q++ = c;
                if (q >= cellbuf + cell_space) {
                    cell_space *= 2;
                    new_cellbuf = realloc(cellbuf, cell_space);
                    q = new_cellbuf + (q - cellbuf);
                    cellbuf = new_cellbuf;
                }
            }
            ++p;
            if (p >= buf_end) {
                goto atstringend;
            }
            c = *p;
        }
    atstringend:
        common->result->add_header(common->result, cellbuf, q - cellbuf);
        if (p >= buf_end) {
            break;
        }
        ++p;
    }

    free(cellbuf);

    return p;
}

int
init_csv(FastCsvInput *input, const uchar *csv_buf, size_t buf_len,
         int nheaders, int nthreads)
{
    input->csv_buf = csv_buf;
    input->buf_len = buf_len;
    input->nheaders = nheaders;
    input->nthreads = nthreads;
    input->sep = ',';
    input->flags = 0;
    input->missing_int_val = 0;
    input->missing_float_val = NAN;

    return 0;
}

int
parse_csv(const FastCsvInput *input, FastCsvResult *res)
{
    size_t buf_len = input->buf_len;
    int nthreads = input->nthreads;
    const uchar *buf_end;
    const uchar *data_begin;
    int i;
    Chunk *chunks;
    ThreadData *thread_datas;
    ThreadCommon common;
    pthread_t *threads;

    buf_end = input->csv_buf + buf_len;

    chunks = (Chunk *)malloc(nthreads * sizeof(Chunk));
    common.nchunks = nthreads;
    common.all_chunks = chunks;
    common.bigchunk = NULL;
    common.str_idxs = NULL;
    common.n_str_cols = 0;
    common.flags = input->flags;
    common.sep = input->sep;
    common.result = res;
    common.missing_int_val = input->missing_int_val;
    common.missing_float_val = input->missing_float_val;
    pthread_barrier_init(&common.barrier1, NULL, nthreads);
    pthread_barrier_init(&common.barrier2, NULL, nthreads);

    if (input->nheaders) {
        data_begin = parse_headers(&common, input->csv_buf, buf_end);
        buf_len = buf_end - data_begin;
    } else {
        data_begin = input->csv_buf;
    }

    thread_datas = (ThreadData *)malloc(nthreads * sizeof(ThreadData));
    for (i = 0; i < nthreads; i++) {
        const uchar *chunk_buf = data_begin + buf_len * i / nthreads;
        const uchar *chunk_end = data_begin + buf_len * (i + 1) / nthreads;
        if (chunk_end > buf_end) {
            chunk_end = buf_end;
        }

        chunks[i].chunk_idx = i;
        chunks[i].buf = chunk_buf;
        chunks[i].soft_end = chunk_end;
        chunks[i].buf_end = buf_end;
        array_buf_init(&chunks[i].columns);

        thread_datas[i].chunk = &chunks[i];
        thread_datas[i].common = &common;
    }

    threads = (pthread_t *)malloc(nthreads * sizeof(pthread_t));
    for (i = 0; i < nthreads; i++) {
        pthread_create(&threads[i], NULL, parse_thread, (void *)&thread_datas[i]);
    }

    for (i = 0; i < nthreads; i++) {
        pthread_join(threads[i], NULL);
    }

    /* free */
    free(threads);
    free(thread_datas);
    for (i = 0; i < common.nchunks; i++) {
        chunk_free(&chunks[i]);
    }
    if (common.bigchunk != NULL) {
        chunk_free(common.bigchunk);
        free(common.bigchunk);
    }
    free(chunks);
    free(common.str_idxs);

    return 0;
}
