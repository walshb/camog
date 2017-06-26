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
#include <pthread.h>

#include "osx_pthread_barrier.h"

#include "fastcsv.h"

#include "powers.h"

#define LINKED_MAX 1024

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
    LinkedBuf buf;
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

#define LINKED_STEP(L, P, T, N)                         \
    do {                                                \
        int n = (N) * sizeof(T);                        \
        while ((P) >= (L)->data + LINKED_MAX - n) {     \
            LinkedLink *new_link = (L)->next;           \
            n -= (L)->data + LINKED_MAX - (P);          \
            P = new_link->data;                         \
            L = new_link;                               \
        }                                               \
        P += n;                                         \
    } while (0)

#define COLUMN_INIT(C, R, T)                    \
    do {                                        \
        LINKED_INIT(&(C)->buf, double);         \
        (C)->width = 0;                         \
        (C)->first_row = R;                     \
        (C)->type = T;                          \
    } while (0)

#define CHANGE_TYPE(C, S, T)                                            \
    do {                                                                \
        LinkedLink *link;                                               \
        for (link = (C)->buf.first; link != NULL; link = link->next) {  \
            uchar *vp;                                                  \
            for (vp = link->data; vp < link->ptr; vp += 8) {            \
                *((T *)vp) = (T)*((S *)vp);                             \
            }                                                           \
        }                                                               \
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
    int j;
    for (j = 0; j < chunk->ncols; j++) {
        linked_free(&CHUNK_COLUMN(chunk, j).buf);
    }
    linked_free(&chunk->offset_buf);
    array_buf_free(&chunk->columns);

    return 0;
}

static int
fill_arrays(ThreadCommon *common, Chunk *chunk)
{
    const LinkedLink *offset_link;
    const uchar *offset_ptr;
    const uchar *rowp;
    int col_idx;
    int row_idx;

    if (chunk->nrows == 0) {
        return 0;
    }

    for (col_idx = 0; col_idx < chunk->ncols; col_idx++) {
        const Column *column = &CHUNK_COLUMN(chunk, col_idx);
        uchar *xs = column->arr_ptr;
        const LinkedBuf *linked = &column->buf;
        LinkedLink *link;
        size_t nbytes, col_nbytes;

        if (column->type == COL_TYPE_INT || column->type == COL_TYPE_DOUBLE) {

            col_nbytes = chunk->nrows * sizeof(double);
            if (column->first_row > 0) {
                nbytes = column->first_row * sizeof(double);
                memset(xs, 0, nbytes);
                col_nbytes -= nbytes;
                xs += nbytes;
            }

            link = linked->first;
            if (link != NULL) {
                nbytes = link->ptr - linked->first_data;
                if (nbytes > col_nbytes) {
                    nbytes = col_nbytes;
                }
                memcpy(xs, linked->first_data, nbytes);
                col_nbytes -= nbytes;
                xs += nbytes;
                link = link->next;
                while (link != NULL && col_nbytes > 0) {
                    nbytes = link->ptr - link->data;
                    if (nbytes > col_nbytes) {
                        nbytes = col_nbytes;
                    }
                    memcpy(xs, link->data, nbytes);
                    col_nbytes -= nbytes;
                    xs += nbytes;
                    link = link->next;
                }
            }

            if (col_nbytes > 0) {
                memset(xs, 0, col_nbytes);
            }
        }
    }

    offset_link = chunk->offset_buf.first;
    offset_ptr = chunk->offset_buf.first_data;
    rowp = chunk->buf;
    for (row_idx = 0; row_idx < chunk->nrows; row_idx++) {
        width_t row_n_cols = *((width_t *)offset_ptr);
        int prev_col_idx = 0;
        int str_col_idx;

        for (str_col_idx = 0; str_col_idx < common->n_str_cols; str_col_idx++) {
            const uchar *cellp;
            uchar *dest;
            const uchar *cell_end;
            size_t cell_width;
            const uchar *p;
            uchar *q;
            uchar c;
            Column *column;

            col_idx = common->str_idxs[str_col_idx];
            if (col_idx >= row_n_cols) {
                column = &CHUNK_COLUMN(chunk, col_idx);
                dest = column->arr_ptr + row_idx * column->width;
;
                memset(dest, 0, column->width);
                continue;  /* maybe more empty columns to the right */
            }
            if (col_idx == 0) {
                cellp = rowp;
            } else {
                LINKED_STEP(offset_link, offset_ptr, width_t, col_idx - prev_col_idx);
                cellp = rowp + *((width_t *)offset_ptr);
            }
            LINKED_STEP(offset_link, offset_ptr, width_t, 1);
            cell_end = rowp + *((width_t *)offset_ptr);  /* XXX tidy up */
            cell_width = cell_end - cellp - 1;  /* excluding separator */

#if NUMPY_STRING_OBJECT
            dest = column->arr_ptr + row_idx * sizeof(PyObject *);
            Py_SIZE(*((PyObject **)dest)) = cell_width;
            dest = PyString_AsString(*(PyObject **)dest);
            memcpy(dest, cellp, cell_width);
#else
            column = &CHUNK_COLUMN(chunk, col_idx);
            dest = column->arr_ptr + row_idx * column->width;
            p = cellp;
            q = dest;

            if (cell_width == 0) {
                goto atstringend;
            }

            c = *p++;
            if (c == '"') {
                /* c is opening quote here */
                while (1) {
                    if (p >= (cell_end - 1)) {
                        goto atstringend;
                    }
                    c = *p++;
                    if (c == '"') {
                        if (p >= (cell_end - 1)) {
                            goto atstringend;
                        }
                        c = *p++;
                        if (c != '"') {
                            break;
                        }
                    }
                    *q++ = c;
                }
            }
            /* c is non-quoted char here */
            while (1) {
                *q++ = c;
                if (p >= (cell_end - 1)) {
                    goto atstringend;
                }
                c = *p++;
            }
        atstringend:
            if ((q - dest) < column->width) {
                memset(q, 0, column->width - (q - dest));
            }
#endif

            prev_col_idx = col_idx + 1;
        }
        LINKED_STEP(offset_link, offset_ptr, width_t, row_n_cols - prev_col_idx);
        rowp += *((width_t *)offset_ptr);
        LINKED_STEP(offset_link, offset_ptr, width_t, 1);
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

        col_type = COL_TYPE_INT;
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
        /* make the column the same type in each chunk */
        for (i = 0; i < nchunks; i++) {
            Column *column;
            array_buf_enlarge(&chunks[i].columns, ncols * sizeof(Column));
            column = &CHUNK_COLUMN(&chunks[i], col_idx);
            if (col_idx >= chunks[i].ncols) {
                column->buf.first = NULL;
                column->first_row = 0;
            } else if (column->type == COL_TYPE_INT && col_type == COL_TYPE_DOUBLE) {
                CHANGE_TYPE(column, int64_t, double);
            }
            column->type = col_type;
            column->width = width;
        }

        if (col_type == COL_TYPE_INT) {
            xs = (uchar *)common->result->add_column(common->result, col_type, nrows, 0);
            memset(xs, 1, nrows * sizeof(int64_t));
            for (i = 0; i < nchunks; i++) {
                CHUNK_COLUMN(&chunks[i], col_idx).arr_ptr = xs;
                xs += chunks[i].nrows * sizeof(int64_t);
            }
        } else if (col_type == COL_TYPE_DOUBLE) {
            xs = (uchar *)common->result->add_column(common->result, col_type, nrows, 0);
            memset(xs, 1, nrows * sizeof(double));
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
            memset(xs, 1, nrows * width);
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
        int col_idx;
        for (col_idx = chunks[i].ncols; col_idx < ncols; col_idx++) {
            CHUNK_COLUMN(&chunks[i], col_idx).buf.own_data = 0;
        }
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
    width_t *row_np;

    LINKED_INIT(offset_buf, width_t);

    LINKED_PUT(offset_buf, width_t, 0);
    row_np = &((width_t *)offset_buf->last->ptr)[-1];

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
        if (p >= soft_end) {
            goto finished;
        }
    }

    rowp = p;
    row_idx = 0;
    while (1) {
        /* Start of cell */

        width_t width = 0;
        double val;
        uint64_t value = 0;
        int64_t sign = 1;
        int expo = 0;
        int fracexpo = 0;
        int exposign = 1;
        int nquotes = 0;
        int col_type;
        int digit;
#ifdef WITH_NUMIDX
        size_t numidx;
#endif

        if (col_idx >= ncols) {
            ncols++;
            columns = (Column *)array_buf_enlarge(&chunk->columns, ncols * sizeof(Column));
            COLUMN_INIT(&CHUNK_COLUMN(chunk, col_idx), row_idx, COL_TYPE_INT);
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

        if (nquotes == 1 || (c != sep && c != '\n')) {
            goto bad;
        }

    goodend:
        if (col_type == COL_TYPE_INT) {
            LINKED_PUT(&columns[col_idx].buf, int64_t, value * sign);
        } else {
            expo = expo * exposign - fracexpo;
            if (expo >= 0) {
                if (expo > 309) {
                    expo = 309;
                }
                val = (double)value * powers[expo + 324] * sign;
            } else if (expo < -309) {
                if (expo < -324) {
                    expo = -324;
                }
                val = (double)value * powers[expo + 324] * sign;
            } else {  /* more accurate to divide by precise value */
                val = (double)value / powers[324 - expo] * sign;
            }
            LINKED_PUT(&columns[col_idx].buf, double, val);
        }

        goto comma;

    badend:
        columns[col_idx].type = COL_TYPE_STRING;
        goto comma;

    bad:
        columns[col_idx].type = COL_TYPE_STRING;
        goto atstringbegin;

    parsestring:
        if (c == '"') {
            ++cellp;
            ++nquotes;
        }

    atstringbegin:
        if (nquotes == 1) {
            while (1) {
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
            ++p;
            if (p >= buf_end) {
                break;
            }
            c = *p;
        }

    comma:
        width = (width_t)(p - cellp);
        if (width > columns[col_idx].width) {
            columns[col_idx].width = width;
        }

        if (p >= buf_end) {
            goto athardend;
        }

        LINKED_PUT(offset_buf, width_t, (width_t)(p - rowp + 1));

        if (c == '\n') {
            *row_np = col_idx + 1;  /* ncols */

            for (col_idx++ ; col_idx < ncols; col_idx++) {
                /* ragged right */
                if (columns[col_idx].type == COL_TYPE_INT) {
                    LINKED_PUT(&columns[col_idx].buf, int64_t, 0);
                } else if (columns[col_idx].type == COL_TYPE_DOUBLE) {
                    LINKED_PUT(&columns[col_idx].buf, double, 0.0);
                }
            }

            if (p >= soft_end) {  /* newline (ie. p) >= soft_end */
                /* break out before we get set for new row */
                goto finished;
            }

            col_idx = 0;
            row_idx++;
            rowp = p + 1;
            LINKED_PUT(offset_buf, width_t, 0);
            row_np = &((width_t *)offset_buf->last->ptr)[-1];
        } else {
            col_idx++;
        }
        p++;
    }

 athardend:

    /* pretend separator for consistency */
    LINKED_PUT(offset_buf, width_t, (width_t)(p - rowp + 1));
    *row_np = col_idx + 1;  /* ncols */

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
    LinkedLink **val_links;
    uchar **val_ptrs;
    int col_idx;
    int first_row;
    int i;

    for (i = 1; ; i++) {
        if (i >= common->nchunks) {
            return 0;
        }
        if (common->all_chunks[i].buf < common->all_chunks[i].buf_end
            && common->all_chunks[i - 1].found_end + 1 != common->all_chunks[i].buf) {
            break;
        }
    }

    bigchunk = (Chunk *)malloc(sizeof(Chunk));
    common->bigchunk = bigchunk;

    /* fix everything from chunk[i] onward */
    bigchunk->chunk_idx = i;
    bigchunk->buf = common->all_chunks[i - 1].found_end;
    bigchunk->soft_end = common->all_chunks[i - 1].buf_end;
    bigchunk->buf_end = common->all_chunks[i - 1].buf_end;
    array_buf_init(&bigchunk->columns);

    parse_stage1(common, bigchunk);

    offset_link = bigchunk->offset_buf.first;
    offset_ptr = offset_link->data;
    rowp = bigchunk->buf;

    val_links = (LinkedLink **)malloc(bigchunk->ncols * sizeof(LinkedLink *));
    val_ptrs = (uchar **)malloc(bigchunk->ncols * sizeof(uchar *));
    for (col_idx = 0; col_idx < bigchunk->ncols; col_idx++) {
        val_links[col_idx] = CHUNK_COLUMN(bigchunk, col_idx).buf.first;
        val_ptrs[col_idx] = CHUNK_COLUMN(bigchunk, col_idx).buf.first_data;
    }

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
            width_t row_n_cols = *((width_t *)offset_ptr);
            LINKED_STEP(offset_link, offset_ptr, width_t, row_n_cols);
            rowp += *((width_t *)offset_ptr);
            LINKED_STEP(offset_link, offset_ptr, width_t, 1);
            nrows++;
        }

        chunk->nrows = nrows;

        for (col_idx = 0; col_idx < bigchunk->ncols; col_idx++) {
            const Column *bigcolumn = &CHUNK_COLUMN(bigchunk, col_idx);
            Column *column;

            if (bigcolumn->first_row >= first_row + nrows) {
                break;
            }

            array_buf_enlarge(&chunk->columns, col_idx * sizeof(Column));
            column = &CHUNK_COLUMN(chunk, col_idx);

            if (col_idx < chunk->ncols) {
                linked_free(&column->buf);
                column->buf.own_data = 0;
            }

            column->type = bigcolumn->type;
            column->width = bigcolumn->width;
            if (bigcolumn->first_row > first_row) {
                column->first_row = bigcolumn->first_row - first_row;
            } else {
                column->first_row = 0;
            }

            if (column->type == COL_TYPE_INT || column->type == COL_TYPE_DOUBLE) {
                LinkedLink *val_link = val_links[col_idx];
                uchar *val_ptr = val_ptrs[col_idx];
                size_t inc = (nrows - column->first_row) * sizeof(double);

                column->buf.own_data = 0;
                column->buf.first = val_link;
                column->buf.first_data = val_ptr;
                while (inc >= val_link->data + LINKED_MAX - val_ptr) {
                    inc -= val_link->data + LINKED_MAX - val_ptr;
                    val_link = val_link->next;
                    val_ptr = val_link->data;
                }
                val_ptr += inc;

                val_links[col_idx] = val_link;
                val_ptrs[col_idx] = val_ptr;

                column->buf.last = val_link;
            }

            ncols++;
        }

        for (col_idx = ncols; col_idx < chunk->ncols; col_idx++) {
            Column *column = &CHUNK_COLUMN(chunk, col_idx);
            linked_free(&column->buf);
        }

        chunk->ncols = ncols;
        first_row += nrows;
    }

    free(val_links);
    free(val_ptrs);

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
            *q++ = c;
            if (q >= cellbuf + cell_space) {
                cell_space *= 2;
                new_cellbuf = realloc(cellbuf, cell_space);
                q = new_cellbuf + (q - cellbuf);
                cellbuf = new_cellbuf;
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
