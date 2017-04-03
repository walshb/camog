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

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "Python.h"
#include "math.h"
#include "numpy/arrayobject.h"

#include "osx_pthread_barrier.h"

#include "powers.h"

#define LINKED_MAX 1024

#define COL_TYPE_INT 1
#define COL_TYPE_DOUBLE 2
#define COL_TYPE_STRING 3

#define FLAG_EXCEL_QUOTES 1

#define NUMPY_STRING_OBJECT 0

typedef uint32_t width_t;

typedef struct linked_link_s {
    char *ptr;
    char data[LINKED_MAX];
    struct linked_link_s *next;
} LinkedLink;

typedef struct {
    LinkedLink *first;
    LinkedLink *last;
    int own_data;
    char *first_data;
} LinkedBuf;

typedef struct {
    LinkedBuf buf;
    width_t width;
    int first_row;
    int type;
} Column;

typedef struct {
    int chunk_idx;
    const char *buf;
    const char *buf_end;
    const char *soft_end;
    Column columns[256];
    LinkedBuf offset_buf;
    int ncols;
    int nrows;
    char *arr_ptrs[256];
} Chunk;

typedef struct {
    int nchunks;
    Chunk *all_chunks;
    Chunk *bigchunk;  /* for quoted string fixup */
    pthread_barrier_t barrier1;
    pthread_barrier_t barrier2;
    int flags;
    int str_idxs[256];
    int n_str_cols;
    PyObject *headers;
    PyObject *result;
    char sep;
} ThreadCommon;

typedef struct {
    Chunk *chunk;
    ThreadCommon *common;
} ThreadData;

#define MAXLINE 256

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

#define LINKED_STEP(L, P, T, N)                                         \
    do {                                                                \
        if ((P) >= (L)->data + LINKED_MAX - (N) * sizeof(T)) {          \
            LinkedLink *new_link = (L)->next;                           \
            P = new_link->data + ((P) - (L)->data + (N) * sizeof(T) - LINKED_MAX); \
            L = new_link;                                               \
        } else {                                                        \
            P += (N) * sizeof(T);                                       \
        }                                                               \
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
            char *vp;                                                   \
            for (vp = link->data; vp < link->ptr; vp += 8) {            \
                *((T *)vp) = (T)*((S *)vp);                             \
            }                                                           \
        }                                                               \
    } while (0)

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
        linked_free(&chunk->columns[j].buf);
    }
    linked_free(&chunk->offset_buf);

    return 0;
}

static int
fill_arrays(ThreadCommon *common, Chunk *chunk)
{
    const LinkedLink *offset_link;
    const char *offset_ptr;
    const char *rowp;
    int col_idx;
    int row_idx;

    if (chunk->nrows == 0) {
        return 0;
    }

    for (col_idx = 0; col_idx < chunk->ncols; col_idx++) {
        const Column *column = &chunk->columns[col_idx];
        char *xs = chunk->arr_ptrs[col_idx];
        const LinkedBuf *linked = &column->buf;
        LinkedLink *link;
        size_t nbytes;

        if (column->type == COL_TYPE_INT || column->type == COL_TYPE_DOUBLE) {

            if (column->first_row > 0) {
                nbytes = column->first_row * sizeof(double);
                memset(xs, 0, nbytes);
                xs += nbytes;
            }

            link = linked->first;
            if (link != NULL) {
                nbytes = link->ptr - linked->first_data;
                memcpy(xs, linked->first_data, nbytes);
                xs += nbytes;
                link = link->next;
                while (link != NULL) {
                    nbytes = link->ptr - link->data;
                    memcpy(xs, link->data, nbytes);
                    xs += nbytes;
                    link = link->next;
                }
            }

            nbytes = chunk->arr_ptrs[col_idx] + chunk->nrows * sizeof(double) - xs;
            if (nbytes > 0) {
                memset(xs, 0, nbytes);
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
            const char *cellp;
            char *dest;
            const char *cell_end;
            size_t cell_width;
            const char *p;
            char *q;
            char c;

            col_idx = common->str_idxs[str_col_idx];
            if (col_idx >= row_n_cols) {
                dest = chunk->arr_ptrs[col_idx] + row_idx * chunk->columns[col_idx].width;
                memset(dest, 0, chunk->columns[col_idx].width);
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
            dest = chunk->arr_ptrs[col_idx] + row_idx * sizeof(PyObject *);
            Py_SIZE(*((PyObject **)dest)) = cell_width;
            dest = PyString_AsString(*(PyObject **)dest);
            memcpy(dest, cellp, cell_width);
#else
            dest = chunk->arr_ptrs[col_idx] + row_idx * chunk->columns[col_idx].width;
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
            if ((q - dest) < chunk->columns[col_idx].width) {
                memset(q, 0, chunk->columns[col_idx].width - (q - dest));
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

    common->result = PyList_New(ncols);

    for (col_idx = 0; col_idx < ncols; col_idx++) {
        char *xs;
        int col_type;
        width_t width;
        PyObject *arr = NULL;
        npy_intp dims[1];

        col_type = COL_TYPE_INT;
        width = 1;  /* numpy has minimum string len of 1 */
        for (i = 0; i < nchunks; i++) {
            if (col_idx >= chunks[i].ncols) {
                continue;
            }
            if (chunks[i].columns[col_idx].type > col_type) {  /* "supertype" */
                col_type = chunks[i].columns[col_idx].type;
            }
            if (chunks[i].columns[col_idx].width > width) {
                width = chunks[i].columns[col_idx].width;
            }
        }
        /* make the column the same type in each chunk */
        for (i = 0; i < nchunks; i++) {
            Column *column = &chunks[i].columns[col_idx];
            if (col_idx >= chunks[i].ncols) {
                column->buf.first = NULL;
                column->first_row = 0;
            }
            if (column->type == COL_TYPE_INT && col_type == COL_TYPE_DOUBLE) {
                CHANGE_TYPE(column, int64_t, double);
            }
            column->type = col_type;
            column->width = width;
        }

        dims[0] = nrows;

        if (col_type == COL_TYPE_INT) {
            arr = PyArray_SimpleNew(1, dims, NPY_INT64);
            xs = (char *)PyArray_DATA((PyArrayObject *)arr);
            memset(xs, 1, nrows * sizeof(int64_t));
            for (i = 0; i < nchunks; i++) {
                chunks[i].arr_ptrs[col_idx] = xs;
                xs += chunks[i].nrows * sizeof(int64_t);
            }
        } else if (col_type == COL_TYPE_DOUBLE) {
            arr = PyArray_SimpleNew(1, dims, NPY_DOUBLE);
            xs = (char *)PyArray_DATA((PyArrayObject *)arr);
            memset(xs, 1, nrows * sizeof(double));
            for (i = 0; i < nchunks; i++) {
                chunks[i].arr_ptrs[col_idx] = xs;
                xs += chunks[i].nrows * sizeof(double);
            }
        } else if (col_type == COL_TYPE_STRING) {
#if NUMPY_STRING_OBJECT
            arr = PyArray_SimpleNew(1, dims, NPY_OBJECT);
            xs = (char *)PyArray_DATA((PyArrayObject *)arr);
            for (i = 0; i < nrows; i++) {
                /* PyString_FromStringAndSize is not thread-safe. */
                ((PyObject **)xs)[i] = PyString_FromStringAndSize(NULL, width);
            }
            for (i = 0; i < nchunks; i++) {
                chunks[i].arr_ptrs[col_idx] = xs;
                xs += chunks[i].nrows * sizeof(PyObject *);
            }
#else
            arr = PyArray_New(&PyArray_Type, 1, dims, NPY_STRING, NULL, NULL, width, 0, NULL);
            xs = (char *)PyArray_DATA((PyArrayObject *)arr);
            memset(xs, 1, nrows * width);
            for (i = 0; i < nchunks; i++) {
                chunks[i].arr_ptrs[col_idx] = xs;
                xs += chunks[i].nrows * width;
            }
#endif
            common->str_idxs[n_str_cols] = col_idx;
            n_str_cols++;
        }
        PyList_SET_ITEM(common->result, col_idx, arr);  /* steals ref */
    }

    /* Give each chunk the same number of columns */
    for (i = 0; i < nchunks; i++) {
        int col_idx;
        for (col_idx = chunks[i].ncols; col_idx < ncols; col_idx++) {
            chunks[i].columns[col_idx].buf.own_data = 0;
        }
        chunks[i].ncols = ncols;
    }

    common->n_str_cols = n_str_cols;

    return 0;
}

static int
parse_stage1(ThreadCommon *common, Chunk *chunk)
{
    const char *buf = chunk->buf;
    const char *buf_end = chunk->buf_end;
    const char *soft_end = chunk->soft_end;  /* can go past this in middle of line */
    const char sep = common->sep;
    LinkedBuf *offset_buf = &chunk->offset_buf;
    Column *columns = chunk->columns;
    const char *p = buf;
    const char *cellp = NULL;
    const char *rowp = buf;
    int col_idx = 0, row_idx = -1;  /* currently not parsing a row */
    int ncols = 0;
    char c = 0;
    width_t *row_np;

    LINKED_INIT(offset_buf, width_t);

    LINKED_PUT(offset_buf, width_t, 0);
    row_np = &((width_t *)offset_buf->last->ptr)[-1];

    if (chunk->chunk_idx > 0) {
        while (1) {
            if (p >= buf_end) {
                chunk->buf = p;
                goto athardend;
            }
            c = *p++;
            if (c == '\n') {
                break;
            }
        }
        chunk->buf = p;
        if (p >= soft_end) {
            goto atsoftend;
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

        if (p >= buf_end) {
            goto athardend;
        }
        if (col_idx >= ncols) {
            ncols++;
            COLUMN_INIT(&columns[col_idx], row_idx, COL_TYPE_INT);
        }
        cellp = p;

        col_type = columns[col_idx].type;

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
                if (expo > 308) {
                    expo = 308;
                }
                val = (double)value * powers[expo + 324] * sign;
            } else {
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
            /* pretend separator for consistency */
            LINKED_PUT(offset_buf, width_t, (width_t)(p - rowp + 1));
            *row_np = col_idx + 1;  /* ncols */
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
                goto atsoftend;
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

    /* empty last line? */
    if (p == rowp) {
        row_idx--;
    }

 atsoftend:

    chunk->ncols = ncols;
    chunk->nrows = row_idx + 1;
    chunk->soft_end = p;

    return 0;
}

static int
fixup_parse(ThreadCommon *common)
{
    Chunk *bigchunk;
    LinkedLink *offset_link;
    char *offset_ptr;
    const char *rowp;
    LinkedLink *val_links[256];
    char *val_ptrs[256];
    int col_idx;
    int first_row;
    int i;

    for (i = 1; ; i++) {
        if (i >= common->nchunks) {
            return 0;
        }
        if (common->all_chunks[i].buf < common->all_chunks[i].buf_end
            && common->all_chunks[i - 1].soft_end + 1 != common->all_chunks[i].buf) {
            break;
        }
    }

    bigchunk = (Chunk *)malloc(sizeof(Chunk));
    common->bigchunk = bigchunk;

    /* fix everything from chunk[i] onward */
    bigchunk->chunk_idx = i;
    bigchunk->buf = common->all_chunks[i - 1].soft_end;
    bigchunk->soft_end = common->all_chunks[i - 1].buf_end;
    bigchunk->buf_end = common->all_chunks[i - 1].buf_end;

    parse_stage1(common, bigchunk);

    offset_link = bigchunk->offset_buf.first;
    offset_ptr = offset_link->data;
    rowp = bigchunk->buf;

    for (col_idx = 0; col_idx < bigchunk->ncols; col_idx++) {
        val_links[col_idx] = bigchunk->columns[col_idx].buf.first;
        val_ptrs[col_idx] = bigchunk->columns[col_idx].buf.first_data;
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
            const Column *bigcolumn = &bigchunk->columns[col_idx];
            Column *column = &chunk->columns[col_idx];

            if (col_idx < chunk->ncols) {
                linked_free(&column->buf);
                column->buf.own_data = 0;
            }

            column->type = bigcolumn->type;
            column->width = bigcolumn->width;
            if (bigcolumn->first_row >= first_row + nrows) {
                break;
            } else if (bigcolumn->first_row > first_row) {
                column->first_row = bigcolumn->first_row - first_row;
            } else {
                column->first_row = 0;
            }

            if (column->type == COL_TYPE_INT || column->type == COL_TYPE_DOUBLE) {
                LinkedLink *val_link = val_links[col_idx];
                char *val_ptr = val_ptrs[col_idx];
                size_t inc = nrows * sizeof(double);

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

        for (col_idx = chunk->ncols; col_idx < ncols; col_idx++) {
            chunk->columns[col_idx].buf.own_data = 0;
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

static const char *
parse_headers(ThreadCommon *common, const char *csv_buf, const char *buf_end)
{
    const char *p = csv_buf;
    char *cellbuf, *new_cellbuf, *q;
    size_t cell_space;
    char sep = common->sep;
    PyObject *str_obj;
    char c = 0;

    PyObject *res = PyList_New(0);
    common->headers = res;

    cell_space = 256;
    cellbuf = (char *)malloc(cell_space);

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
        str_obj = PyString_FromStringAndSize(cellbuf, q - cellbuf);
        PyList_Append(res, str_obj);  /* increfs */
        Py_DECREF(str_obj);
        if (p >= buf_end) {
            break;
        }
        ++p;
    }

    free(cellbuf);

    return p;
}

static PyObject *
parse_csv(const char *csv_buf, size_t buf_len, char sep, int nthreads, int flags, int nheaders)
{
    const char *buf_end;
    const char *data_begin;
    int i;
    Chunk *chunks;
    ThreadData *thread_datas;
    ThreadCommon common;
    pthread_t *threads;
    PyObject *res_obj;

    buf_end = csv_buf + buf_len;

    chunks = (Chunk *)malloc(nthreads * sizeof(Chunk));
    common.nchunks = nthreads;
    common.all_chunks = chunks;
    common.bigchunk = NULL;
    common.n_str_cols = 0;
    common.flags = flags;
    common.sep = sep;
    pthread_barrier_init(&common.barrier1, NULL, nthreads);
    pthread_barrier_init(&common.barrier2, NULL, nthreads);

    if (nheaders) {
        data_begin = parse_headers(&common, csv_buf, buf_end);
        buf_len = buf_end - data_begin;
    } else {
        Py_INCREF(Py_None);
        common.headers = Py_None;
        data_begin = csv_buf;
    }

    thread_datas = (ThreadData *)malloc(nthreads * sizeof(ThreadData));
    for (i = 0; i < nthreads; i++) {
        const char *chunk_buf = data_begin + buf_len * i / nthreads;
        const char *chunk_end = data_begin + buf_len * (i + 1) / nthreads;
        if (chunk_end > buf_end) {
            chunk_end = buf_end;
        }

        chunks[i].chunk_idx = i;
        chunks[i].buf = chunk_buf;
        chunks[i].soft_end = chunk_end;
        chunks[i].buf_end = buf_end;

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

#ifdef DEBUG_COUNT
    Py_INCREF(Py_None);
    return Py_None;
#else

    res_obj = PyTuple_New(2);
    PyTuple_SET_ITEM(res_obj, 0, common.headers);
    PyTuple_SET_ITEM(res_obj, 1, common.result);

    return res_obj;
#endif
}

static PyObject *
parse_csv_func(PyObject *self, PyObject *args)
{
    PyObject *str_obj, *sep_obj = NULL;
    const char *csv_buf;
    char sep;
    size_t buf_len;
    int nthreads = 4;
    int flags = 0;
    int nheaders = 0;

    if (!PyArg_ParseTuple(args, "O|Oiii", &str_obj, &sep_obj, &nthreads, &flags, &nheaders)) {
        return NULL;
    }

    if (sep_obj == NULL) {
        sep = ',';
    } else {
        sep = PyString_AsString(sep_obj)[0];
    }
    csv_buf = PyString_AsString(str_obj);
    buf_len = PyString_Size(str_obj);

    return parse_csv(csv_buf, buf_len, sep, nthreads, flags, nheaders);
}

static PyObject *
parse_file_func(PyObject *self, PyObject *args)
{
    PyObject *fname_obj;
    PyObject *sep_obj = NULL;
    PyObject *res;
    void *filedata;
    const char *fname;
    char sep;
    int nthreads = 4;
    int flags = 0;
    int nheaders = 0;
    int fd;
    struct stat stat_buf;

    if (!PyArg_ParseTuple(args, "O|Oiii", &fname_obj, &sep_obj, &nthreads, &flags, &nheaders)) {
        return NULL;
    }

    if (sep_obj == NULL) {
        sep = ',';
    } else {
        sep = PyString_AsString(sep_obj)[0];
    }

    fname = PyString_AsString(fname_obj);
    if ((fd = open(fname, O_RDONLY)) < 0) {
        return PyErr_Format(PyExc_IOError, "%s: could not open", fname);
    }

    fstat(fd, &stat_buf);

    if ((filedata = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) == MAP_FAILED) {
        close(fd);
        return PyErr_Format(PyExc_IOError, "%s: mmap failed", fname);
    }

    res = parse_csv((const char *)filedata, stat_buf.st_size, sep, nthreads, flags, nheaders);

    munmap(filedata, stat_buf.st_size);

    close(fd);

    return res;
}

static PyMethodDef mod_methods[] = {
    {"parse_csv", (PyCFunction)parse_csv_func, METH_VARARGS,
     "Parse csv"},
    {"parse_file", (PyCFunction)parse_file_func, METH_VARARGS,
     "Parse csv file"},
    {NULL}  /* Sentinel */
};


#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
init_cfastcsv(void)
{
    PyObject* m;

    import_array();

    m = Py_InitModule3("camog._cfastcsv", mod_methods,
                       "Example module that creates an extension type.");

    (void)m;  /* avoid warning */
}
