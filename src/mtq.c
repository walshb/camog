/*
 * Copyright 2020 Ben Walsh
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

#include <stdlib.h>

#include "mtq.h"

int
queue_reset(JobQueue *q, size_t n) {
#ifdef _WIN32
    EnterCriticalSection(&q->mutex);
#else
    int rc;

    if ((rc = pthread_mutex_lock(&q->mutex)) != 0) {
        return rc;
    }
#endif

    if (n > q->n) {
        q->elems = (void **)realloc(q->elems, n * sizeof(void *));
        q->n = n;
    }

    q->read_idx = 0;
    q->write_idx = 0;

#ifdef _WIN32
    LeaveCriticalSection(&q->mutex);
#else
    if ((rc = pthread_mutex_unlock(&q->mutex)) != 0) {
        return rc;
    }
#endif

    return 0;
}

int
queue_init(JobQueue *q)
{
#ifdef _WIN32
    InitializeCriticalSection(&q->mutex);
    InitializeConditionVariable(&q->cond);
#else
    int rc;

    if ((rc = pthread_mutex_init(&q->mutex, NULL)) != 0) {
        return rc;
    }
    if ((rc = pthread_cond_init(&q->cond, NULL)) != 0) {
        pthread_mutex_destroy(&q->mutex);
        return rc;
    }
#endif

    q->n = 0;
    q->elems = NULL;

    return 0;
}

void *
queue_pop(JobQueue *q)
{
    void *res;

#ifdef _WIN32
    EnterCriticalSection(&q->mutex);
#else
    int rc;
    if ((rc = pthread_mutex_lock(&q->mutex)) != 0) {
        return NULL;
    }
#endif

    while (q->read_idx == q->write_idx) {
#ifdef _WIN32
        if (!SleepConditionVariableCS(&q->cond, &q->mutex, INFINITE)) {
            return NULL;
        }
#else
        if ((rc = pthread_cond_wait(&q->cond, &q->mutex)) != 0) {
            return NULL;
        }
#endif
    }

    res = q->elems[q->read_idx++];

#ifdef _WIN32
    LeaveCriticalSection(&q->mutex);
#else
    if ((rc = pthread_mutex_unlock(&q->mutex)) != 0) {
        return NULL;
    }
#endif

    return res;
}

int
queue_push(JobQueue *q, void *d)
{
#ifdef _WIN32
    EnterCriticalSection(&q->mutex);
#else
    int rc;
    if ((rc = pthread_mutex_lock(&q->mutex)) != 0) {
        return rc;
    }
#endif

    q->elems[q->write_idx++] = d;

#ifdef _WIN32
    LeaveCriticalSection(&q->mutex);
    WakeConditionVariable(&q->cond);
#else
    if ((rc = pthread_mutex_unlock(&q->mutex)) != 0) {
        return rc;
    }
    if ((rc = pthread_cond_signal(&q->cond)) != 0) {
        return rc;
    }
#endif

    return 0;
}
