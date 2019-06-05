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

#if defined(__APPLE__) || defined(_WIN32)

#ifndef OSX_PTHREAD_BARRIER_H
#define OSX_PTHREAD_BARRIER_H

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#define OSX_PTHREAD_BARRIER_SERIAL_THREAD -1

typedef struct {
#ifdef _WIN32
    CRITICAL_SECTION mutex;
    CONDITION_VARIABLE cond;
#else
    pthread_mutex_t mutex;
    pthread_cond_t cond;
#endif
    int phase;
    int counter;
    int trigger;
} osx_pthread_barrier_t;

typedef struct {
    int dummy;
} osx_pthread_barrier_attr_t;

int osx_pthread_barrier_destroy(osx_pthread_barrier_t *barrier)
{
#ifdef _WIN32
    DeleteCriticalSection(&barrier->mutex);
#else
    int rc;

    if ((rc = pthread_cond_destroy(&barrier->cond)) < 0) {
        return rc;
    }
    if ((rc = pthread_mutex_destroy(&barrier->mutex)) < 0) {
        return rc;
    }
#endif
    return 0;
}

int osx_pthread_barrier_init(osx_pthread_barrier_t *barrier,
                             const osx_pthread_barrier_attr_t *attr,
                             unsigned int count)
{
#ifdef _WIN32
    InitializeCriticalSection(&barrier->mutex);
    InitializeConditionVariable(&barrier->cond);
#else
    int rc;

    if ((rc = pthread_mutex_init(&barrier->mutex, NULL)) < 0) {
        return rc;
    }
    if ((rc = pthread_cond_init(&barrier->cond, NULL)) < 0) {
        pthread_mutex_destroy(&barrier->mutex);
        return rc;
    }
#endif

    barrier->phase = 0;
    barrier->counter = 0;
    barrier->trigger = count;

    return 0;
}

int osx_pthread_barrier_wait(osx_pthread_barrier_t *barrier)
{
    int serial;

#ifdef _WIN32
    EnterCriticalSection(&barrier->mutex);
#else
    int rc;

    if ((rc = pthread_mutex_lock(&barrier->mutex)) < 0) {
        return rc;
    }
#endif

    ++barrier->counter;
    if (barrier->counter >= barrier->trigger) {
        barrier->phase ^= 1;
        barrier->counter = 0;
#ifdef _WIN32
        WakeAllConditionVariable(&barrier->cond);
#else
        if ((rc = pthread_cond_broadcast(&barrier->cond)) < 0) {
            return rc;
        }
#endif
        serial = OSX_PTHREAD_BARRIER_SERIAL_THREAD;
    } else {
        int phase = barrier->phase;
        while (barrier->phase == phase) {
#ifdef _WIN32
            if (!SleepConditionVariableCS(&barrier->cond, &barrier->mutex, INFINITE)) {
                return OSX_PTHREAD_BARRIER_SERIAL_THREAD - 1;  /* another -ve number */
            }
#else
            if ((rc = pthread_cond_wait(&barrier->cond, &barrier->mutex)) < 0) {
                return rc;
            }
#endif
        }
        serial = 0;
    }

#ifdef _WIN32
    LeaveCriticalSection(&barrier->mutex);
#else
    if ((rc = pthread_mutex_unlock(&barrier->mutex)) < 0) {
        return rc;
    }
#endif

    return serial;
}

#define PTHREAD_BARRIER_SERIAL_THREAD OSX_PTHREAD_BARRIER_SERIAL_THREAD
#define pthread_barrier_t osx_pthread_barrier_t
#define pthread_barrier_attr_t osx_pthread_barrier_attr_t
#define pthread_barrier_init(B, A, C) osx_pthread_barrier_init(B, A, C)
#define pthread_barrier_destroy(B) osx_pthread_barrier_destroy(B)
#define pthread_barrier_wait(B) osx_pthread_barrier_wait(B)

#endif  /* OSX_PTHREAD_BARRIER_H */

#endif  /* __APPLE__ */
