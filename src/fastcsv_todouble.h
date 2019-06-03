/*
 * Copyright 2019 Ben Walsh
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

#ifndef INC_FASTCSV_TODOUBLE
#define INC_FASTCSV_TODOUBLE

#include <stdlib.h>

#ifdef NO_LONG_DOUBLE

#include "powers5.h"

#define FASTCSV_TODOUBLE(s, m, e, v)                            \
    do {                                                        \
        int64_t m2, r, alo, ahi, blo, bhi;                      \
        int c, lz;                                              \
        e = (e > 309) ? 309 : (e < -340) ? -340 : e;            \
        r = pow5[e + 340];                                      \
        c = shift5[e + 340];                                    \
        lz = __builtin_clzl(m) - 2;                             \
        m2 = m << lz;                                           \
        c -= lz;                                                \
        alo = m2 & 0x7fffffffL;                                 \
        ahi = m2 >> 31;                                         \
        blo = r & 0x7fffffffL;                                  \
        bhi = r >> 31;                                          \
        m2 = ahi * bhi + ((alo * bhi + ahi * blo) >> 31);       \
        if (c > -1023) {                                        \
            v = (double)(s * m2) * pow2[c + 1022];              \
        } else {                                                \
            v = (double)(s * m2) * pow2[0] * pow2[c + 2044];    \
        }                                                       \
    } while (0)

#else

#include "powers.h"

#define FASTCSV_TODOUBLE(s, m, e, v)                                    \
    do {                                                                \
        if (e >= 0) {                                                   \
            if (e > 309) {                                              \
                e = 309;                                                \
            }                                                           \
            v = (long double)(s * m) * powers[e];                       \
        } else {                                                        \
            e = -e;                                                     \
            if (e > 308) {                                              \
                if (e > 340) {                                          \
                    e = 340;                                            \
                }                                                       \
                v = (long double)(s * m) / 1.0e308 / powers[e - 308];   \
            } else {                                                    \
                v = (long double)(s * m) / powers[e];                   \
            }                                                           \
        }                                                               \
    } while (0)

#endif

#endif
