# Copyright 2019 Ben Walsh
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

cmake_minimum_required(VERSION 2.8.9)
project(ctests)
find_package(Threads REQUIRED)
add_executable(test_floats test_floats.c
                           ${CMAKE_CURRENT_LIST_DIR}/../src/fastcsv.c
                           ${CMAKE_CURRENT_LIST_DIR}/../src/mtq.c)
include_directories(${CMAKE_CURRENT_LIST_DIR}/../gensrc ${CMAKE_CURRENT_LIST_DIR}/../src)
target_link_libraries(test_floats Threads::Threads)
