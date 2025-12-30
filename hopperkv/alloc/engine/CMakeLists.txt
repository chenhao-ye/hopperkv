cmake_minimum_required(VERSION 3.4...3.18)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../../lib/pybind11 pybind11)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../lib/spdlog/include)

set(HARE_ALLOC_SOURCE_FILES
  alloc.h
  log.h
  mrc.h
  params.cpp
  params.h
  resrc.h
  tenant.h)

pybind11_add_module(hare_alloc_engine ${HARE_ALLOC_SOURCE_FILES} bind.cpp)

add_executable(test_mrc ${HARE_ALLOC_SOURCE_FILES} tests/test_mrc.cpp)
add_test(NAME test_mrc COMMAND test_mrc)

add_executable(test_alloc ${HARE_ALLOC_SOURCE_FILES} tests/test_alloc.cpp)
add_test(NAME test_alloc COMMAND test_alloc)

set_target_properties(hare_alloc_engine PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
