cmake_minimum_required(VERSION 3.9)
set(CMAKE_CXX_STANDARD 20)

if (NOT DEFINED AWSSDK_PATH)
    set(AWSSDK_PATH ../../lib/aws-sdk-cpp/install)
endif()

if(NOT DEFINED DYNAMO_PATH)
    set(DYNAMO_PATH ${CMAKE_CURRENT_SOURCE_DIR})
endif()

list(APPEND CMAKE_PREFIX_PATH ${AWSSDK_PATH})
find_package(AWSSDK REQUIRED COMPONENTS dynamodb)

add_library(dynamo STATIC ${DYNAMO_PATH}/dynamo.cpp ${DYNAMO_PATH}/dynamo.h)
target_link_libraries(dynamo ${AWSSDK_LINK_LIBRARIES})

option(DYNAMO_BUILD_TEST "Build the test executable" OFF)
if(DYNAMO_BUILD_TEST)
    add_executable(test_dynamo ${DYNAMO_PATH}/test_dynamo.cpp)
    target_link_libraries(test_dynamo dynamo)
endif()
