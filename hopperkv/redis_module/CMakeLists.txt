cmake_minimum_required(VERSION 3.9)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

set(RM_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/RedisModulesSDK)
set(RMHEADER_PATH ${RM_PATH}/7.0)
set(RMUTIL_DIR ${RM_PATH}/rmutil)
set(RMUTIL_LIB ${RMUTIL_DIR}/librmutil.a)
set(GCACHE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/gcache/include)
set(DYNAMO_PATH ${CMAKE_CURRENT_SOURCE_DIR}/dynamo)
set(AWSSDK_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/aws-sdk-cpp/install)

include_directories(${RMHEADER_PATH})
include_directories(${GCACHE_PATH})
include_directories(${DYNAMO_PATH})

include(${DYNAMO_PATH}/CMakeLists.txt)

# Build rmutil.a using custom command so ninja knows how to generate it
add_custom_command(
    OUTPUT ${RMUTIL_LIB}
    COMMAND make
    WORKING_DIRECTORY ${RMUTIL_DIR}
    COMMENT "Building rmutil static library"
)

add_custom_target(rmutil_build DEPENDS ${RMUTIL_LIB})

# Register rmutil as an IMPORTED static library
add_library(rmutil STATIC IMPORTED GLOBAL)
set_target_properties(rmutil PROPERTIES
    IMPORTED_LOCATION ${RMUTIL_LIB}
)

# Build hopper_redis_module
add_library(hopper_redis_module MODULE
	barrier.cpp
	barrier.h
	config.cpp
	config.h
	get.cpp
	get.h
	ghost.cpp
	ghost.h
	inflight.cpp
	inflight.h
	module.cpp
	module.h
	network.cpp
	network.h
	rate.h
	resrc.cpp
	resrc.h
	set.cpp
	set.h
	stats.cpp
	stats.h
	storage.cpp
	storage.h
	task.h
	${RMHEADER_PATH}/redismodule.h
)

add_dependencies(hopper_redis_module rmutil_build)
target_link_libraries(hopper_redis_module rmutil dynamo)

# Output the Redis module `.so` to the current directory for python import
set_target_properties(hopper_redis_module PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
