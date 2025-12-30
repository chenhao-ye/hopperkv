cmake_minimum_required(VERSION 3.15)
project(hopperkv)
set(CMAKE_CXX_STANDARD 20)

option(BUILD_REDIS_MODULE "Build HARE Redis module" ON)
option(BUILD_ALLOC_ENGINE "Build HARE alloc engine" ON)

enable_testing()

if (NOT CMAKE_BUILD_TYPE)
	message(STATUS "No build type selected, default to Release")
	set(CMAKE_BUILD_TYPE "Release")
endif()

if (BUILD_REDIS_MODULE)
	add_subdirectory(hopperkv/redis_module)
endif()

if (BUILD_ALLOC_ENGINE)
	add_subdirectory(hopperkv/alloc/engine)
endif()
