cmake_minimum_required(VERSION 3.22)
project(MapReduce1)

set(CMAKE_CXX_STANDARD 17)

add_executable(MapReduce
        SampleClient.cpp
        MapReduceFramework.cpp
        Barrier.cpp)
