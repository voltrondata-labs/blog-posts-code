
cmake_minimum_required(VERSION 3.14)
project(AdbcSimpleCsvDriver)

set(CMAKE_CXX_STANDARD 11)
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

add_library(
    adbc_simple_csv_driver
    simple_csv_reader.cc
    driver.cc
    nanoarrow.c)
