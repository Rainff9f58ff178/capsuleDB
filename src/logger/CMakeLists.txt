cmake_minimum_required(VERSION 3.20)
set(CMAKE_CXX_STANDARD 20)
set(SRCS Logger.cpp )


add_library(log STATIC ${SRCS})

target_link_directories(log PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

