
cmake_minimum_required(VERSION 3.20)

set(CMAKE_CXX_STANDARD 20)


set(SRCS BufferManager.cpp )

add_library(store STATIC ${SRCS})

target_link_directories(store PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

