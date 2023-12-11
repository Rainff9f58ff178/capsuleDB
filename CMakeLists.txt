cmake_minimum_required(VERSION 3.20)
project(STARTD_DB)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_BUILD_TYPE Debug)


add_subdirectory(thirdparty)
add_subdirectory(src)

include_directories(thirdparty)
include_directories(src/logger)
add_executable(main main.cpp)
add_executable(test_buffermanager test_buffermanager.cpp)
add_executable(loggertest loggertest.cpp)

add_executable(test_coro test_coroutine.cpp)

target_link_libraries(main store)
target_link_libraries(test_buffermanager store)
target_link_libraries(loggertest log)
target_link_directories(test_buffermanager PUBLIC ${CMAKE_SOURCE_DIR}/src/Storage)
target_link_directories(loggertest PUBLIC ${CMAKE_SOURCE_DIR}/src/logger)
target_link_libraries(main stard_db)
target_link_libraries(test_coro stard_db)


