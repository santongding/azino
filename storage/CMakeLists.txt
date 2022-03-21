project(azino_storage C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(storage_server ${PROJECT_SOURCE_DIR}/server/main.cpp
                              ${PROJECT_SOURCE_DIR}/leveldb/dbimpl.cpp)

target_link_libraries(storage_server ${BRPC_LIB} ${DYNAMIC_LIB} azino::lib)

# build tests
enable_testing()

add_executable(test_dbimpl  ${PROJECT_SOURCE_DIR}/test/test_dbimpl.cpp
                            ${PROJECT_SOURCE_DIR}/leveldb/dbimpl.cpp)
target_link_libraries(test_dbimpl
                        gtest_main
                        ${BRPC_LIB}
                        ${DYNAMIC_LIB}
                        azino::lib)

include(GoogleTest)
gtest_discover_tests(test_dbimpl)