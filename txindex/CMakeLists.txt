project(azino_txindex C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/simpletxindex/txindeximpl.cpp
                                    )
add_library(azino_txindex::lib ALIAS ${PROJECT_NAME})

add_executable(txindex_server ${PROJECT_SOURCE_DIR}/main.cpp)

target_link_libraries(txindex_server
                        azino::lib
                        azino_txindex::lib
                        ${BRPC_LIB}
                        ${DYNAMIC_LIB}
                        )

# build tests
enable_testing()

add_executable(test_txindeximpl  ${PROJECT_SOURCE_DIR}/test/test_txindeximpl.cpp)
target_link_libraries(test_txindeximpl
                        azino::lib
                        azino_txindex::lib
                        gtest_main
                        ${BRPC_LIB}
                        ${DYNAMIC_LIB})

include(GoogleTest)
gtest_discover_tests(test_txindeximpl)