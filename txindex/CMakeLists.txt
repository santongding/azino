project(azino_txindex C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(txindex_server ${PROJECT_SOURCE_DIR}/server/main.cpp)

target_link_libraries(txindex_server
                        ${BRPC_LIB}
                        ${DYNAMIC_LIB}
                        azino::lib)