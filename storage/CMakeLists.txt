project(azino_storage C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/service/storage)

add_executable(storage_server ${PROJECT_SOURCE_DIR}/server/main.cpp
                              ${PROJECT_SOURCE_DIR}/leveldb/dbimpl.cpp
                              ${CMAKE_SOURCE_DIR}/service/storage/storage.pb.cc)

target_link_libraries(storage_server ${BRPC_LIB} ${DYNAMIC_LIB})
