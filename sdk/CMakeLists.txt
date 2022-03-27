project(azino_sdk C CXX)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_library(${PROJECT_NAME} STATIC ${PROJECT_SOURCE_DIR}/src/client.cpp
                                   ${PROJECT_SOURCE_DIR}/src/txopslog.cpp)
add_library(azino_sdk::lib ALIAS ${PROJECT_NAME})

target_link_libraries(azino_sdk
        azino::lib
        ${BRPC_LIB}
        ${DYNAMIC_LIB})