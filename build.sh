#!/bin/bash

CURRENTDIR=$(pwd)

if [ -d "${CURRENTDIR}/third_party/incubator-brpc/output" ];then
    echo "Alread built brpc."
else
    cd ${CURRENTDIR}/third_party/incubator-brpc && sh config_brpc.sh --headers=/usr/include --libs=/usr/lib && make -j"$(nproc)" && cd ${CURRENTDIR}
fi

rm -rf tmp.sqPg7ubHww && rm -rf build && rm -rf cmake-build-debug-
mkdir build && cd build && cmake .. && make -j$(nproc) && cd ..
