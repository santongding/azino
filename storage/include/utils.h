//
// Created by os on 4/6/22.
//

#ifndef AZINO_STORAGE_INCLUDE_UTILS_H
#define AZINO_STORAGE_INCLUDE_UTILS_H
#include "storage.h"
#include "service/storage/storage.pb.h"
namespace azino {
    namespace storage {
        const static char format_key[]="MVCCKEY_%s";
        const static char format_ts[]="_%016lx_%c";

        static std::string convert(const std::string key,::google::protobuf::uint64 ts,bool is_delete){

            static auto format = std::string(format_key)+std::string(format_ts);

            char *buffer=new char[key.length()+format.length()+16];
            sprintf(buffer,format.c_str(),key.data(),~ts,is_delete?'1':'0');//need to inverse timestamp because leveldb's seek find the bigger one
            auto ans = std::string(buffer);
            delete[]buffer;

            return ans;
        }
        static std::string convertPrefix(const std::string key){
            static auto format_len = strlen(format_key);

            char *buffer=new char[key.length()+format_len];
            sprintf(buffer,format_key,key.data());
            auto ans = std::string(buffer);
            delete[]buffer;

            return ans;
        }
    }
}
#endif //AZINO_STORAGE_INCLUDE_UTILS_H
