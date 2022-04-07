//
// Created by os on 4/6/22.
//

#ifndef AZINO_STORAGE_INCLUDE_UTILS_H
#define AZINO_STORAGE_INCLUDE_UTILS_H
#include "storage.h"
namespace azino {
    namespace storage {
        class MvccUtils{
        public:
            enum KeyState{
                Mismatch,
                Deleted,
                OK
            };
            static KeyState GetKeyState(const std::string&origin_key,const std::string &found_key){
                auto prefix = convertPrefix(origin_key);
                if(found_key.compare(0, prefix.length(),prefix)){
                    return Mismatch;
                }else{
                    if(found_key[found_key.size()-1]=='1')return Deleted;
                    else{
                        return OK;
                    }
                }
            }
            static std::string Convert(const std::string &key,uint64_t ts,bool is_delete){

                static auto format = std::string(format_key)+std::string(format_ts);

                char *buffer=new char[key.length()+format.length()+16];
                sprintf(buffer,format.c_str(),key.data(),~ts,is_delete?'1':'0');//need to inverse timestamp because leveldb's seek find the bigger one
                auto ans = std::string(buffer);
                delete[]buffer;

                return ans;
            }

        private:
            static std::string convertPrefix(const std::string &key){
                static auto format_len = strlen(format_key);

                char *buffer=new char[key.length()+format_len];
                sprintf(buffer,format_key,key.data());
                auto ans = std::string(buffer);
                delete[]buffer;

                return ans;
            }
            const static char format_key[];
            const static char format_ts[];
        };
        const char MvccConverter::format_key[]="MVCCKEY_%s";
        const char format_ts[]="_%016lx_%c";
    }
}
#endif //AZINO_STORAGE_INCLUDE_UTILS_H
