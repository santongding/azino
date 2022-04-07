//
// Created by os on 4/6/22.
//

#ifndef AZINO_STORAGE_INCLUDE_UTILS_H
#define AZINO_STORAGE_INCLUDE_UTILS_H
#include "storage.h"
namespace azino {
    namespace storage {
        class MvccUtils{
        private:
            constexpr static char *format="MVCCKEY_%s_%016lx_%c";
            constexpr static int format_common_suffix_length = 18;//the common suffix's length, include timestamp and delete tag
        public:
            enum KeyState{
                Mismatch,
                Deleted,
                OK
            };
            static KeyState GetKeyState(const std::string&internal_key,const std::string &found_key){

                if(internal_key.length()!=found_key.length()||
                    found_key.compare(0,found_key.length()-format_common_suffix_length,internal_key,0,internal_key.length()-format_common_suffix_length)){
                    return Mismatch;
                }else{
                    if(found_key[found_key.size()-1]=='1')return Deleted;
                    else{
                        return OK;
                    }
                }
            }
            static std::string Convert(const std::string &key,uint64_t ts,bool is_delete) {

                static int key_len = strlen(format) + format_common_suffix_length;

                char *buffer = new char[key.length() + key_len];
                sprintf(buffer, format, key.data(), ~ts,
                        is_delete ? '1' : '0');//need to inverse timestamp because leveldb's seek find the bigger one
                auto ans = std::string(buffer);
                delete[]buffer;
                return ans;
            }



        };
    }
}
#endif //AZINO_STORAGE_INCLUDE_UTILS_H
