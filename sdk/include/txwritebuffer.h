#ifndef AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
#define AZINO_SDK_INCLUDE_TXWRITEBUFFER_H

#include "azino/kv.h"
#include "service/kv.pb.h"

#include <butil/macros.h>
#include <unordered_map>

namespace azino {
   class TxWriteBuffer {
   public:
       enum WriteType {
           Pessimistic = 0,
           Optimistic = 1
       };
       struct Write {
           Write(const WriteType t, const Value& v)
                   : type(t), value(v) {}
           WriteType type;
           Value value;
       };

       TxWriteBuffer() = default;
       DISALLOW_COPY_AND_ASSIGN(TxWriteBuffer);
       ~TxWriteBuffer() = default;

       void Write(const UserKey& key, const Value* value, const WriteType type);

       std::__detail::_Node_iterator<std::pair<const std::basic_string<char>, struct Write>, false, true> begin() {
           return _m.begin();
       }

       std::__detail::_Node_iterator<std::pair<const std::basic_string<char>, struct Write>, false, true> end() {
           return _m.end();
       }

   private:
       std::unordered_map<UserKey, struct Write> _m;
   };
}
#endif // AZINO_SDK_INCLUDE_TXWRITEBUFFER_H
