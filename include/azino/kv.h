#ifndef AZINO_INCLUDE_KV_H
#define AZINO_INCLUDE_KV_H

#include <string>

namespace azino {
    typedef std::string UserKey;

    class Value {
    public:
        static Value Delete() {
            return Value(true);
        }
        static Value Put(const std::string& s) {
            return Value(false, s);
        }

        Value(const Value& rhs) = default;
        Value& operator=(const Value& rhs) = default;
        Value(Value&& rhs) = default;
        Value& operator=(Value&& rhs) = default;

        ~Value() = default;

        bool IsDelete() const {
            return _delete;
        }

        std::string Content() const {
            return _content;
        }
    private:
        Value(bool is_delete, const std::string& s = "") :
        _delete(is_delete), _content(s) {}
        bool _delete;
        std::string _content;
    };
} // namespace azino

#endif // AZINO_INCLUDE_KV_H
