#ifndef AZINO_INCLUDE_STATUS_H
#define AZINO_INCLUDE_STATUS_H

#include <string>

namespace azino {
    class Status {
    public:
        ~Status() = default;
        Status static Ok(const std::string& s = "") {
            return Status(kOk, s);
        }
        Status static NotFound(const std::string& s = "") {
            return Status(kNotFound, s);
        }
        Status static NetworkErr(const std::string& s = "") {
            return Status(kNetworkErr, s);
        }
        Status static IllegalTxOp(const std::string& s = "") {
            return Status(kIllegalTxOp, s);
        }
        bool IsOk() {
            return _error_code == kOk;
        }
        bool IsNotFound() {
            return _error_code == kNotFound;
        }
        bool IsNetWorkErr() {
            return _error_code == kNetworkErr;
        }
        bool IsIllegalTxOp() {
            return  _error_code == kIllegalTxOp;
        }
        std::string ToString() {
            return _error_message;
        }
    private:
        enum Code {
            kOk = 0,
            kNotFound = 1,
            kNetworkErr = 2,
            kIllegalTxOp = 3
        };
        Status(Code c, const std::string& s)
        : _error_code(c),
          _error_message(s) {}
        Code _error_code;
        std::string _error_message;
    };
}
#endif // AZINO_INCLUDE_STATUS_H
