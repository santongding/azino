#ifndef AZINO_STORAGE_INCLUDE_STATUS_H
#define AZINO_STORAGE_INCLUDE_STATUS_H

#include <algorithm>
#include <string>
#include <cassert>
#include <memory>
#include <cstring>

namespace leveldb {
    class Status;
}

namespace azino {
namespace storage {
    class Status {
        public:
        // Create a success status.
        Status() noexcept : state_(nullptr) {}
        ~Status() { delete[] state_; }

        Status(const Status& rhs);
        Status& operator=(const Status& rhs);

        Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
        Status& operator=(Status&& rhs) noexcept;

        // status translation for each DB impl
        static Status LevelDBStatus(const leveldb::Status& rhs);

        // Return a success status.
        static Status OK() { return Status(); }

        // Return error status of an appropriate type.
        static Status NotFound(const std::string& msg, const std::string& msg2 = std::string()) {
            return Status(kNotFound, msg, msg2);
        }
        static Status Corruption(const std::string& msg, const std::string& msg2 = std::string()) {
            return Status(kCorruption, msg, msg2);
        }
        static Status NotSupported(const std::string& msg, const std::string& msg2 = std::string()) {
            return Status(kNotSupported, msg, msg2);
        }
        static Status InvalidArgument(const std::string& msg, const std::string& msg2 = std::string()) {
            return Status(kInvalidArgument, msg, msg2);
        }
        static Status IOError(const std::string& msg, const std::string& msg2 = std::string()) {
            return Status(kIOError, msg, msg2);
        }

        // Returns true iff the status indicates success.
        bool ok() const { return (state_ == nullptr); }

        // Returns true iff the status indicates a NotFound error.
        bool IsNotFound() const { return code() == kNotFound; }

        // Returns true iff the status indicates a Corruption error.
        bool IsCorruption() const { return code() == kCorruption; }

        // Returns true iff the status indicates an IOError.
        bool IsIOError() const { return code() == kIOError; }

        // Returns true iff the status indicates a NotSupportedError.
        bool IsNotSupportedError() const { return code() == kNotSupported; }

        // Returns true iff the status indicates an InvalidArgument.
        bool IsInvalidArgument() const { return code() == kInvalidArgument; }

        // Return a string representation of this status suitable for printing.
        // Returns the string "OK" for success.
        std::string ToString() const;

        private:
        enum Code {
            kOk = 0,
            kNotFound = 1,
            kCorruption = 2,
            kNotSupported = 3,
            kInvalidArgument = 4,
            kIOError = 5
        };

        Code code() const {
            return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
        }

        Status(Code code, const std::string& msg, const std::string& msg2);
        static const char* CopyState(const char* s);

        // OK status has a null state_.  Otherwise, state_ is a new[] array
        // of the following form:
        //    state_[0..3] == length of message
        //    state_[4]    == code
        //    state_[5..]  == message
        const char* state_;
    };

    inline Status::Status(const Status& rhs) {
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }

    inline Status& Status::operator=(const Status& rhs) {
        // The following condition catches both aliasing (when this == &rhs),
        // and the common case where both rhs and *this are ok.
        if (state_ != rhs.state_) {
            delete[] state_;
            state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
        }
        return *this;
    }

    inline Status& Status::operator=(Status&& rhs) noexcept {
        std::swap(state_, rhs.state_);
        return *this;
    }

    inline const char* Status::CopyState(const char* state) {
        uint32_t size;
        std::memcpy(&size, state, sizeof(size));
        char* result = new char[size + 5];
        std::memcpy(result, state, size + 5);
        return result;
    }

    inline Status::Status(Code code, const std::string& msg, const std::string& msg2) {
        assert(code != kOk);
        const uint32_t len1 = static_cast<uint32_t>(msg.size());
        const uint32_t len2 = static_cast<uint32_t>(msg2.size());
        const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
        char* result = new char[size + 5];
        std::memcpy(result, &size, sizeof(size));
        result[4] = static_cast<char>(code);
        std::memcpy(result + 5, msg.data(), len1);
        if (len2) {
            result[5 + len1] = ':';
            result[6 + len1] = ' ';
            std::memcpy(result + 7 + len1, msg2.data(), len2);
        }
        state_ = result;
    }

    inline std::string Status::ToString() const {
        if (state_ == nullptr) {
            return "OK";
        } else {
            char tmp[30];
            const char* type;
            switch (code()) {
                case kOk:
                    type = "OK";
                    break;
                case kNotFound:
                    type = "NotFound: ";
                    break;
                case kCorruption:
                    type = "Corruption: ";
                    break;
                case kNotSupported:
                    type = "Not implemented: ";
                    break;
                case kInvalidArgument:
                    type = "Invalid argument: ";
                    break;
                case kIOError:
                    type = "IO error: ";
                    break;
                default:
                    std::snprintf(tmp, sizeof(tmp),
                                  "Unknown code(%d): ", static_cast<int>(code()));
                    type = tmp;
                    break;
            }
            std::string result(type);
            uint32_t length;
            std::memcpy(&length, state_, sizeof(length));
            result.append(state_ + 5, length);
            return result;
        }
    }


} // namespace storage
} // namespace azino

#endif // AZINO_STORAGE_INCLUDE_STATUS_H
