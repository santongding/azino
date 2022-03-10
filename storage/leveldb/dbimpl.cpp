#include <leveldb/db.h>
#include <memory>

#include "db.h"

namespace azino {
namespace storage {
namespace {

    class LevelDBImpl : public DB {
    public:
        LevelDBImpl() : _leveldbptr(nullptr) {}
        LevelDBImpl(const LevelDBImpl&) = delete;
        LevelDBImpl& operator=(const LevelDBImpl&) = delete;

        virtual ~LevelDBImpl() = default;

        virtual Status Open(const std::string& name) override {
            if (_leveldbptr != nullptr) {
                return Status::InvalidArgument("Already opened an leveldb");
            }
            leveldb::DB* leveldbptr;
            leveldb::Options opt;
            opt.create_if_missing = true;
            leveldb::Status leveldbstatus;
            leveldbstatus =  leveldb::DB::Open(opt, name, &leveldbptr);
            if (leveldbstatus.ok()) {
                _leveldbptr.reset(leveldbptr);
            }
            return Status::LevelDBStatus(leveldbstatus);
        }

        // Set the database entry for "key" to "value".  Returns OK on success,
        // and a non-OK status on error.
        virtual Status Put(const std::string& key, const std::string& value) override {
            if (_leveldbptr == nullptr) {
                return Status::InvalidArgument("No opened leveldb");
            }
            leveldb::WriteOptions opts;
            opts.sync = true;
            leveldb::Status leveldbstatus;
            leveldbstatus = _leveldbptr->Put(opts, key, value);
            return Status::LevelDBStatus(leveldbstatus);
        }

        // Remove the database entry (if any) for "key".  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        virtual Status Delete(const std::string& key) override {
            if (_leveldbptr == nullptr) {
                return Status::InvalidArgument("No opened leveldb");
            }
            leveldb::WriteOptions opts;
            opts.sync = true;
            leveldb::Status leveldbstatus;
            leveldbstatus = _leveldbptr->Delete(opts, key);
            return Status::LevelDBStatus(leveldbstatus);
        }

        // If the database contains an entry for "key" store the
        // corresponding value in *value and return OK.
        //
        // If there is no entry for "key" leave *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual Status Get(const std::string& key, std::string& value) override {
            if (_leveldbptr == nullptr) {
                return Status::InvalidArgument("No opened leveldb");
            }
            leveldb::ReadOptions opt;
            opt.verify_checksums = true;
            leveldb::Status leveldbstatus;
            leveldbstatus = _leveldbptr->Get(opt, key, &value);
            return Status::LevelDBStatus(leveldbstatus);
        }

    private:
        std::unique_ptr<leveldb::DB> _leveldbptr;
    };
} // namespace

    Status Status::LevelDBStatus(const leveldb::Status &rhs) {
        if (rhs.ok()) {
            return Status::OK();
        } else if (rhs.IsCorruption()) {
            return Status::Corruption(rhs.ToString());
        } else if (rhs.IsIOError()) {
            return Status::IOError(rhs.ToString());
        } else if (rhs.IsInvalidArgument()) {
            return Status::InvalidArgument(rhs.ToString());
        } else if (rhs.IsNotFound()) {
            return Status::NotFound(rhs.ToString());
        } else {
            return Status::NotSupported(rhs.ToString());
        }
    }

    DB *DB::DefaultDB() {
        return new LevelDBImpl();
    }
} // namespace storage
} // namespace azino