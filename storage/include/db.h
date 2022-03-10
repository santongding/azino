
#ifndef AZINO_STORAGE_INCLUDE_DB_H
#define AZINO_STORAGE_INCLUDE_DB_H

#include <string>
#include "status.h"

namespace azino {
namespace storage {

    class DB {
    public:
        // return the default db impl
        static DB* DefaultDB();

        DB() = default;

        DB(const DB&) = delete;
        DB& operator=(const DB&) = delete;

        virtual ~DB() = default;

        virtual Status Open(const std::string& name) = 0;

        // Set the database entry for "key" to "value".  Returns OK on success,
        // and a non-OK status on error.
        virtual Status Put(const std::string& key,
                           const std::string& value) = 0;

        // Remove the database entry (if any) for "key".  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        virtual Status Delete(const std::string& key) = 0;

        // If the database contains an entry for "key" store the
        // corresponding value in *value and return OK.
        //
        // If there is no entry for "key" leave *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual Status Get(const std::string& key,
                           std::string& value) = 0;
    };

} // namespace storage
} // namespace azino

#endif // AZINO_STORAGE_INCLUDE_DB_H
