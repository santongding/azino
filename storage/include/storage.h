
#ifndef AZINO_STORAGE_INCLUDE_STORAGE_H
#define AZINO_STORAGE_INCLUDE_STORAGE_H

#include <string>
#include <butil/macros.h>
#include "service/storage/storage.pb.h"

namespace azino {
namespace storage {

    class Storage {
    public:
        // return the default Storage impl
        static Storage* DefaultStorage();

        Storage() = default;
        DISALLOW_COPY_AND_ASSIGN(Storage);
        virtual ~Storage() = default;

        virtual StorageStatus Open(const std::string& name) = 0;

        // Set the database entry for "key" to "value".  Returns OK on success,
        // and a non-OK status on error.
        virtual StorageStatus Put(const std::string& key,
                           const std::string& value) = 0;

        // Remove the database entry (if any) for "key".  Returns OK on
        // success, and a non-OK status on error.  It is not an error if "key"
        // did not exist in the database.
        virtual StorageStatus Delete(const std::string& key) = 0;

        // If the database contains an entry for "key" store the
        // corresponding value in *value and return OK.
        //
        // If there is no entry for "key" leave *value unchanged and return
        // a status for which Status::IsNotFound() returns true.
        //
        // May return some other Status on an error.
        virtual StorageStatus Get(const std::string& key,
                           std::string& value) = 0;
    };

} // namespace storage
} // namespace azino

#endif // AZINO_STORAGE_INCLUDE_STORAGE_H
