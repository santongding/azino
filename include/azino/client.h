#include <butil/macros.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

#include "status.h"
#include "kv.h"
#include "options.h"

#ifndef AZINO_INCLUDE_CLIENT_H
#define AZINO_INCLUDE_CLIENT_H

namespace brpc {
    class Channel;
}

namespace azino {
    class TxIdentifier;
    class TxOpsLog;

    // not thread safe, and it is not reusable.
    class Trasaction {
    public:
        Trasaction(const Options& options, const std::string& txplanner_addr);
        DISALLOW_COPY_AND_ASSIGN(Trasaction);
        ~Trasaction();

        // tx operations
        Status Start();
        Status Commit();

        // kv operations, fail when tx has not started
        Status Put(const WriteOptions& options, const UserKey& key, const std::string& value);
        Status Get(const ReadOptions& options, const UserKey& Key, std::string& value);
        Status Delete(const WriteOptions& options, const UserKey& key);
        
    private:
        std::pair<std::string, std::shared_ptr<brpc::Channel>> _txplanner;
        std::pair<std::string, std::shared_ptr<brpc::Channel>> _storage;
        std::unordered_map<std::string, std::shared_ptr<brpc::Channel>> _txindexs;
        std::unique_ptr<TxIdentifier> _txid;
        std::unique_ptr<TxOpsLog> _txopslog;
    };


}

#endif // AZINO_INCLUDE_CLIENT_H
