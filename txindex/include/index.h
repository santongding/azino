#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H

#include "azino/kv.h"
#include "service/tx.pb.h"

namespace azino {
namespace txindex {
    class TxIndex {
    public:
        // return the default index impl
        static TxIndex* DefaultTxIndex();

        TxIndex() = default;

        TxIndex(const TxIndex&) = delete;
        TxIndex& operator=(const TxIndex&) = delete;

        virtual ~TxIndex() = default;

        // This is an atomic read-write operation for one user_key, only used in pessimistic transactions.
        // Success when no newer version of this key, intent or lock exists.
        // Should success if txid already hold this lock.
        virtual TxOpStatus WriteLock(const UserKey& key, const Value& v, const TxIdentifier& txid, std::function<void> callback) = 0;

        // This is an atomic read-write operation for one user_key, used in both pessimistic and optimistic transactions.
        // Success when no newer version of this key, intent or lock exists.
        // Should success if txid already hold this intent or lock, and change lock to intent at the same time.
        virtual TxOpStatus WriteIntent(const UserKey& key, const Value& v, const TxIdentifier& txid) = 0;

        // Current implementation uses snapshot isolation, so read is declared as a const method.
        // read will be blocked if there exists and intent who has a smaller ts than read's ts.
        // read will bypass any lock, and return the key value pair who has the biggest ts among all that have ts smaller than read's ts.
        virtual TxOpStatus Read(const UserKey& key, Value& v, const TxIdentifier& txid, std::function<void> callback) const = 0;

    };
}
}
#endif // AZINO_TXINDEX_INCLUDE_INDEX_H
