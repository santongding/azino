#ifndef AZINO_TXINDEX_INCLUDE_INDEX_H
#define AZINO_TXINDEX_INCLUDE_INDEX_H


namespace azino {
    class InternalKey;
    class Value;
    class TxIdentifier;
namespace txindex {
    class TxindexStatus;
    class Index {
        // return the default index impl
        static Index* DefaultIndex();

        Index() = default;

        Index(const Index&) = delete;
        Index& operator=(const Index&) = delete;

        virtual ~Index() = default;

        // This is an atomic read-write operation for one user_key, only used in pessimistic transactions.
        // Success when no newer version of this key, intent or lock exists.
        // Should success if txid already hold this lock.
        virtual TxindexStatus writeLock(const InternalKey& key, const Value& v, const TxIdentifier& txid, std::function<void> callback) = 0;

        // This is an atomic read-write operation for one user_key, used in both pessimistic and optimistic transactions.
        // Success when no newer version of this key, intent or lock exists.
        // Should success if txid already hold this intent or lock, and change lock to intent at the same time.
        virtual TxindexStatus writeIntent(const InternalKey& key, const Value& v, const TxIdentifier& txid) = 0;


        // Current implementation uses snapshot isolation, so read is declared as a const method.
        // read will be blocked if there exists and intent who has a smaller ts than read's ts.
        // read will bypass any lock, and return the key value pair who has the biggest ts among all that have ts smaller than read's ts.
        virtual TxindexStatus read(const InternalKey& key, Value& v, const TxIdentifier& txid, std::function<void> callback) const = 0;

    };
}
}
#endif // AZINO_TXINDEX_INCLUDE_INDEX_H
