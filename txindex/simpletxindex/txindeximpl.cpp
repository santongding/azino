#include <bthread/mutex.h>
#include <gflags/gflags.h>
#include <butil/hash.h>
#include <butil/containers/flat_map.h>
#include <functional>
#include <unordered_map>

#include "index.h"

DEFINE_int32(latch_bucket_num, 1024, "latch buckets number");

namespace azino {
namespace {

class MVCCValue {
public:
    MVCCValue() :
    _has_lock(false),
    _has_intent(false),
    _holder(), _t2v() {}
    DISALLOW_COPY_AND_ASSIGN(MVCCValue);
    ~MVCCValue() = default;
    bool HasLock() const { return _has_lock; }
    bool HasIntent() const { return _has_intent; }
    std::pair<TimeStamp, Value*> LargestTSValue() const {
       if (_t2v.empty()) {
           return std::make_pair(0, nullptr);
       }
       auto iter = _t2v.rbegin();
       return std::make_pair(iter->first, iter->second.get());
    }
    TxIdentifier Holder() const { return _holder; }
    Value* TmpValue() const {
        return _tmp_value.get();
    }

private:
    friend class KVBucket;
    bool _has_lock;
    bool _has_intent;
    std::unique_ptr<Value> _tmp_value;
    TxIdentifier _holder;
    std::map<TimeStamp, std::unique_ptr<Value>> _t2v;
};

class KVBucket : public txindex::TxIndex {
public:
    KVBucket() = default;
    DISALLOW_COPY_AND_ASSIGN(KVBucket);
    ~KVBucket() = default;

    virtual TxOpStatus WriteLock(const UserKey& key, const Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        if (_kvs.find(key) == _kvs.end()) {
           _kvs.insert(std::make_pair(key, new MVCCValue()));
        }
        MVCCValue* mv = _kvs[key].get();
        auto ltv = mv->LargestTSValue();

        if (ltv.first >= txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "\"" << key  << "\"" << " too late. "
               << "Find " << "largest ts: " << ltv.first << " value: "
               << ltv.second->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        if (mv->HasIntent() || mv->HasLock()) {
            assert(!(mv->HasIntent() && mv->HasLock()));
            if (txid.start_ts() != mv->Holder().start_ts()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "\"" << key  << "\"" << " blocked. "
                   << "Find " << (mv->HasLock() ? "lock" : "intent") << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
                   << mv->TmpValue()->ShortDebugString();
                sts.set_error_code(TxOpStatus_Code_WriteBlock);
                sts.set_error_message(ss.str());
                LOG(INFO) << ss.str();
                if (_blocked_ops.find(key) == _blocked_ops.end()) {
                    _blocked_ops.insert(std::make_pair(key, std::vector<std::function<void()>>()));
                }
                _blocked_ops[key].push_back(callback);
                return sts;
            }
            ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "\"" << key  << "\"" << " repeated. "
               << "Find "<< (mv->HasLock() ? "lock" : "intent") << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
               << mv->TmpValue()->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(WARNING) << ss.str();
            return sts;
        }

        mv->_has_lock = true;
        mv->_holder = txid;
        mv->_tmp_value.reset(new Value(v));
        ss << "Tx(" << txid.ShortDebugString() << ") write lock on " << "\"" << key  << "\"" << " successes. ";
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();
        return sts;
    }

    virtual TxOpStatus WriteIntent(const UserKey& key, const Value& v, const TxIdentifier& txid) override {
        std::lock_guard<bthread::Mutex> lck(_latch);

        TxOpStatus sts;
        std::stringstream ss;
        if (_kvs.find(key) == _kvs.end()) {
            _kvs.insert(std::make_pair(key, new MVCCValue()));
        }
        MVCCValue* mv = _kvs[key].get();
        auto ltv = mv->LargestTSValue();

        if (ltv.first >= txid.start_ts()) {
            ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "\"" << key  << "\"" << " too late. "
               << "Find " << "largest ts: " << ltv.first << " value: "
               << ltv.second->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_WriteTooLate);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        if (mv->HasIntent() || mv->HasLock()) {
            assert(!(mv->HasIntent() && mv->HasLock()));
            if (txid.start_ts() != mv->Holder().start_ts()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "\"" << key  << "\"" << " conflicts. "
                   << "Find " << (mv->HasLock() ? "lock" : "intent") << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
                   << mv->TmpValue()->ShortDebugString();
                sts.set_error_code(TxOpStatus_Code_WriteConflicts);
                sts.set_error_message(ss.str());
                LOG(INFO) << ss.str();
                return sts;
            }
            if (mv->HasIntent()) {
                ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "\"" << key  << "\"" << " repeated. "
                   << "Find "<< "intent" << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
                   << mv->TmpValue()->ShortDebugString();
                sts.set_error_code(TxOpStatus_Code_Ok);
                sts.set_error_message(ss.str());
                LOG(WARNING) << ss.str();
                return sts;
            }

            mv->_has_lock = false;
            mv->_has_intent = true;
            mv->_holder = txid;
            ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "\"" << key  << "\"" << " successes. "
               << "Find "<< "lock" << " Tx(" << mv->Holder().ShortDebugString() << ") value: "
               << mv->TmpValue()->ShortDebugString();
            sts.set_error_code(TxOpStatus_Code_Ok);
            sts.set_error_message(ss.str());
            LOG(INFO) << ss.str();
            return sts;
        }

        mv->_has_intent = true;
        mv->_holder = txid;
        mv->_tmp_value.reset(new Value(v));
        ss << "Tx(" << txid.ShortDebugString() << ") write intent on " << "\"" << key  << "\"" << " successes. ";
        sts.set_error_code(TxOpStatus_Code_Ok);
        sts.set_error_message(ss.str());
        LOG(INFO) << ss.str();
        return sts;
    }

    virtual TxOpStatus Read(const UserKey& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        std::lock_guard<bthread::Mutex> lck(_latch);
        return TxOpStatus();
    }

private:
    /*butil::FlatMap<UserKey, std::unique_ptr<MVCCValue>> _kvs;
    butil::FlatMap<UserKey, std::vector<std::function<void()>>> _blocked_ops;*/
    std::unordered_map<UserKey, std::unique_ptr<MVCCValue>> _kvs;
    std::unordered_map<UserKey, std::vector<std::function<void()>>> _blocked_ops;
    bthread::Mutex _latch;
};

class TxIndexImpl : public txindex::TxIndex {
public:
    TxIndexImpl() {
        for (int i = 0; i < FLAGS_latch_bucket_num; i++) {
            _kvbs.emplace_back(new KVBucket);
        }
    }
    DISALLOW_COPY_AND_ASSIGN(TxIndexImpl);
    ~TxIndexImpl() = default;

    virtual TxOpStatus WriteLock(const UserKey& key, const Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->WriteLock(key, v, txid, callback);
    }

    virtual TxOpStatus WriteIntent(const UserKey& key, const Value& v, const TxIdentifier& txid) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->WriteIntent(key, v, txid);
    }

    virtual TxOpStatus Read(const UserKey& key, Value& v, const TxIdentifier& txid, std::function<void()> callback) override {
        auto bucket_num = butil::Hash(key) % FLAGS_latch_bucket_num;
        return _kvbs[bucket_num]->Read(key, v, txid, callback);
    }

private:
    std::vector<std::unique_ptr<KVBucket>> _kvbs;
};

} // namespace

namespace txindex {
    TxIndex* TxIndex::DefaultTxIndex() {
        return new TxIndexImpl();
    }
} // namespace txindex

} // namespace azino