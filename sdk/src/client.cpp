#include <brpc/channel.h>

#include "azino/client.h"
#include "txwritebuffer.h"
#include "service/tx.pb.h"
#include "service/txplanner/txplanner.pb.h"

namespace azino {

DEFINE_int32(timeout_ms, -1, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 2, "Max retries(not including the first RPC)");

    Transaction::Transaction(const Options& options, const std::string& txplanner_addr)
    : _channel_options(new brpc::ChannelOptions),
      _txid(nullptr),
      _txwritebuffer(new TxWriteBuffer) {
        _channel_options->timeout_ms = FLAGS_max_retry;
        _channel_options->max_retry = FLAGS_timeout_ms;

        auto* channel = new brpc::Channel();
        if (channel->Init(txplanner_addr.c_str(), _channel_options.get()) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }
        _txplanner = std::make_pair(txplanner_addr, std::shared_ptr<brpc::Channel>(channel));
    }

    Transaction::~Transaction() = default;

    Status Transaction::Begin() {
        std::stringstream ss;
        if (_txid) {
            ss << "Transaction has already began. " << _txid->ShortDebugString();
            return Status::IllegalTxOp(ss.str());
        }
        azino::txplanner::TxService_Stub stub(_txplanner.second.get());
        brpc::Controller cntl;
        azino::txplanner::BeginTxRequest req;
        azino::txplanner::BeginTxResponse resp;
        stub.BeginTx(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            ss << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
            LOG(WARNING) << ss.str();
            return Status::NetworkErr(ss.str());
        }
        if (!resp.has_txid() || !resp.has_storage_addr() || resp.txindex_addrs_size() == 0) {
            ss << "sdk: " << cntl.local_side() << " BeginTx from txplanner: " << cntl.remote_side()
               << " response: " << resp.ShortDebugString()
               << " is incomplete." << cntl.latency_us();
            LOG(WARNING) << ss.str();
            return Status::NotSupportedErr(ss.str());
        }

        ss << "sdk: " << cntl.local_side() << " BeginTx from txplanner: " << cntl.remote_side()
           << " response: " << resp.ShortDebugString()
           << " latency=" << cntl.latency_us() << "us";
        LOG(INFO) << ss.str();

        _txid.reset(resp.release_txid());

        auto* channel = new brpc::Channel();
        if (channel->Init(resp.storage_addr().c_str(), _channel_options.get()) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }
        _storage = std::make_pair(resp.storage_addr(), std::shared_ptr<brpc::Channel>(channel));

        for (int i = 0; i < resp.txindex_addrs_size(); i++) {
            auto* channel = new brpc::Channel();
            if (channel->Init(resp.txindex_addrs(i).c_str(), _channel_options.get()) != 0) {
                LOG(ERROR) << "Fail to initialize channel";
            }
            _txindexs[resp.txindex_addrs(i)] = std::shared_ptr<brpc::Channel>(channel);
        }

        return Status::Ok(ss.str());
    }

    Status Transaction::Commit() {
        std::stringstream ss;
        if (!_txid) {
            ss << "Transaction has not began. ";
            return Status::IllegalTxOp(ss.str());
        }
        if (_txid->status().status_code() != TxStatus_Code_Started) {
            ss << "Transaction is not allowed to commit. " << _txid->ShortDebugString();
            return Status::IllegalTxOp(ss.str());
        }

        azino::txplanner::TxService_Stub stub(_txplanner.second.get());
        brpc::Controller cntl;
        azino::txplanner::CommitTxRequest req;
        req.set_allocated_txid(new TxIdentifier(*_txid));
        azino::txplanner::CommitTxResponse resp;
        stub.CommitTx(&cntl, &req, &resp, nullptr);
        if (!resp.has_txid()) {
            ss << "sdk: " << cntl.local_side() << " CommitTx from txplanner: " << cntl.remote_side()
               << " response: " << resp.ShortDebugString()
               << " is incomplete." << cntl.latency_us();
            LOG(WARNING) << ss.str();
            return Status::NotSupportedErr(ss.str());
        }

        ss << "sdk: " << cntl.local_side() << " CommitTx from txplanner: " << cntl.remote_side()
           << " response: " << resp.ShortDebugString()
           << " latency=" << cntl.latency_us() << "us";

        _txid.reset(resp.release_txid());

        LOG(INFO) << ss.str();
        return Status::Ok(ss.str());
        // todo: commit all kvs
    }
}