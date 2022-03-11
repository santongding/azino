#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <toml/toml.hpp>

#include "db.h"
#include "service/storage/storage.pb.h"

DEFINE_int32(port, 8000, "TCP Port of this server");
DEFINE_string(db_name, "azino_storage", "default name of azino's storage(leveldb)");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
"read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
"(waiting for client to close connection before server stops)");

namespace azino {
namespace storage {
    class StorageServiceImpl : public StorageService {
    public:
        StorageServiceImpl() : _db(DB::DefaultDB()) {
            Status sts = _db->Open(FLAGS_db_name);
            if (!sts.ok()) {
                LOG(FATAL) << "Fail to open db: " << FLAGS_db_name
                            << ", error text: " << sts.ToString();
            } else {
                LOG(INFO) << "Successes to open db: " << FLAGS_db_name;
            }
        }

        virtual ~StorageServiceImpl() = default;

        virtual void Put(::google::protobuf::RpcController* controller,
                         const ::azino::storage::PutRequest* request,
                         ::azino::storage::PutResponse* response,
                         ::google::protobuf::Closure* done) override {
            // This object helps you to call done->Run() in RAII style. If you need
            // to process the request asynchronously, pass done_guard.release().
            brpc::ClosureGuard done_guard(done);
            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

            Status sts = _db->Put(request->key(), request->value());
            if (!sts.ok()) {
                StorageStatus* ssts = new StorageStatus();
                ssts->set_error_code(static_cast<StorageStatus_Code>(sts.code()));
                ssts->set_error_message(sts.ToString());
                response->set_allocated_status(ssts);
            }
        }

        virtual void Get(::google::protobuf::RpcController* controller,
                         const ::azino::storage::GetRequest* request,
                         ::azino::storage::GetResponse* response,
                         ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard done_guard(done);
            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

            std::string value;
            Status sts = _db->Get(request->key(), value);
            if (!sts.ok()) {
                StorageStatus* ssts = new StorageStatus();
                ssts->set_error_code(static_cast<StorageStatus_Code>(sts.code()));
                ssts->set_error_message(sts.ToString());
                response->set_allocated_status(ssts);
            }
            response->set_value(value);
        }

        virtual void Delete(::google::protobuf::RpcController* controller,
                            const ::azino::storage::DeleteRequest* request,
                            ::azino::storage::DeleteResponse* response,
                            ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard done_guard(done);
            brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

            Status sts = _db->Delete(request->key());
            if (!sts.ok()) {
                StorageStatus* ssts = new StorageStatus();
                ssts->set_error_code(static_cast<StorageStatus_Code>(sts.code()));
                ssts->set_error_message(sts.ToString());
                response->set_allocated_status(ssts);
            }
        }
    private:
        std::unique_ptr<DB> _db;
    };
} // namespace storage
} // namespace azino

int main(int argc, char* argv[]) {
    std::string s;
    return 0;
}

