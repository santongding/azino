#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <toml/toml.hpp>

#include "db.h"
#include "service/storage/storage.pb.h"

DEFINE_string(server, "0.0.0.0:8000", "Address of server");
DEFINE_string(db_name, "azino_storage", "default name of azino's storage(leveldb)");

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
                LOG(WARNING) << cntl->remote_side() << " Fail to put key: " << request->key()
                        << " error code: " << sts.code()
                        << " value: " << request->value() << " error message: " << sts.ToString();
            } else {
                LOG(INFO) << cntl->remote_side() << " Success to put key: " << request->key()
                        << " value: " << request->value();
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
                LOG(WARNING) << cntl->remote_side() << " Fail to get key: " << request->key()
                        << " error code: " << sts.code()
                        << " error message: " << sts.ToString();
            } else {
                LOG(INFO) << cntl->remote_side() << " Success to get key: " << request->key()
                        << " value: " << value;
                response->set_value(value);
            }
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
                LOG(WARNING) << cntl->remote_side() << " Fail to delete key: " << request->key()
                        << " error code: " << sts.code()
                        << " error message: " << sts.ToString();
            } else {
                LOG(INFO) << cntl->remote_side() << " Success to delete key: " << request->key();
            }
        }
    private:
        std::unique_ptr<DB> _db;
    };
} // namespace storage
} // namespace azino

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    azino::storage::StorageServiceImpl storage_service_impl;
    if (server.AddService(&storage_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add storage_service_impl";
        return -1;
    }

    brpc::ServerOptions options;
    if (server.Start(FLAGS_server.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start StorageServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}

