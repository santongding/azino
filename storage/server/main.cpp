#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <toml/toml.hpp>

#include "storage.h"
#include "service/storage/storage.pb.h"

DEFINE_string(server, "0.0.0.0:8000", "Address of server");
DEFINE_string(storage_name, "azino_storage", "default name of azino's storage(leveldb)");

namespace azino {
namespace storage {
    class StorageServiceImpl : public StorageService {
    public:
        StorageServiceImpl() : _storage(Storage::DefaultStorage()) {
            StorageStatus ss = _storage->Open(FLAGS_storage_name);
            if (ss.error_code() != StorageStatus::Ok) {
                LOG(FATAL) << "Fail to open storage: " << FLAGS_storage_name
                            << ", error text: " << ss.error_message();
            } else {
                LOG(INFO) << "Successes to open storage: " << FLAGS_storage_name;
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

            StorageStatus ss = _storage->Put(request->key(), request->value());
            if (ss.error_code() != StorageStatus::Ok) {
                StorageStatus* ssts = new StorageStatus(ss);
                response->set_allocated_status(ssts);
                LOG(WARNING) << cntl->remote_side() << " Fail to put key: " << request->key()
                        << " error code: " << ss.error_code()
                        << " value: " << request->value() << " error message: " << ss.error_message();
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
            StorageStatus ss = _storage->Get(request->key(), value);
            if (ss.error_code() != StorageStatus::Ok) {
                StorageStatus* ssts = new StorageStatus(ss);
                response->set_allocated_status(ssts);
                LOG(WARNING) << cntl->remote_side() << " Fail to get key: " << request->key()
                        << " error code: " << ss.error_code()
                        << " error message: " << ss.error_message();
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

            StorageStatus ss = _storage->Delete(request->key());
            if (ss.error_code() != StorageStatus::Ok) {
                StorageStatus* ssts = new StorageStatus(ss);
                response->set_allocated_status(ssts);
                LOG(WARNING) << cntl->remote_side() << " Fail to delete key: " << request->key()
                        << " error code: " << ss.error_code()
                        << " error message: " << ss.error_message();
            } else {
                LOG(INFO) << cntl->remote_side() << " Success to delete key: " << request->key();
            }
        }
    private:
        std::unique_ptr<Storage> _storage;
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

