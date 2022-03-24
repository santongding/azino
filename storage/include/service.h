#ifndef AZINO_STORAGE_INCLUDE_SERVICE_H
#define AZINO_STORAGE_INCLUDE_SERVICE_H

#include "storage.h"
#include "service/storage/storage.pb.h"

namespace azino {
namespace storage {

    class StorageServiceImpl : public StorageService {
    public:
        StorageServiceImpl();

        virtual ~StorageServiceImpl() = default;

        virtual void Put(::google::protobuf::RpcController* controller,
                         const ::azino::storage::PutRequest* request,
                         ::azino::storage::PutResponse* response,
                         ::google::protobuf::Closure* done) override;

        virtual void Get(::google::protobuf::RpcController* controller,
                         const ::azino::storage::GetRequest* request,
                         ::azino::storage::GetResponse* response,
                         ::google::protobuf::Closure* done) override;

        virtual void Delete(::google::protobuf::RpcController* controller,
                            const ::azino::storage::DeleteRequest* request,
                            ::azino::storage::DeleteResponse* response,
                            ::google::protobuf::Closure* done) override;
    private:
        std::unique_ptr<Storage> _storage;
    };

} // namespace storage
} // namespace azino


#endif // AZINO_STORAGE_INCLUDE_SERVICE_H
