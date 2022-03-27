#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <iostream>
#include "service/storage/storage.pb.h"


DEFINE_string(server, "0.0.0.0:8000", "IP Address of server");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 0, "Max retries(not including the first RPC)");

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // A Channel represents a communication line to a Server. Notice that
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::Channel channel;

    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    azino::storage::StorageService_Stub stub(&channel);
    int log_id = 0;

    while (!brpc::IsAskedToQuit()) {
        std::string action;
        std::string key, value;
        brpc::Controller cntl;
        azino::storage::StorageStatus ssts;
        std::cin >> action;

        if (action == "put") {
            azino::storage::PutRequest req;
            azino::storage::PutResponse resp;
            cntl.set_log_id(log_id++);
            std::cin >> key >> value;
            req.set_key(key);
            req.set_value(value);
            stub.Put(&cntl, &req, &resp, NULL);
            ssts = resp.status();
            if (ssts.error_code() != azino::storage::StorageStatus::Ok) {
                std::cout << "error code: "<< ssts.error_code() << " error message: " << ssts.error_message() << std::endl;
            } else if (!cntl.Failed()) {
                std::cout << "OK." << std::endl;
            }
        } else if (action == "get") {
            azino::storage::GetRequest req;
            azino::storage::GetResponse resp;
            cntl.set_log_id(log_id++);
            std::cin >> key;
            req.set_key(key);
            stub.Get(&cntl, &req, &resp, NULL);
            ssts = resp.status();
            if (ssts.error_code() != azino::storage::StorageStatus::Ok) {
                std::cout << "error code: "<< ssts.error_code() << " error message: " << ssts.error_message() << std::endl;
            } else if (!cntl.Failed()) {
                std::cout << resp.value() << std::endl;
            }
        } else if (action == "delete") {
            azino::storage::DeleteRequest req;
            azino::storage::DeleteResponse resp;
            cntl.set_log_id(log_id++);
            std::cin >> key;
            req.set_key(key);
            stub.Delete(&cntl, &req, &resp, NULL);
            ssts = resp.status();
            if (ssts.error_code() != azino::storage::StorageStatus::Ok) {
                std::cout << "error code: "<< ssts.error_code() << " error message: " << ssts.error_message() << std::endl;
            } else if (!cntl.Failed()) {
                std::cout << "OK." << std::endl;
            }
        } else {
            std::getline(std::cin, action);
            std::cout << "Use put, get or delete" << std::endl;
            continue;
        }

        if (!cntl.Failed()) {
            LOG(INFO) << action << " " << key << " " << value
                      << " received response from " << cntl.remote_side()
                      << " to " << cntl.local_side()
                      << " error code: " << ssts.error_code()
                      << " error message: " << ssts.error_message()
                      << " latency=" << cntl.latency_us() << "us";
        } else {
            LOG(WARNING) << "Controller failed error code: " << cntl.ErrorCode() << " error text: " << cntl.ErrorText();
        }
    }

    LOG(INFO) << "azino client is going to quit";
    return 0;
}


