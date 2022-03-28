#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <toml/toml.hpp>

#include "service.h"

DEFINE_string(txindex_addr, "0.0.0.0:8002", "Addresses of txindex");
namespace logging {
    DECLARE_bool(crash_on_fatal_log);
}

namespace azino {
namespace txindex {

}
}

int main(int argc, char* argv[]) {
    logging::FLAGS_crash_on_fatal_log = true;
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    azino::txindex::TxOpServiceImpl tx_op_service_impl;
    if (server.AddService(&tx_op_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add tx_op_service_impl";
        return -1;
    }

    brpc::ServerOptions options;
    if (server.Start(FLAGS_txindex_addr.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to start TxIndexServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}