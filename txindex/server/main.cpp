#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <toml/toml.hpp>

#include "index.h"

namespace azino {
namespace txindex {

}
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
}