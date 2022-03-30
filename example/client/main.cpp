#include "azino/client.h"

#include <gflags/gflags.h>
#include <iostream>

DEFINE_string(txplanner_addr, "0.0.0.0:8001", "Address of txplanner");

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    while (true) {
        azino::Options options;
        azino::Transaction tx(options, FLAGS_txplanner_addr);
        while (true) {
            std::string action;
            std::string key, value;
            std::cin >> action;
            if (action == "begin") {
                auto sts = tx.Begin();
                std::cout << sts.ToString() << std::endl;
            } else if (action == "commit") {
                auto sts = tx.Commit();
                std::cout << sts.ToString() << std::endl;
                if (sts.IsOk()) break;
            } else if (action == "put") {
                std::cin >> key >> value;
            } else if (action == "get") {
                std::cin >> key;
            } else if (action == "delete") {
                std::cin >> key;
            } else {
                std::getline(std::cin, action);
                std::cout << "Use put, get or delete" << std::endl;
                continue;
            }
        }
    }
}


