#include <brpc/channel.h>

#include "azino/client.h"
#include "txopslog.h"
#include "service/tx.pb.h"


namespace azino {
    Trasaction::Trasaction(const Options& options, const std::string& txplanner_addr)
    : _txid(nullptr),
      _txopslog(nullptr) {

        auto* channel = new brpc::Channel();
        auto* channelOptions = new brpc::ChannelOptions();

        if (channel->Init(txplanner_addr.c_str(), channelOptions) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }

        _txplanner = std::make_pair(txplanner_addr, std::shared_ptr<brpc::Channel>(channel));
    }

    Trasaction::~Trasaction() = default;

}