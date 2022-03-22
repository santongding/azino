#include <gtest/gtest.h>
#include <butil/hash.h>

#include "index.h"

class TxIndexImplTest : public testing::Test {
public:

protected:
    void SetUp() {

    }
    void TearDown() {

    }
};

TEST_F(TxIndexImplTest, hash) {
    ASSERT_EQ(butil::Hash("abc"), 3535673738);
    ASSERT_EQ(butil::Hash("hello"), 2963130491);
    azino::TxIdentifier id;
    id.set_start_ts(123);
    id.set_commit_ts(456);
    LOG(INFO) << id.ShortDebugString();
}