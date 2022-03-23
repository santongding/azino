#include <gtest/gtest.h>
#include <butil/hash.h>

#include "index.h"

class TxIndexImplTest : public testing::Test {
public:
    azino::txindex::TxIndex* ti;
    azino::TxIdentifier t1;
    azino::TxIdentifier t2;
    std::string k1 = "key1";
    azino::Value v1;
    std::string k2 = "key2";
    azino::Value v2;
protected:
    void SetUp() {
       ti = azino::txindex::TxIndex::DefaultTxIndex();
       t1.set_start_ts(1);
       t2.set_start_ts(2);
       v1.set_content("tx1value");
       v2.set_content("tx2value");
    }
    void TearDown() {
        delete ti;
    }
};

void dummyCallback() {}

TEST_F(TxIndexImplTest, dummy) {
    ASSERT_EQ(butil::Hash("abc"), 3535673738);
    ASSERT_EQ(butil::Hash("hello"), 2963130491);
    azino::TxIdentifier id;
    id.set_start_ts(123);
    id.set_commit_ts(456);
    LOG(INFO) << id.ShortDebugString();
}

TEST_F(TxIndexImplTest, write_intent_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, dummyCallback).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
}

TEST_F(TxIndexImplTest, write_intent_conflicts) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts, ti->WriteIntent(k1, v2, t2).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, dummyCallback).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts, ti->WriteIntent(k2, v2, t2).error_code());
}

TEST_F(TxIndexImplTest, write_intent_too_late) {

}

TEST_F(TxIndexImplTest, write_lock_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v1, t1, dummyCallback).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v1, t1, dummyCallback).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, dummyCallback).error_code());
}

TEST_F(TxIndexImplTest, write_lock_block) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteBlock, ti->WriteLock(k1, v2, t2, dummyCallback).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, dummyCallback).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteBlock, ti->WriteLock(k2, v2, t2, dummyCallback).error_code());
}

TEST_F(TxIndexImplTest, write_lock_too_late) {

}

TEST_F(TxIndexImplTest, read_ok) {

}

TEST_F(TxIndexImplTest, read_block) {

}

TEST_F(TxIndexImplTest, read_not_exist) {

}

TEST_F(TxIndexImplTest, read_deleted) {

}