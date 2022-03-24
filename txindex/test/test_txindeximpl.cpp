#include <gtest/gtest.h>
#include <butil/hash.h>
#include <bthread/bthread.h>

#include "index.h"

class TxIndexImplTest : public testing::Test {
public:
    TxIndexImplTest() {
        bthread_mutex_init(&_m, nullptr);
        bthread_cond_init(&_cv, nullptr);
    }

    azino::txindex::TxIndex* ti;
    azino::TxIdentifier t1;
    azino::TxIdentifier t2;
    std::string k1 = "key1";
    azino::Value v1;
    std::string k2 = "key2";
    azino::Value v2;

    void dummyCallback() {
        LOG(INFO) << "Calling dummy call back.";
        bthread_cond_signal(&_cv);
    }

    void waitDummyCallback() {
        bthread_mutex_lock(&_m);
        bthread_cond_wait(&_cv, &_m);
        _called = true;
        bthread_mutex_unlock(&_m);
    }

    bool Called() {
        return _called;
    }

    void UnCalled() {
        _called = false;
    }

protected:
    void SetUp() {
        UnCalled();
        ti = azino::txindex::TxIndex::DefaultTxIndex();
        t1.set_start_ts(1);
        t2.set_start_ts(2);
        v1.set_content("tx1value");
        v2.set_content("tx2value");
    }
    void TearDown() {
        delete ti;
    }

private:
    bool _called;
    bthread_mutex_t _m;
    bthread_cond_t _cv;
};



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

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
}

TEST_F(TxIndexImplTest, write_intent_conflicts) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts, ti->WriteIntent(k1, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteConflicts, ti->WriteIntent(k2, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v2, t2).error_code());
}

TEST_F(TxIndexImplTest, write_intent_too_late) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteTooLate, ti->WriteIntent(k1, v1, t1).error_code());
}

TEST_F(TxIndexImplTest, write_lock_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
}

TEST_F(TxIndexImplTest, write_lock_block) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteBlock, ti->WriteLock(k1, v2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
    UnCalled();

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteBlock, ti->WriteLock(k2, v2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
    waitDummyCallback();
    ASSERT_TRUE(Called());
}

TEST_F(TxIndexImplTest, write_lock_too_late) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_WriteTooLate, ti->WriteLock(k1, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
}

TEST_F(TxIndexImplTest, clean_not_exist) {
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist, ti->Clean(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CleanNotExist, ti->Clean(k1, t1).error_code());
}

TEST_F(TxIndexImplTest, clean_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k1, t1).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v1, t1, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Clean(k2, t1).error_code());
}

TEST_F(TxIndexImplTest, commit_not_exist) {
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k1, v2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t1).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v2, t2).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_CommitNotExist, ti->Commit(k1, t1).error_code());
}

TEST_F(TxIndexImplTest, commit_ok) {
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k1, v1, t1).error_code());
    t1.set_commit_ts(3);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k1, t1).error_code());

    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteLock(k2, v2, t2, std::bind(&TxIndexImplTest::dummyCallback, this)).error_code());
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->WriteIntent(k2, v2, t2).error_code());
    t2.set_commit_ts(4);
    ASSERT_EQ(azino::TxOpStatus_Code_Ok, ti->Commit(k2, t2).error_code());
}

TEST_F(TxIndexImplTest, read_ok) {

}

TEST_F(TxIndexImplTest, read_block) {

}

TEST_F(TxIndexImplTest, read_not_exist) {

}

TEST_F(TxIndexImplTest, read_deleted) {

}