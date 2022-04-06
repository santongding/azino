#include <gtest/gtest.h>
#include <leveldb/db.h>
#include <string>

#include "storage.h"
#include "utils.h"

azino::storage::Storage* storage;

class DBImplTest : public testing::Test {
public:

protected:
    void SetUp() {
        storage = azino::storage::Storage::DefaultStorage();
        storage->Open("TestDB");
    }
    void TearDown() {
        leveldb::Options opt;
        leveldb::DestroyDB("TestDB", opt);
    }
};

TEST_F(DBImplTest, crud) {

    std::string seeked_key;

    ASSERT_EQ(azino::storage::convertPrefix("test"),"MVCCKEY_test");
    ASSERT_EQ(azino::storage::convert("test",0,0),"MVCCKEY_test_ffffffffffffffff_0");
    ASSERT_EQ(azino::storage::convert("",~0,1),"MVCCKEY__0000000000000000_1");

    auto mvcc_key = azino::storage::convert("mvcc",233,0);
    ASSERT_TRUE(storage->Put(mvcc_key, "world").error_code() == azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->Seek(azino::storage::convert("mvcc",234,0),seeked_key).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_key, mvcc_key);



    ASSERT_TRUE(storage->Seek("seek",seeked_key).error_code()==azino::storage::StorageStatus_Code_NotFound);
    ASSERT_TRUE(storage->Put("seek1", "world").error_code() == azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->Seek("seek",seeked_key).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_key,"seek1");
    ASSERT_TRUE(storage->Put("seek", "world").error_code() == azino::storage::StorageStatus_Code_Ok);
    ASSERT_TRUE(storage->Seek("seek",seeked_key).error_code()==azino::storage::StorageStatus_Code_Ok);
    ASSERT_EQ(seeked_key,"seek");
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Delete("seek").error_code());
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Delete("seek1").error_code());
    ASSERT_TRUE(storage->Seek("seek",seeked_key).error_code()==azino::storage::StorageStatus_Code_NotFound);

    ASSERT_TRUE(storage->Put("hello", "world").error_code() == azino::storage::StorageStatus_Code_Ok);
    std::string s;
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Get("hello", s).error_code());
    ASSERT_EQ("world", s);
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Delete("hello").error_code());
    ASSERT_EQ(azino::storage::StorageStatus_Code_NotFound, storage->Get("hello", s).error_code());
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Put("de", "ll").error_code());
    delete storage;
    storage = azino::storage::Storage::DefaultStorage();
    storage->Open("TestDB");
    ASSERT_EQ(azino::storage::StorageStatus_Code_Ok, storage->Get("de", s).error_code());
    ASSERT_EQ("ll", s);
    delete storage;
}