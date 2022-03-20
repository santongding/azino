#include <gtest/gtest.h>
#include <leveldb/db.h>
#include <string>

#include "storage.h"

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