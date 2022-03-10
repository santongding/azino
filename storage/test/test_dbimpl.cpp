#include <gtest/gtest.h>
#include <leveldb/db.h>
#include <string>

#include "db.h"

azino::storage::DB* db;

class DBImplTest : public testing::Test {
public:

protected:
    void SetUp() {
        db = azino::storage::DB::DefaultDB();
        db->Open("TestDB");
    }
    void TearDown() {
        leveldb::Options opt;
        leveldb::DestroyDB("TestDB", opt);
    }
};

TEST_F(DBImplTest, crud) {
    ASSERT_TRUE(db->Put("hello", "world").ok());
    std::string s;
    ASSERT_TRUE(db->Get("hello", s).ok());
    ASSERT_EQ("world", s);
    ASSERT_TRUE(db->Delete("hello").ok());
    ASSERT_TRUE(db->Get("hello", s).IsNotFound());
    ASSERT_TRUE(db->Put("de", "ll").ok());
    delete db;
    db = azino::storage::DB::DefaultDB();
    db->Open("TestDB");
    ASSERT_TRUE(db->Get("de", s).ok());
    ASSERT_EQ("ll", s);
    delete db;
}