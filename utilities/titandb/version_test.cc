#include "util/testharness.h"
#include "utilities/titandb/test_util.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_edit.h"
#include "utilities/titandb/version_builder.h"

namespace rocksdb {
namespace titandb {

class VersionTest : public testing::Test {
 public:
  VersionTest() : list_(nullptr) {
    AppendVersion(new Version(nullptr));
  }

  ~VersionTest() {
    for (auto v : referenced_) {
      v->Unref();
    }
    current_->Unref();
    assert(list_.next_ == &list_);
    assert(list_.prev_ == &list_);
  }

  Version* GetAndRefCurrent() {
    auto v = current_;
    v->Ref();
    referenced_.push_back(v);
    return v;
  }

  void AddBlobFiles(uint64_t start_number, uint64_t end_number) {
    auto v = new Version(nullptr);
    v->files_ = current_->files_;
    for (auto i = start_number; i < end_number; i++) {
      BlobFileMeta file;
      file.file_number = i;
      v->files_.emplace(i, file);
    }
    AppendVersion(v);
  }

  void DeleteBlobFiles(uint64_t start_number, uint64_t end_number) {
    auto v = new Version(nullptr);
    v->files_ = current_->files_;
    for (auto i = start_number; i < end_number; i++) {
      v->files_.erase(i);
    }
    AppendVersion(v);
  }

  void BuildAndCheckCurrent(Version* base,
                            const std::vector<VersionEdit*> edit_list) {
    VersionBuilder builder(base);
    for (auto& edit : edit_list) {
      builder.Apply(edit);
    }
    Version v(nullptr);
    builder.SaveTo(&v);
    ASSERT_EQ(v.files_, current_->files_);
  }

 private:
  void AppendVersion(Version* v) {
    if (current_) {
      current_->Unref();
    }

    current_ = v;
    current_->Ref();

    v->prev_ = list_.prev_;
    v->next_ = &list_;
    v->prev_->next_ = v;
    v->next_->prev_ = v;
  }

  Version list_;
  Version* current_ {nullptr};
  std::vector<Version*> referenced_;
};

TEST_F(VersionTest, VersionEdit) {
  VersionEdit input;
  CheckCodec(input);
  input.SetNextFileNumber(1);
  CheckCodec(input);
  BlobFileMeta file1;
  file1.file_number = 3;
  file1.file_size = 4;
  BlobFileMeta file2;
  file2.file_number = 5;
  file2.file_size = 6;
  input.AddBlobFile(file1);
  input.AddBlobFile(file2);
  input.DeleteBlobFile(7);
  input.DeleteBlobFile(8);
  CheckCodec(input);
}

TEST_F(VersionTest, VersionBuilder) {
  auto base = GetAndRefCurrent();
  BuildAndCheckCurrent(base, {});

  // Add files [0, 4)
  VersionEdit add_0_4;
  for (uint64_t i = 0; i < 4; i++) {
    BlobFileMeta file;
    file.file_number = i;
    add_0_4.AddBlobFile(file);
  }
  AddBlobFiles(0, 4);
  BuildAndCheckCurrent(base, {&add_0_4});
  auto base_0_4 = GetAndRefCurrent();

  // Add files [4, 8)
  VersionEdit add_4_8;
  for (uint64_t i = 4; i < 8; i++) {
    BlobFileMeta file;
    file.file_number = i;
    add_4_8.AddBlobFile(file);
  }
  AddBlobFiles(4, 8);
  BuildAndCheckCurrent(base, {&add_0_4, &add_4_8});
  BuildAndCheckCurrent(base_0_4, {&add_4_8});
  auto base_0_8 = GetAndRefCurrent();

  // Delete files [4, 6)
  VersionEdit del_4_6;
  for (uint64_t i = 4; i < 6; i++) {
    del_4_6.DeleteBlobFile(i);
  }
  // Delete files [6, 8)
  VersionEdit del_6_8;
  for (uint64_t i = 6; i < 8; i++) {
    del_6_8.DeleteBlobFile(i);
  }
  DeleteBlobFiles(4, 8);

  BuildAndCheckCurrent(base, {&add_0_4});
  BuildAndCheckCurrent(base, {&add_0_4, &add_4_8, &del_4_6, &del_6_8});
  BuildAndCheckCurrent(base_0_4, {});
  BuildAndCheckCurrent(base_0_4, {&add_4_8, &del_4_6, &del_6_8});
  BuildAndCheckCurrent(base_0_8, {&del_4_6, &del_6_8});
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
