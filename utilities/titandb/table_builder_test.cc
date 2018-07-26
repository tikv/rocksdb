#include "util/testharness.h"
#include "util/filename.h"
#include "table/table_reader.h"
#include "table/block_based_table_reader.h"
#include "table/block_based_table_builder.h"
#include "utilities/titandb/table_builder.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_manager.h"

namespace rocksdb {
namespace titandb {

const uint64_t kTestFileNumber = 123;

class FileManager : public BlobFileManager {
 public:
  FileManager(const DBOptions& db_options, const TitanDBOptions& tdb_options)
      : env_(db_options.env),
        env_options_(db_options),
        tdb_options_(tdb_options) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle) override {
    auto number = kTestFileNumber;
    auto name = BlobFileName(tdb_options_.dirname, number);
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      Status s = env_->NewWritableFile(name, &f, env_options_);
      if (!s.ok()) return s;
      file.reset(new WritableFileWriter(std::move(f), env_options_));
    }
    handle->reset(new FileHandle(number, name, std::move(file)));
    return Status::OK();
  }

  Status FinishFile(const BlobFileMeta& /*file*/,
                    std::unique_ptr<BlobFileHandle> handle) {
    Status s = handle->file()->Sync(true);
    if (s.ok()) {
      s = handle->file()->Close();
    }
    return s;
  }

  Status DeleteFile(std::unique_ptr<BlobFileHandle> handle) {
    return env_->DeleteFile(handle->name());
  }

 private:
  class FileHandle : public BlobFileHandle {
   public:
    FileHandle(uint64_t _number,
               const std::string& _name,
               std::unique_ptr<WritableFileWriter> _file)
        : number_(_number), name_(_name), file_(std::move(_file)) {}

    uint64_t number() const override { return number_; }

    const std::string& name() const override { return name_; }

    WritableFileWriter* file() const override { return file_.get(); }

   private:
    friend class FileManager;

    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  Env* env_;
  EnvOptions env_options_;
  TitanDBOptions tdb_options_;
};

class TableBuilderTest : public testing::Test {
 public:
  TableBuilderTest()
      : env_(db_options_.env),
        env_options_(db_options_),
        tmpdir_(test::TmpDir(env_)),
        base_name_(tmpdir_ + "/base"),
        blob_name_(BlobFileName(tmpdir_, kTestFileNumber)) {
    tdb_options_.dirname = tmpdir_;
    blob_manager_.reset(new FileManager(db_options_, tdb_options_));
  }

  ~TableBuilderTest() {
    env_->DeleteFile(base_name_);
    env_->DeleteFile(blob_name_);
    env_->DeleteDir(tmpdir_);
  }

  void NewFileWriter(const std::string& fname,
                     std::unique_ptr<WritableFileWriter>* result) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(fname, &file, env_options_));
    result->reset(new WritableFileWriter(std::move(file), env_options_));
  }

  void NewFileReader(const std::string& fname,
                     std::unique_ptr<RandomAccessFileReader>* result) {
    std::unique_ptr<RandomAccessFile> file;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &file, env_options_));
    result->reset(new RandomAccessFileReader(std::move(file), fname, env_));
  }

  void NewBaseFileWriter(std::unique_ptr<WritableFileWriter>* result) {
    NewFileWriter(base_name_, result);
  }

  void NewBaseFileReader(std::unique_ptr<RandomAccessFileReader>* result) {
    NewFileReader(base_name_, result);
  }

  void NewBlobFileReader(std::unique_ptr<BlobFileReader>* result) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(blob_name_, &file);
    std::unique_ptr<BlobFile> blob_file;
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(blob_name_, &file_size));
    ASSERT_OK(BlobFileReader::Open(
        tdb_options_, std::move(file), file_size, result));
  }

  void NewTitanTableBuilder(std::unique_ptr<TableBuilder> base_builder,
                            std::unique_ptr<TitanTableBuilder>* result) {
    result->reset(new TitanTableBuilder(tdb_options_, 0,
                                        std::move(base_builder),
                                        blob_manager_));
  }

  DBOptions db_options_;
  TitanDBOptions tdb_options_;
  Env* env_;
  EnvOptions env_options_;
  std::string tmpdir_;
  std::string base_name_;
  std::string blob_name_;
  std::shared_ptr<BlobFileManager> blob_manager_;
};

TEST_F(TableBuilderTest, Basic) {
  Options options;
  ColumnFamilyOptions cf_options;
  ImmutableCFOptions cf_ioptions(options);
  MutableCFOptions cf_moptions(cf_options);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>> collectors;

  BlockBasedTableOptions block_table_options;
  std::unique_ptr<TableFactory> base_factory(
      NewBlockBasedTableFactory(block_table_options));

  std::unique_ptr<TableBuilder> base_builder;
  std::unique_ptr<WritableFileWriter> base_file;
  {
    TableBuilderOptions table_builder_options(
        cf_ioptions, cf_moptions, cf_ioptions.internal_comparator,
        &collectors, kNoCompression, CompressionOptions(), nullptr,
        false, "default", 0);
    NewBaseFileWriter(&base_file);
    base_builder.reset(base_factory->NewTableBuilder(
        table_builder_options, 0, base_file.get()));
  }

  std::unique_ptr<TitanTableBuilder> table_builder;
  NewTitanTableBuilder(std::move(base_builder), &table_builder);

  const int n = 100;
  for (char i = 0; i < n; i++) {
    std::string key(1, i);
    InternalKey ikey(key, 1, kTypeValue);
    std::string value;
    if (i % 2 == 0) {
      value = std::string(1, i);
    } else {
      value = std::string(tdb_options_.min_blob_size, i);
    }
    table_builder->Add(ikey.Encode(), value);
  }
  ASSERT_OK(table_builder->Finish());
  ASSERT_OK(base_file->Sync(true));
  ASSERT_OK(base_file->Close());

  std::unique_ptr<TableReader> base_reader;
  {
    TableReaderOptions table_reader_options(
        cf_ioptions, nullptr, env_options_,
        cf_ioptions.internal_comparator);
    std::unique_ptr<RandomAccessFileReader> file;
    NewBaseFileReader(&file);
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    ASSERT_OK(base_factory->NewTableReader(
        table_reader_options, std::move(file), file_size, &base_reader));
  }

  std::unique_ptr<BlobFileReader> blob_reader;
  NewBlobFileReader(&blob_reader);

  std::unique_ptr<InternalIterator> iter(
      base_reader->NewIterator(ReadOptions(), nullptr));
  iter->SeekToFirst();
  for (char i = 0; i < n; i++) {
    ASSERT_TRUE(iter->Valid());
    std::string key(1, i);
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    ASSERT_EQ(ikey.user_key, key);
    if (i % 2 == 0) {
      ASSERT_EQ(ikey.type, kTypeValue);
      ASSERT_EQ(iter->value(), std::string(1, i));
    } else {
      ASSERT_EQ(ikey.type, kTypeBlobIndex);
      BlobIndex index;
      ASSERT_OK(DecodeInto(iter->value(), &index));
      ASSERT_EQ(index.file_number, kTestFileNumber);
      BlobRecord record;
      std::string buffer;
      ASSERT_OK(blob_reader->Get(ReadOptions(), index.blob_handle,
                                 &record, &buffer));
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, std::string(tdb_options_.min_blob_size, i));
    }
    iter->Next();
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
