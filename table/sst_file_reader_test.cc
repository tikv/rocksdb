//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include <stdint.h>
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based_table_factory.h"
#include "table/table_builder.h"
#include "util/file_reader_writer.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

const uint32_t optLength = 100;

namespace {

static std::string MakeKey(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "k_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

static std::string MakeValue(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "v_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

void createSST(const std::string& file_name) {
  std::shared_ptr<rocksdb::TableFactory> tf;
  tf.reset(new rocksdb::BlockBasedTableFactory(BlockBasedTableOptions()));

  unique_ptr<WritableFile> file;
  Env* env = Env::Default();
  EnvOptions env_options;
  ReadOptions read_options;
  Options opts;
  const ImmutableCFOptions imoptions(opts);
  rocksdb::InternalKeyComparator ikc(opts.comparator);
  unique_ptr<TableBuilder> tb;

  env->NewWritableFile(file_name, &file, env_options);
  opts.table_factory = tf;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
      int_tbl_prop_collector_factories;
  unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), EnvOptions()));
  std::string column_family_name;
  int unknown_level = -1;
  tb.reset(opts.table_factory->NewTableBuilder(
      TableBuilderOptions(imoptions, ikc, &int_tbl_prop_collector_factories,
                          CompressionType::kNoCompression, CompressionOptions(),
                          nullptr /* compression_dict */,
                          false /* skip_filters */, column_family_name,
                          unknown_level),
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      file_writer.get()));

  // Populate slightly more than 1K keys
  uint32_t num_keys = 1024;
  for (uint32_t i = 0; i < num_keys; i++) {
    tb->Add(MakeKey(i), MakeValue(i));
  }
  tb->Finish();
  file_writer->Close();
}

void cleanup(const std::string& file_name) {
  Env* env = Env::Default();
  env->DeleteFile(file_name);
  std::string outfile_name = file_name.substr(0, file_name.length() - 4);
  outfile_name.append("_dump.txt");
  env->DeleteFile(outfile_name);
}

}  // namespace

// Test for sst file reader "raw" mode
class SstFileReaderTest : public testing::Test {
 public:
  SstFileReaderTest() {}

  ~SstFileReaderTest() {}
};

TEST_F(SstFileReaderTest, GetProperties) {
  std::string file_name = "rocksdb_sst_file_reader_test.sst";
  createSST(file_name);

  SstFileReader *reader = new SstFileReader(file_name, false);
  ASSERT_TRUE(reader->getStatus().ok());

  std::shared_ptr<const TableProperties> props;
  auto s = reader->ReadTableProperties(&props);
  ASSERT_TRUE(s.ok());
  cleanup(file_name);
  delete reader;
}

TEST_F(SstFileReaderTest, VerifyChecksum) {
  std::string file_name = "rocksdb_sst_file_reader_test.sst";
  createSST(file_name);

  SstFileReader *reader = new SstFileReader(file_name, false);
  ASSERT_TRUE(reader->getStatus().ok());

  auto s = reader->VerifyChecksum();
  ASSERT_TRUE(s.ok());
  cleanup(file_name);
  delete reader;
}

TEST_F(SstFileReaderTest, ReadSequential) {
  std::string file_name = "rocksdb_sst_file_reader_test.sst";
  createSST(file_name);

  SstFileReader *reader = new SstFileReader(file_name, false);
  ASSERT_TRUE(reader->getStatus().ok());

  uint64_t num = 10;
  auto s = reader->ReadSequential(num, false, std::string("k_0000"), true, std::string("k_0009"));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(reader->GetReadNumber() == num);

  cleanup(file_name);
  delete reader;
}

TEST_F(SstFileReaderTest, DumpTable) {
  std::string file_name = "rocksdb_sst_file_reader_test.sst";
  createSST(file_name);

  SstFileReader *reader = new SstFileReader(file_name, false);
  ASSERT_TRUE(reader->getStatus().ok());

  std::string dump_name = "rocksdb_sst_file_reader_test.dump";
  auto s = reader->DumpTable(dump_name);
  ASSERT_TRUE(s.ok());
  cleanup(file_name);
  cleanup(dump_name);
  delete reader;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as SstFileReader is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE  return RUN_ALL_TESTS();
