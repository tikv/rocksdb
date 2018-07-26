#include "utilities/titandb/blob_file_cache.h"

#include "util/filename.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace titandb {

namespace {

void DeleteValue(const Slice&, void* value) {
  delete reinterpret_cast<BlobFileReader*>(value);
}

Slice FileNumberSlice(const uint64_t* number) {
  return Slice(reinterpret_cast<const char*>(number), sizeof(*number));
}

}

BlobFileCache::BlobFileCache(const DBOptions& db_options,
                             const TitanDBOptions& tdb_options)
    : db_options_(db_options),
      tdb_options_(tdb_options),
      env_(db_options.env),
      env_options_(db_options),
      cache_(NewLRUCache(tdb_options.max_open_files)) {}

Status BlobFileCache::Get(const ReadOptions& options,
                          uint64_t file_number,
                          uint64_t file_size,
                          const BlobHandle& handle,
                          BlobRecord* record, std::string* buffer) {
  Cache::Handle* ch = nullptr;
  Status s = FindFile(file_number, file_size, &ch);
  if (!s.ok()) return s;

  auto reader = reinterpret_cast<BlobFileReader*>(cache_->Value(ch));
  s = reader->Get(options, handle, record, buffer);
  cache_->Release(ch);
  return s;
}

Status BlobFileCache::NewReader(const ReadOptions& options,
                                uint64_t file_number,
                                uint64_t file_size,
                                std::unique_ptr<BlobFileReader>* result) {
  Cache::Handle* ch = nullptr;
  Status s = FindFile(file_number, file_size, &ch);
  if (!s.ok()) return s;

  auto reader = reinterpret_cast<BlobFileReader*>(cache_->Value(ch));
  auto blob_file = reader->blob_file();
  cache_->Release(ch);

  std::unique_ptr<RandomAccessFileReader> file;
  s = NewRandomAccessReader(file_number, options.readahead_size, &file);
  if (!s.ok()) return s;

  result->reset(new BlobFileReader(tdb_options_, blob_file, std::move(file)));
  return s;
}

void BlobFileCache::Evict(uint64_t file_number) {
  cache_->Erase(FileNumberSlice(&file_number));
}

Status BlobFileCache::FindFile(uint64_t file_number,
                               uint64_t file_size,
                               Cache::Handle** handle) {
  Status s;
  Slice number = FileNumberSlice(&file_number);
  *handle = cache_->Lookup(number);
  if (*handle) return s;

  std::unique_ptr<RandomAccessFileReader> file;
  s = NewRandomAccessReader(file_number, 0, &file);
  if (!s.ok()) return s;

  std::unique_ptr<BlobFileReader> reader;
  s = BlobFileReader::Open(tdb_options_, std::move(file), file_size, &reader);
  if (!s.ok()) return s;

  cache_->Insert(number, reader.release(), 1, &DeleteValue, handle);
  return s;
}

Status BlobFileCache::NewRandomAccessReader(
    uint64_t file_number, uint64_t readahead_size,
    std::unique_ptr<RandomAccessFileReader>* result) {
  std::unique_ptr<RandomAccessFile> file;
  auto file_name = BlobFileName(tdb_options_.dirname, file_number);
  Status s = env_->NewRandomAccessFile(file_name, &file, env_options_);
  if (!s.ok()) return s;

  if (readahead_size > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
  }
  result->reset(new RandomAccessFileReader(std::move(file), file_name));
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
