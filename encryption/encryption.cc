#include "rocksdb/encryption.h"

#include "util/string_util.h"

namespace rocksdb {
namespace encryption {

Status AESBlockCipher::InitKey(const std::string& key) {
  int ret =
      AES_set_encrypt_key(reinterpret_cast<const unsigned char*>(key.data()),
                          static_cast<int>(key.size()), &encrypt_key_);
  if (ret != 0) {
    return Status::InvalidArgument("AES set encrypt key error: " +
                                   ToString(ret));
  }
  ret = AES_set_decrypt_key(reinterpret_cast<const unsigned char*>(key.data()),
                            static_cast<int>(key.size()), &decrypt_key_);
  if (ret != 0) {
    return Status::InvalidArgument("AES set decrypt key error: " +
                                   ToString(ret));
  }
  return Status::OK();
}

Status NewAESCTRCipherStream(EncryptionMethod method, const std::string& key,
                             const std::string& iv,
                             std::unique_ptr<AESCTRCipherStream>* result) {
  assert(result != nullptr);
  size_t key_size = 0;
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
      key_size = 16;
      break;
    case EncryptionMethod::kAES192_CTR:
      key_size = 24;
      break;
    case EncryptionMethod::kAES256_CTR:
      key_size = 32;
      break;
    default:
      return Status::InvalidArgument("Unsupported encryption method: " +
                                     ToString(static_cast<int>(method)));
  }
  if (key.size() != key_size) {
    return Status::InvalidArgument("Encryption key size mismatch. " +
                                   ToString(key.size()) + "(actual) vs. " +
                                   ToString(key_size) + "(expected).");
  }
  if (iv.size() != AES_BLOCK_SIZE) {
    return Status::InvalidArgument(
        "iv size not equal to block cipher block size: " + ToString(iv.size()) +
        "(actual) vs. " + ToString(AES_BLOCK_SIZE) + "(expected).");
  }
  std::unique_ptr<AESCTRCipherStream> cipher_stream(new AESCTRCipherStream(iv));
  Status s = cipher_stream->InitKey(key);
  if (!s.ok()) {
    return s;
  }
  *result = std::move(cipher_stream);
  return Status::OK();
}

Status AESEncryptionProvider::CreateCipherStream(
    const std::string& fname, const EnvOptions& /*options*/, Slice& /*prefix*/,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  assert(result != nullptr);
  FileInfo file_info;
  Status s = key_manager_->GetInfoForFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<AESCTRCipherStream> cipher_stream;
  s = NewAESCTRCipherStream(file_info.method, file_info.key, file_info.iv,
                            &cipher_stream);
  if (!s.ok()) {
    return s;
  }
  *result = std::move(cipher_stream);
  return Status::OK();
}

Status KeyManagedEncryptedEnv::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options) {
  FileInfo file_info;
  Status s = key_manager_->GetInfoForFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      return target()->NewSequentialFile(fname, result, options);
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return encrypted_env_->NewSequentialFile(fname, result, options);
    default:
      return Status::InvalidArgument(
          "Unsupported encryption method: " +
          ToString(static_cast<int>(file_info.method)));
  }
}

Status KeyManagedEncryptedEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  FileInfo file_info;
  Status s = key_manager_->GetInfoForFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      return target()->NewRandomAccessFile(fname, result, options);
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return encrypted_env_->NewRandomAccessFile(fname, result, options);
    default:
      return Status::InvalidArgument(
          "Unsupported encryption method: " +
          ToString(static_cast<int>(file_info.method)));
  }
}

Status KeyManagedEncryptedEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  printf("encrypted env new writable\n");
  FileInfo file_info;
  Status s = key_manager_->NewFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      return target()->NewWritableFile(fname, result, options);
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return encrypted_env_->NewWritableFile(fname, result, options);
    default:
      return Status::InvalidArgument(
          "Unsupported encryption method: " +
          ToString(static_cast<int>(file_info.method)));
  }
}

Status KeyManagedEncryptedEnv::ReopenWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  FileInfo file_info;
  Status s = key_manager_->GetInfoForFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      return target()->ReopenWritableFile(fname, result, options);
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return encrypted_env_->ReopenWritableFile(fname, result, options);
    default:
      return Status::InvalidArgument(
          "Unsupported encryption method: " +
          ToString(static_cast<int>(file_info.method)));
  }
}

Status KeyManagedEncryptedEnv::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    std::unique_ptr<WritableFile>* result, const EnvOptions& options) {
  FileInfo file_info;
  Status s = key_manager_->NewFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      return target()->ReuseWritableFile(fname, old_fname, result, options);
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return encrypted_env_->ReuseWritableFile(fname, old_fname, result,
                                               options);
    default:
      return Status::InvalidArgument(
          "Unsupported encryption method: " +
          ToString(static_cast<int>(file_info.method)));
  }
}

Status KeyManagedEncryptedEnv::NewRandomRWFile(
    const std::string& fname, std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) {
  FileInfo file_info;
  Status s = key_manager_->NewFile(fname, &file_info);
  if (!s.ok()) {
    return s;
  }
  switch (file_info.method) {
    case EncryptionMethod::kPlaintext:
      return target()->NewRandomRWFile(fname, result, options);
    case EncryptionMethod::kAES128_CTR:
    case EncryptionMethod::kAES192_CTR:
    case EncryptionMethod::kAES256_CTR:
      return encrypted_env_->NewRandomRWFile(fname, result, options);
    default:
      return Status::InvalidArgument(
          "Unsupported encryption method: " +
          ToString(static_cast<int>(file_info.method)));
  }
}

Env* NewKeyManagedEncryptedEnv(Env* base_env,
                               std::shared_ptr<KeyManager>& key_manager) {
  std::unique_ptr<AESEncryptionProvider> provider(
      new AESEncryptionProvider(key_manager.get()));
  std::unique_ptr<Env> encrypted_env(NewEncryptedEnv(base_env, provider.get()));
  return new KeyManagedEncryptedEnv(base_env, key_manager, std::move(provider),
                                    std::move(encrypted_env));
}

}  // namespace encryption
}  // namespace rocksdb
