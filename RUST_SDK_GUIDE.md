# ST-RocksDB Rust SDK 使用指南

> 基于 TiKV 优化的 RocksDB Rust 绑定

## 快速开始

### 生成 SDK

```bash
./scripts/package_rust_sdk.sh 0.1.0
./scripts/demo_rust_sdk.sh  # 完整演示
```

### 使用 SDK

```toml
[dependencies]
st-rocksdb = { path = "../st-rocksdb-rust-0.1.0", features = ["static-link"] }
```

```rust
use st_rocksdb::{DB, Options};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    
    let db = DB::open(&opts, "/path/to/db")?;
    
    // 基本操作
    db.put(b"key", b"value")?;
    let value = db.get(b"key")?.unwrap();
    db.delete(b"key")?;
    
    Ok(())
}
```

## 核心特性

- **TiKV 优化**: 基于 TiKV 的性能改进
- **类型安全**: Rust 内存安全保证
- **双构建模式**: 静态链接(推荐) / 动态链接
- **跨平台**: macOS / Linux 支持

## API 概览

### 基本操作
```rust
let db = DB::open(&opts, path)?;
db.put(key, value)?;
let result = db.get(key)?;
db.delete(key)?;
```

### 批量操作
```rust
let mut batch = WriteBatch::default();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
db.write(&batch)?;
```

### 配置优化
```rust
let mut opts = Options::default();
opts.set_max_open_files(1000);
opts.set_write_buffer_size(64 * 1024 * 1024);
opts.set_compression_type(CompressionType::LZ4);
```

## 构建配置

### 静态链接 (推荐)
```toml
st-rocksdb = { version = "0.1.0", features = ["static-link"] }
```
- ✅ 无运行时依赖
- ❌ 编译时间长

### 动态链接
```toml
st-rocksdb = { version = "0.1.0", features = ["shared-link"] }
```
- ✅ 编译快
- ❌ 需要预安装 RocksDB

## 环境设置

```bash
export ROCKSDB_LIB_DIR=/path/to/lib
export ROCKSDB_INCLUDE_DIR=/path/to/include
```

## 故障排除

### 编译错误
```bash
# 设置环境变量
export ROCKSDB_LIB_DIR=/path/to/lib

# 或使用静态链接
cargo build --features static-link
```

### bindgen 错误
```bash
# Ubuntu/Debian
sudo apt install clang libclang-dev

# macOS
brew install llvm
```

## 性能测试

```bash
cargo bench                    # 运行基准测试
cargo test                     # 运行单元测试
cargo run --example simple     # 运行示例
```

## 发布

```bash
# 发布 sys crate
cd librocksdb-sys && cargo publish

# 发布主 crate
cd .. && cargo publish
```

## 路线图

**v0.1.x**: 基本操作、批量写入、错误处理  
**v0.2.x**: 列族、迭代器、快照、事务  
**v1.0.x**: 完整事务、异步API、分布式功能 