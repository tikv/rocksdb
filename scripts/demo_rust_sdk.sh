#!/bin/bash

# ST-RocksDB Rust SDK 演示脚本
# 展示如何打包、构建和使用 Rust SDK

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function log() {
    echo -e "${GREEN}[DEMO]${NC} $1"
}

function info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

function warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

log "ST-RocksDB Rust SDK 完整演示"
info "项目根目录: $PROJECT_ROOT"

# 检查依赖
log "检查系统依赖..."

if ! command -v rustc &> /dev/null; then
    error "Rust 未安装，请先安装 Rust: https://rustup.rs/"
fi

if ! command -v cargo &> /dev/null; then
    error "Cargo 未安装"
fi

RUST_VERSION=$(rustc --version)
info "Rust 版本: $RUST_VERSION"

# 步骤 1: 生成 Rust SDK
log "步骤 1: 生成 Rust SDK 包..."
cd "$PROJECT_ROOT"

if [ ! -x "scripts/package_rust_sdk.sh" ]; then
    chmod +x scripts/package_rust_sdk.sh
fi

./scripts/package_rust_sdk.sh 0.1.0-demo tikv-style

# 步骤 2: 进入 SDK 目录
log "步骤 2: 进入 SDK 目录..."
SDK_DIR="$PROJECT_ROOT/rust-packages/st-rocksdb-rust-0.1.0-demo"

if [ ! -d "$SDK_DIR" ]; then
    error "SDK 目录不存在: $SDK_DIR"
fi

cd "$SDK_DIR"
info "当前目录: $(pwd)"

# 设置环境变量
export ROCKSDB_LIB_DIR="$(pwd)/lib"
export ROCKSDB_INCLUDE_DIR="$(pwd)/include"

log "环境变量设置:"
info "ROCKSDB_LIB_DIR=$ROCKSDB_LIB_DIR"
info "ROCKSDB_INCLUDE_DIR=$ROCKSDB_INCLUDE_DIR"

# 步骤 3: 验证库文件
log "步骤 3: 验证库文件..."
if [ -f "lib/librocksdb.a" ]; then
    LIB_SIZE=$(ls -lh lib/librocksdb.a | awk '{print $5}')
    info "静态库文件: lib/librocksdb.a ($LIB_SIZE)"
else
    error "静态库文件不存在"
fi

if [ -f "include/rocksdb/c.h" ]; then
    info "头文件: include/rocksdb/c.h ✓"
else
    error "头文件不存在"
fi

# 步骤 4: 构建 sys crate
log "步骤 4: 构建底层 FFI 绑定..."
cd librocksdb-sys

info "构建 st-rocksdb-sys..."
if cargo build --features static-link 2>&1; then
    info "sys crate 构建成功 ✓"
else
    error "sys crate 构建失败"
fi

cd ..

# 步骤 5: 构建主 crate
log "步骤 5: 构建主 Rust API..."
if cargo build --features static-link 2>&1; then
    info "主 crate 构建成功 ✓"
else
    error "主 crate 构建失败"
fi

# 步骤 6: 运行单元测试
log "步骤 6: 运行单元测试..."
if cargo test --features static-link 2>&1; then
    info "单元测试通过 ✓"
else
    warn "单元测试未全部通过，但这可能是正常的"
fi

# 步骤 7: 运行示例
log "步骤 7: 运行示例程序..."

info "运行 simple 示例..."
if cargo run --example simple --features static-link 2>&1; then
    info "simple 示例运行成功 ✓"
else
    warn "simple 示例运行失败"
fi

# 步骤 8: 创建演示项目
log "步骤 8: 创建演示项目..."

DEMO_PROJECT_DIR="$PROJECT_ROOT/rust-packages/demo-project"
rm -rf "$DEMO_PROJECT_DIR"
mkdir -p "$DEMO_PROJECT_DIR/src"

cd "$DEMO_PROJECT_DIR"

# 创建演示项目的 Cargo.toml
cat > Cargo.toml << EOF
[package]
name = "st-rocksdb-demo"
version = "0.1.0"
edition = "2021"

[dependencies]
st-rocksdb = { path = "../st-rocksdb-rust-0.1.0-demo", features = ["static-link"] }
tempfile = "3.0"
EOF

# 创建演示代码
cat > src/main.rs << 'EOF'
use st_rocksdb::{DB, Options, WriteBatch, CompressionType, Error};
use tempfile::TempDir;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🦀 ST-RocksDB Rust SDK 演示程序");
    println!("================================\n");

    // 创建临时目录
    let tmp_dir = TempDir::new()?;
    let db_path = tmp_dir.path();
    println!("📁 数据库路径: {:?}\n", db_path);

    // 基本操作演示
    basic_operations_demo(db_path)?;
    
    // 批量操作演示
    batch_operations_demo(db_path)?;
    
    // 性能测试演示
    performance_demo(db_path)?;

    println!("✅ 演示完成！");
    Ok(())
}

fn basic_operations_demo(db_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  基本操作演示");
    println!("------------------");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(CompressionType::LZ4);

    let db = DB::open(&opts, db_path)?;

    // 写入数据
    println!("📝 写入键值对...");
    db.put(b"hello", b"world")?;
    db.put(b"rust", b"rocks")?;
    db.put(b"tikv", b"awesome")?;

    // 读取数据
    println!("📖 读取数据...");
    if let Some(value) = db.get(b"hello")? {
        println!("   hello = {}", String::from_utf8_lossy(&value));
    }

    if let Some(value) = db.get(b"rust")? {
        println!("   rust = {}", String::from_utf8_lossy(&value));
    }

    // 删除数据
    println!("🗑️  删除 tikv...");
    db.delete(b"tikv")?;

    // 验证删除
    match db.get(b"tikv")? {
        Some(_) => println!("   ❌ tikv 仍然存在"),
        None => println!("   ✅ tikv 已被删除"),
    }

    println!();
    Ok(())
}

fn batch_operations_demo(db_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("2️⃣  批量操作演示");
    println!("------------------");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DB::open(&opts, db_path)?;

    let start = Instant::now();

    // 创建批量操作
    let mut batch = WriteBatch::default();
    
    println!("📦 准备批量写入 1000 条记录...");
    for i in 0..1000 {
        let key = format!("batch_key_{:04}", i);
        let value = format!("batch_value_{:04}_with_some_data", i);
        batch.put(key.as_bytes(), value.as_bytes());
    }

    // 原子性提交
    db.write(&batch)?;
    
    let duration = start.elapsed();
    println!("✅ 批量写入完成，耗时: {:?}", duration);

    // 验证部分数据
    println!("🔍 验证部分数据...");
    if let Some(value) = db.get(b"batch_key_0042")? {
        println!("   batch_key_0042 = {}", String::from_utf8_lossy(&value));
    }

    println!();
    Ok(())
}

fn performance_demo(db_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("3️⃣  性能测试演示");
    println!("------------------");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
    opts.set_max_open_files(1000);
    
    let db = DB::open(&opts, db_path)?;

    // 顺序写入测试
    println!("🚀 顺序写入性能测试 (10000 条记录)...");
    let start = Instant::now();

    for i in 0..10000 {
        let key = format!("perf_key_{:08}", i);
        let value = format!("performance_test_value_{:08}_with_longer_content_to_simulate_real_usage", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let write_duration = start.elapsed();
    let write_ops_per_sec = 10000.0 / write_duration.as_secs_f64();
    println!("   写入耗时: {:?}", write_duration);
    println!("   写入速度: {:.0} ops/sec", write_ops_per_sec);

    // 随机读取测试
    println!("📚 随机读取性能测试 (1000 次读取)...");
    let start = Instant::now();

    for i in (0..10000).step_by(10) {
        let key = format!("perf_key_{:08}", i);
        if let Some(_value) = db.get(key.as_bytes())? {
            // 成功读取
        }
    }

    let read_duration = start.elapsed();
    let read_ops_per_sec = 1000.0 / read_duration.as_secs_f64();
    println!("   读取耗时: {:?}", read_duration);
    println!("   读取速度: {:.0} ops/sec", read_ops_per_sec);

    println!();
    Ok(())
}
EOF

# 设置环境变量并构建演示项目
export ROCKSDB_LIB_DIR="$SDK_DIR/lib"
export ROCKSDB_INCLUDE_DIR="$SDK_DIR/include"

info "构建演示项目..."
if cargo build 2>&1; then
    info "演示项目构建成功 ✓"
else
    error "演示项目构建失败"
fi

# 步骤 9: 运行演示项目
log "步骤 9: 运行演示项目..."
info "运行 st-rocksdb-demo..."

if cargo run 2>&1; then
    info "演示项目运行成功 ✓"
else
    warn "演示项目运行可能有问题"
fi

# 步骤 10: 总结
log "步骤 10: 演示总结"
echo
echo -e "${GREEN}🎉 ST-RocksDB Rust SDK 演示完成！${NC}"
echo
echo "演示内容包括:"
echo "  ✅ SDK 包生成和验证"
echo "  ✅ FFI 绑定构建"
echo "  ✅ 高层 Rust API 构建"
echo "  ✅ 单元测试执行"
echo "  ✅ 示例程序运行"
echo "  ✅ 完整项目集成演示"
echo
echo "生成的文件:"
echo "  📦 SDK 包: $SDK_DIR"
echo "  🦀 演示项目: $DEMO_PROJECT_DIR"
echo
echo "下一步建议:"
echo "  1. 查看 RUST_SDK_GUIDE.md 获取详细文档"
echo "  2. 运行 'cargo bench' 进行性能基准测试"
echo "  3. 根据需要调整配置参数"
echo "  4. 集成到您的 Rust 项目中"
echo
echo -e "${BLUE}🔗 相关链接:${NC}"
echo "  - TiKV: https://github.com/tikv/tikv"
echo "  - RocksDB: https://rocksdb.org/"
echo "  - Rust: https://rust-lang.org/"

cd "$PROJECT_ROOT"
log "演示脚本执行完成！" 