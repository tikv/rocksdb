#!/bin/bash

# ST-RocksDB C++ API/SDK 打包脚本
# 使用方法: ./scripts/package_sdk.sh [版本号] [打包类型] [平台]
# 示例: ./scripts/package_sdk.sh 1.0.0 dev macos

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function log() {
    echo -e "${GREEN}[+]${NC} $1"
}

function warn() {
    echo -e "${YELLOW}[!]${NC} $1"
}

function error() {
    echo -e "${RED}[!]${NC} $1"
    exit 1
}

function info() {
    echo -e "${BLUE}[i]${NC} $1"
}

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 默认参数
VERSION=${1:-"1.0.0"}
PACKAGE_TYPE=${2:-"release"}  # release, debug, dev
PLATFORM=${3:-"auto"}        # auto, linux, macos, windows
BUILD_TYPE=${4:-"both"}       # static, shared, both

# 自动检测平台
if [ "$PLATFORM" = "auto" ]; then
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        PLATFORM="linux"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        PLATFORM="macos"
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        PLATFORM="windows"
    else
        error "不支持的平台: $OSTYPE"
    fi
fi

# 包名和目录设置
PACKAGE_NAME="st-rocksdb-${VERSION}-${PACKAGE_TYPE}-${PLATFORM}"
PACKAGE_DIR="$PROJECT_ROOT/packages"
BUILD_DIR="$PACKAGE_DIR/$PACKAGE_NAME"
INSTALL_DIR="$BUILD_DIR/install"

log "开始打包 ST-RocksDB C++ API/SDK"
info "版本: $VERSION"
info "类型: $PACKAGE_TYPE"
info "平台: $PLATFORM"
info "构建: $BUILD_TYPE"
info "包名: $PACKAGE_NAME"

# 清理和创建目录
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
mkdir -p "$INSTALL_DIR"

cd "$PROJECT_ROOT"

# 设置编译参数
case "$PACKAGE_TYPE" in
    "release")
        DEBUG_LEVEL=0
        OPTIMIZE_FLAGS="-O3 -DNDEBUG"
        ;;
    "debug")
        DEBUG_LEVEL=2
        OPTIMIZE_FLAGS="-O0 -g"
        ;;
    "dev")
        DEBUG_LEVEL=1
        OPTIMIZE_FLAGS="-O2 -g"
        ;;
    *)
        error "不支持的打包类型: $PACKAGE_TYPE"
        ;;
esac

# 设置平台特定参数
case "$PLATFORM" in
    "linux")
        SHARED_EXT="so"
        STATIC_EXT="a"
        JOBS=$(nproc)
        ;;
    "macos")
        SHARED_EXT="dylib"
        STATIC_EXT="a"
        JOBS=$(sysctl -n hw.ncpu)
        ;;
    "windows")
        SHARED_EXT="dll"
        STATIC_EXT="lib"
        JOBS=$(nproc 2>/dev/null || echo 4)
        ;;
    *)
        error "不支持的平台: $PLATFORM"
        ;;
esac

log "清理之前的构建..."
make clean || true

# 构建静态库
if [ "$BUILD_TYPE" = "static" ] || [ "$BUILD_TYPE" = "both" ]; then
    log "构建静态库..."
    make static_lib DEBUG_LEVEL=$DEBUG_LEVEL PORTABLE=1 -j$JOBS
    
    if [ ! -f "librocksdb.a" ]; then
        error "静态库构建失败"
    fi
    log "静态库构建成功: librocksdb.a"
fi

# 构建共享库
if [ "$BUILD_TYPE" = "shared" ] || [ "$BUILD_TYPE" = "both" ]; then
    log "构建共享库..."
    make shared_lib DEBUG_LEVEL=$DEBUG_LEVEL LIB_MODE=shared PORTABLE=1 -j$JOBS
    
    SHARED_LIB=$(ls librocksdb*.${SHARED_EXT} 2>/dev/null | head -n1)
    if [ -z "$SHARED_LIB" ]; then
        error "共享库构建失败"
    fi
    log "共享库构建成功: $SHARED_LIB"
fi

# 安装到临时目录
log "安装库文件和头文件..."
make install DESTDIR="$INSTALL_DIR" PREFIX="/usr"

# 创建SDK目录结构
log "创建SDK目录结构..."

# 创建主目录
mkdir -p "$BUILD_DIR"/{include,lib,examples,docs,cmake}

# 复制头文件
cp -r "$INSTALL_DIR/usr/include/rocksdb" "$BUILD_DIR/include/"

# 复制库文件
if [ -f "librocksdb.a" ]; then
    cp "librocksdb.a" "$BUILD_DIR/lib/"
fi

if [ -f "$SHARED_LIB" ]; then
    cp "$SHARED_LIB" "$BUILD_DIR/lib/"
    # 创建符号链接
    cd "$BUILD_DIR/lib"
    if [ "$PLATFORM" = "linux" ] || [ "$PLATFORM" = "macos" ]; then
        ln -sf "$SHARED_LIB" "librocksdb.${SHARED_EXT}"
    fi
    cd "$PROJECT_ROOT"
fi

# 复制示例代码
log "复制示例代码..."
cp -r examples/* "$BUILD_DIR/examples/" 2>/dev/null || true

# 创建 CMake 配置文件
log "创建 CMake 配置文件..."
cat > "$BUILD_DIR/cmake/st-rocksdb-config.cmake" << 'EOF'
# ST-RocksDB CMake 配置文件

get_filename_component(ST_ROCKSDB_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
get_filename_component(ST_ROCKSDB_ROOT_DIR "${ST_ROCKSDB_CMAKE_DIR}/.." ABSOLUTE)

# 设置库和头文件路径
set(ST_ROCKSDB_INCLUDE_DIRS "${ST_ROCKSDB_ROOT_DIR}/include")
set(ST_ROCKSDB_LIBRARY_DIRS "${ST_ROCKSDB_ROOT_DIR}/lib")

# 查找库文件
find_library(ST_ROCKSDB_STATIC_LIBRARY 
    NAMES librocksdb.a rocksdb
    PATHS ${ST_ROCKSDB_LIBRARY_DIRS}
    NO_DEFAULT_PATH
)

find_library(ST_ROCKSDB_SHARED_LIBRARY 
    NAMES librocksdb.so librocksdb.dylib rocksdb
    PATHS ${ST_ROCKSDB_LIBRARY_DIRS}
    NO_DEFAULT_PATH
)

# 创建导入目标
if(ST_ROCKSDB_STATIC_LIBRARY)
    add_library(st-rocksdb::static STATIC IMPORTED)
    set_target_properties(st-rocksdb::static PROPERTIES
        IMPORTED_LOCATION "${ST_ROCKSDB_STATIC_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ST_ROCKSDB_INCLUDE_DIRS}"
    )
endif()

if(ST_ROCKSDB_SHARED_LIBRARY)
    add_library(st-rocksdb::shared SHARED IMPORTED)
    set_target_properties(st-rocksdb::shared PROPERTIES
        IMPORTED_LOCATION "${ST_ROCKSDB_SHARED_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ST_ROCKSDB_INCLUDE_DIRS}"
    )
endif()

# 设置变量
set(ST_ROCKSDB_FOUND TRUE)
set(ST_ROCKSDB_VERSION "@VERSION@")

# 提供别名
if(TARGET st-rocksdb::static)
    add_library(st-rocksdb::rocksdb ALIAS st-rocksdb::static)
elseif(TARGET st-rocksdb::shared)
    add_library(st-rocksdb::rocksdb ALIAS st-rocksdb::shared)
endif()
EOF

# 替换版本号
sed -i.bak "s/@VERSION@/$VERSION/g" "$BUILD_DIR/cmake/st-rocksdb-config.cmake" && rm "$BUILD_DIR/cmake/st-rocksdb-config.cmake.bak"

# 创建 pkg-config 文件
log "创建 pkg-config 文件..."
cat > "$BUILD_DIR/lib/pkgconfig/st-rocksdb.pc" << EOF
prefix=\${pcfiledir}/../..
includedir=\${prefix}/include
libdir=\${prefix}/lib

Name: ST-RocksDB
Description: High Performance Embedded Database for Key-Value Data (ST fork)
URL: https://github.com/your-org/st-rocksdb
Version: $VERSION
Cflags: -I"\${includedir}"
Libs: -L"\${libdir}" -lrocksdb
EOF

mkdir -p "$BUILD_DIR/lib/pkgconfig"
mv "$BUILD_DIR/lib/pkgconfig/st-rocksdb.pc" "$BUILD_DIR/lib/pkgconfig/"

# 创建 README 文件
log "创建文档..."
cat > "$BUILD_DIR/README.md" << EOF
# ST-RocksDB C++ API/SDK

版本: $VERSION
构建类型: $PACKAGE_TYPE
平台: $PLATFORM
构建时间: $(date)

## 目录结构

- \`include/\` - C++ 头文件
- \`lib/\` - 静态库和共享库文件
- \`examples/\` - 示例代码
- \`docs/\` - 文档
- \`cmake/\` - CMake 配置文件

## 使用方法

### 使用 CMake

\`\`\`cmake
# 添加到你的 CMakeLists.txt
list(APPEND CMAKE_PREFIX_PATH "\${CMAKE_CURRENT_SOURCE_DIR}/path/to/st-rocksdb")
find_package(st-rocksdb REQUIRED)

# 链接库
target_link_libraries(your_target st-rocksdb::rocksdb)
\`\`\`

### 使用 pkg-config

\`\`\`bash
export PKG_CONFIG_PATH=\$PKG_CONFIG_PATH:/path/to/st-rocksdb/lib/pkgconfig
pkg-config --cflags --libs st-rocksdb
\`\`\`

### 直接使用

\`\`\`cpp
#include <rocksdb/db.h>
#include <rocksdb/options.h>

// 链接时添加: -L/path/to/lib -lrocksdb
\`\`\`

## 库文件

EOF

if [ -f "$BUILD_DIR/lib/librocksdb.a" ]; then
    echo "- 静态库: lib/librocksdb.a" >> "$BUILD_DIR/README.md"
fi

if ls "$BUILD_DIR/lib/librocksdb"*.${SHARED_EXT} >/dev/null 2>&1; then
    echo "- 共享库: lib/librocksdb.${SHARED_EXT}" >> "$BUILD_DIR/README.md"
fi

cat >> "$BUILD_DIR/README.md" << EOF

## 系统要求

- C++17 兼容的编译器
- CMake 3.10+ (如果使用 CMake)
- 支持的平台: Linux, macOS, Windows

## 许可证

请查看原项目的许可证文件。

EOF

# 复制重要文档
cp LICENSE.Apache "$BUILD_DIR/LICENSE" 2>/dev/null || true
cp README.md "$BUILD_DIR/ORIGINAL_README.md" 2>/dev/null || true

# 创建构建信息文件
cat > "$BUILD_DIR/BUILD_INFO.txt" << EOF
ST-RocksDB SDK 构建信息
========================

版本: $VERSION
构建类型: $PACKAGE_TYPE  
平台: $PLATFORM
构建方式: $BUILD_TYPE
调试级别: $DEBUG_LEVEL
构建时间: $(date)
构建主机: $(hostname)
Git 提交: $(git rev-parse HEAD 2>/dev/null || echo "unknown")

编译器信息:
$(${CXX:-g++} --version | head -n1 2>/dev/null || echo "unknown")

库文件信息:
EOF

if [ -f "$BUILD_DIR/lib/librocksdb.a" ]; then
    echo "librocksdb.a: $(ls -lh "$BUILD_DIR/lib/librocksdb.a" | awk '{print $5}')" >> "$BUILD_DIR/BUILD_INFO.txt"
fi

if ls "$BUILD_DIR/lib/librocksdb"*.${SHARED_EXT} >/dev/null 2>&1; then
    SHARED_LIB_PATH=$(ls "$BUILD_DIR/lib/librocksdb"*.${SHARED_EXT} | head -n1)
    echo "$(basename "$SHARED_LIB_PATH"): $(ls -lh "$SHARED_LIB_PATH" | awk '{print $5}')" >> "$BUILD_DIR/BUILD_INFO.txt"
fi

# 创建压缩包
log "创建压缩包..."
cd "$PACKAGE_DIR"

# 创建 tar.gz
tar -czf "${PACKAGE_NAME}.tar.gz" "$PACKAGE_NAME"
log "创建了: ${PACKAGE_NAME}.tar.gz"

# 创建 zip (如果 zip 命令可用)
if command -v zip >/dev/null 2>&1; then
    zip -r "${PACKAGE_NAME}.zip" "$PACKAGE_NAME" >/dev/null
    log "创建了: ${PACKAGE_NAME}.zip"
fi

# 显示结果
log "打包完成！"
info "包目录: $BUILD_DIR"
info "压缩包:"
ls -lh "$PACKAGE_DIR"/${PACKAGE_NAME}.* | while read line; do
    info "  $line"
done

# 显示使用说明
echo
echo -e "${BLUE}使用说明:${NC}"
echo "1. 解压包到目标目录"
echo "2. 参考 README.md 集成到您的项目中"
echo "3. 使用 CMake 或 pkg-config 进行链接"

echo
echo -e "${BLUE}快速测试:${NC}"
echo "cd $BUILD_DIR/examples"
echo "make  # 编译示例程序"

# 验证包的完整性
log "验证包完整性..."
REQUIRED_FILES=(
    "include/rocksdb/db.h"
    "include/rocksdb/options.h"
    "README.md"
    "BUILD_INFO.txt"
    "cmake/st-rocksdb-config.cmake"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$BUILD_DIR/$file" ]; then
        warn "缺少文件: $file"
    fi
done

if [ -f "$BUILD_DIR/lib/librocksdb.a" ] || ls "$BUILD_DIR/lib/librocksdb"*.${SHARED_EXT} >/dev/null 2>&1; then
    log "包验证通过 ✓"
else
    error "包验证失败: 没有找到库文件"
fi

log "ST-RocksDB SDK 打包完成！" 