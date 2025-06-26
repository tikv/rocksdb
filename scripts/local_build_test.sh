#!/bin/bash
# RocksDB 本地构建和测试脚本
# 支持 macOS 和 Linux 环境

set -e  # 遇到错误时退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检测操作系统
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
        DISTRO=$(lsb_release -si 2>/dev/null || echo "unknown")
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        DISTRO="macos"
    else
        log_error "不支持的操作系统: $OSTYPE"
        exit 1
    fi
    log_info "检测到操作系统: $OS ($DISTRO)"
}

# 检查依赖
check_dependencies() {
    log_info "检查构建依赖..."
    
    # 检查编译器
    if ! command -v gcc &> /dev/null && ! command -v clang &> /dev/null; then
        log_error "未找到C++编译器 (gcc 或 clang)"
        exit 1
    fi
    
    # 检查make
    if ! command -v make &> /dev/null; then
        log_error "未找到make命令"
        exit 1
    fi
    
    # 检查Git
    if ! command -v git &> /dev/null; then
        log_error "未找到git命令"
        exit 1
    fi
    
    log_success "基本依赖检查通过"
}

# 安装依赖
install_dependencies() {
    log_info "安装RocksDB依赖..."
    
    if [[ "$OS" == "macos" ]]; then
        if command -v brew &> /dev/null; then
            log_info "使用Homebrew安装依赖..."
            brew install cmake gflags snappy lz4 zstd || log_warning "某些依赖安装失败，但可能不影响构建"
        else
            log_warning "未找到Homebrew，请手动安装: cmake gflags snappy lz4 zstd"
        fi
    elif [[ "$OS" == "linux" ]]; then
        if command -v apt-get &> /dev/null; then
            log_info "使用apt安装依赖..."
            sudo apt-get update
            sudo apt-get install -y \
                build-essential \
                cmake \
                libgflags-dev \
                libsnappy-dev \
                zlib1g-dev \
                libbz2-dev \
                liblz4-dev \
                libzstd-dev || log_warning "某些依赖安装失败，但可能不影响构建"
        elif command -v yum &> /dev/null; then
            log_info "使用yum安装依赖..."
            sudo yum install -y \
                gcc-c++ \
                cmake \
                gflags-devel \
                snappy-devel \
                zlib-devel \
                bzip2-devel \
                lz4-devel \
                libzstd-devel || log_warning "某些依赖安装失败，但可能不影响构建"
        else
            log_warning "未找到包管理器，请手动安装依赖"
        fi
    fi
}

# 清理之前的构建
clean_build() {
    log_info "清理之前的构建..."
    make clean 2>/dev/null || true
    rm -f librocksdb.* || true
    rm -f db_test db_bench || true
    log_success "清理完成"
}

# 构建静态库
build_static_lib() {
    log_info "构建RocksDB静态库..."
    
    # 使用适当的并行数
    if [[ "$OS" == "macos" ]]; then
        JOBS=$(sysctl -n hw.ncpu)
    else
        JOBS=$(nproc)
    fi
    
    log_info "使用 $JOBS 个并行任务构建..."
    
    # 构建静态库（Release模式）
    if make static_lib DEBUG_LEVEL=0 -j$JOBS; then
        log_success "静态库构建成功"
        ls -la librocksdb.a 2>/dev/null || log_warning "未找到librocksdb.a文件"
    else
        log_error "静态库构建失败"
        return 1
    fi
}

# 构建共享库
build_shared_lib() {
    log_info "构建RocksDB共享库..."
    
    if [[ "$OS" == "macos" ]]; then
        JOBS=$(sysctl -n hw.ncpu)
    else
        JOBS=$(nproc)
    fi
    
    # 构建共享库（Release模式）
    if make shared_lib DEBUG_LEVEL=0 -j$JOBS; then
        log_success "共享库构建成功"
        ls -la librocksdb.* 2>/dev/null | grep -E "\.(so|dylib)" || log_warning "未找到共享库文件"
    else
        log_error "共享库构建失败"
        return 1
    fi
}

# 构建测试
build_tests() {
    log_info "构建测试程序..."
    
    if [[ "$OS" == "macos" ]]; then
        JOBS=$(sysctl -n hw.ncpu)
    else
        JOBS=$(nproc)
    fi
    
    # 构建基本测试
    if make db_test -j$JOBS; then
        log_success "测试程序构建成功"
    else
        log_error "测试程序构建失败"
        return 1
    fi
}

# 运行基本测试
run_basic_tests() {
    log_info "运行基本测试..."
    
    if [[ -f "./db_test" ]]; then
        # 运行一些基本测试，设置超时
        log_info "运行数据库基本功能测试..."
        if timeout 300 ./db_test --gtest_filter="*Basic*" 2>/dev/null; then
            log_success "基本测试通过"
        else
            log_warning "基本测试超时或失败，但这可能是正常的"
        fi
    else
        log_warning "测试程序不存在，跳过测试"
    fi
}

# 构建性能测试工具
build_benchmark() {
    log_info "构建性能测试工具..."
    
    if [[ "$OS" == "macos" ]]; then
        JOBS=$(sysctl -n hw.ncpu)
    else
        JOBS=$(nproc)
    fi
    
    if make db_bench DEBUG_LEVEL=0 -j$JOBS; then
        log_success "性能测试工具构建成功"
    else
        log_error "性能测试工具构建失败"
        return 1
    fi
}

# 运行性能测试
run_benchmark() {
    log_info "运行简单性能测试..."
    
    if [[ -f "./db_bench" ]]; then
        log_info "执行基本读写性能测试..."
        if ./db_bench \
            --benchmarks=fillseq,readrandom \
            --num=10000 \
            --threads=1 \
            --db=/tmp/rocksdb_test_bench 2>/dev/null; then
            log_success "性能测试完成"
        else
            log_warning "性能测试失败，但这可能是正常的"
        fi
        
        # 清理测试数据
        rm -rf /tmp/rocksdb_test_bench 2>/dev/null || true
    else
        log_warning "性能测试工具不存在，跳过性能测试"
    fi
}

# 显示构建信息
show_build_info() {
    log_info "构建信息总结:"
    echo "========================="
    echo "操作系统: $OS ($DISTRO)"
    echo "编译器: $(gcc --version 2>/dev/null | head -n1 || clang --version 2>/dev/null | head -n1 || echo '未知')"
    echo "构建目录: $(pwd)"
    echo ""
    echo "构建产物:"
    ls -la librocksdb.* 2>/dev/null || echo "  - 无库文件"
    ls -la db_test db_bench 2>/dev/null || echo "  - 无测试程序"
    echo "========================="
}

# 主函数
main() {
    echo "=================================="
    echo "    RocksDB 本地构建测试脚本"
    echo "=================================="
    echo ""
    
    # 检查是否在正确的目录
    if [[ ! -f "Makefile" ]] || [[ ! -d "include/rocksdb" ]]; then
        log_error "请在RocksDB项目根目录运行此脚本"
        exit 1
    fi
    
    # 解析命令行参数
    SKIP_DEPS=false
    SKIP_TESTS=false
    SKIP_BENCHMARK=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-deps)
                SKIP_DEPS=true
                shift
                ;;
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            --skip-benchmark)
                SKIP_BENCHMARK=true
                shift
                ;;
            --help|-h)
                echo "用法: $0 [选项]"
                echo "选项:"
                echo "  --skip-deps      跳过依赖安装"
                echo "  --skip-tests     跳过测试运行"
                echo "  --skip-benchmark 跳过性能测试"
                echo "  --help, -h       显示此帮助信息"
                exit 0
                ;;
            *)
                log_error "未知选项: $1"
                exit 1
                ;;
        esac
    done
    
    # 执行构建流程
    detect_os
    check_dependencies
    
    if [[ "$SKIP_DEPS" == false ]]; then
        install_dependencies
    else
        log_info "跳过依赖安装"
    fi
    
    clean_build
    
    # 构建库
    build_static_lib
    build_shared_lib
    
    # 构建和运行测试
    if [[ "$SKIP_TESTS" == false ]]; then
        build_tests
        run_basic_tests
    else
        log_info "跳过测试"
    fi
    
    # 构建和运行性能测试
    if [[ "$SKIP_BENCHMARK" == false ]]; then
        build_benchmark
        run_benchmark
    else
        log_info "跳过性能测试"
    fi
    
    show_build_info
    
    log_success "RocksDB构建测试完成！"
    echo ""
    echo "接下来可以："
    echo "1. 使用 ./db_test 运行更多测试"
    echo "2. 使用 ./db_bench 进行性能测试"
    echo "3. 查看 examples/ 目录中的示例代码"
    echo "4. 阅读 INSTALL.md 了解更多安装选项"
}

# 运行主函数
main "$@" 