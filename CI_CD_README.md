# RocksDB CI/CD 技术文档

## 📋 配置概览

本项目配置了完整的CI/CD流水线，包括：

1. **主CI流水线** (`.github/workflows/ci.yml`) - Clang编译器，多平台构建
2. **PR审查流水线** (`.github/workflows/pr-review.yml`) - 安全扫描，跨平台测试
3. **本地构建脚本** (`scripts/local_build_test.sh`) - 一键本地验证

## CI/CD 功能特性

### 🔧 持续集成 (CI Pipeline)

**触发条件：**
- 推送到 `main`, `master`, `denjixu_dev` 分支
- 创建Pull Request

**包含的任务：**

1. **代码格式检查**
   - 使用 clang-format 检查代码风格
   - 检查源代码规范性

2. **多平台构建**
   - Ubuntu (gcc/clang, Debug/Release)
   - macOS (Debug/Release)
   - 自动安装依赖包

3. **单元测试**
   - 编译并运行完整的单元测试套件
   - 使用 `make check` 运行所有测试

4. **性能测试**
   - 仅在PR时运行，避免资源浪费
   - 基础读写性能测试

5. **内存安全检查**
   - AddressSanitizer (ASan) 构建
   - 内存泄漏检测

### 🔍 PR审查流程 (PR Review Pipeline)

**触发条件：**
- PR创建、同步、重新打开
- 针对 `main`, `master`, `denjixu_dev` 分支

**高级功能：**

1. **PR信息收集**
   - 自动分析修改的文件
   - 生成变更摘要

2. **代码质量检查**
   - 格式化检查
   - 源代码规范检查
   - Buck构建目标验证

3. **安全扫描**
   - 使用 Trivy 进行漏洞扫描
   - 结果上传到 GitHub Security tab

4. **构建验证**
   - 多编译器、多配置矩阵测试
   - 失败快速反馈机制

5. **跨平台测试**
   - Ubuntu 20.04/22.04
   - macOS 11/12
   - 兼容性验证

6. **性能回归测试**
   - 仅在大量文件变更时运行
   - 自动性能基准测试

7. **内存泄漏检查**
   - Valgrind 内存检测
   - 详细的内存错误报告

8. **文档检查**
   - 检测API变更是否需要文档更新
   - 自动提醒文档维护

9. **智能摘要**
   - 自动生成测试报告
   - 中文评论回复到PR
   - 一键了解测试状态

## 本地开发和测试

### 本地构建脚本

使用提供的脚本进行本地环境测试：

```bash
# 给脚本添加执行权限
chmod +x scripts/local_build_test.sh

# 完整构建和测试
./scripts/local_build_test.sh

# 跳过依赖安装（如果已安装）
./scripts/local_build_test.sh --skip-deps

# 仅构建，跳过测试
./scripts/local_build_test.sh --skip-tests --skip-benchmark

# 查看帮助
./scripts/local_build_test.sh --help
```

### 脚本功能

1. **自动环境检测**
   - 支持 macOS 和 Linux
   - 自动检测包管理器

2. **依赖管理**
   - macOS: 使用 Homebrew
   - Ubuntu: 使用 apt-get
   - CentOS/RHEL: 使用 yum

3. **构建流程**
   - 清理之前的构建
   - 构建静态库和共享库
   - 编译测试程序

4. **测试执行**
   - 基本功能测试
   - 性能基准测试
   - 自动清理测试数据

## 环境要求

### 最低要求

- **编译器**: GCC >= 7 或 Clang >= 5 (支持C++17)
- **构建工具**: make, cmake
- **版本控制**: git

### 推荐依赖库

- **压缩库**: snappy, lz4, zstd, bzip2, zlib
- **工具库**: gflags
- **构建加速**: ninja (可选)

### macOS 安装

```bash
# 使用 Homebrew
brew install cmake gflags snappy lz4 zstd

# 或者让脚本自动安装
./scripts/local_build_test.sh
```

### Ubuntu 安装

```bash
# 手动安装
sudo apt-get update
sudo apt-get install -y \
    build-essential cmake \
    libgflags-dev libsnappy-dev \
    zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev

# 或者让脚本自动安装
./scripts/local_build_test.sh
```

## 使用建议

### 开发工作流

1. **本地开发**
   ```bash
   # 首次克隆后
   ./scripts/local_build_test.sh
   
   # 日常开发
   make static_lib  # 快速构建
   make db_test && ./db_test  # 运行测试
   ```

2. **提交前检查**
   ```bash
   # 格式检查
   make check-format
   
   # 完整本地测试
   ./scripts/local_build_test.sh
   ```

3. **创建PR**
   - 确保所有本地测试通过
   - PR会自动触发完整的CI流程
   - 查看自动生成的测试报告

### 性能优化建议

1. **Release构建**
   ```bash
   make static_lib DEBUG_LEVEL=0  # Release模式
   make static_lib DEBUG_LEVEL=1  # Debug模式
   ```

2. **并行构建**
   ```bash
   make -j$(nproc) static_lib  # Linux
   make -j$(sysctl -n hw.ncpu) static_lib  # macOS
   ```

3. **目标架构优化**
   ```bash
   PORTABLE=1 make static_lib      # 通用兼容性
   PORTABLE=haswell make static_lib # x86_64优化
   ```

## 故障排除

### 常见问题

1. **依赖缺失**
   - 运行脚本自动安装：`./scripts/local_build_test.sh`
   - 手动安装缺失的依赖库

2. **编译器版本过低**
   ```bash
   # Ubuntu
   sudo apt-get install gcc-9 g++-9
   export CC=gcc-9 CXX=g++-9
   
   # macOS
   xcode-select --install
   ```

3. **内存不足**
   ```bash
   # 减少并行度
   make -j2 static_lib
   ```

4. **磁盘空间不足**
   ```bash
   # 清理构建缓存
   make clean
   ```

### 获取帮助

- 查看构建日志中的详细错误信息
- 检查 GitHub Actions 的失败日志
- 参考 RocksDB 官方文档
- 提交 Issue 描述具体问题

## 自定义配置

### 修改CI触发条件

编辑 `.github/workflows/ci.yml`:

```yaml
on:
  push:
    branches: [ main, master, your-branch ]  # 添加你的分支
  pull_request:
    branches: [ main, master, your-branch ]
```

### 调整测试超时

编辑工作流文件中的 `timeout` 值：

```yaml
- name: Run tests
  run: timeout 600 ./db_test  # 10分钟超时
```

### 添加新的构建配置

在构建矩阵中添加新配置：

```yaml
strategy:
  matrix:
    compiler: [gcc, clang]
    build_type: [Debug, Release, MinSizeRel]  # 添加新类型
    os: [ubuntu-20.04, ubuntu-22.04, macos-11, macos-12]
```

## 监控和维护

### CI状态监控

- 在GitHub仓库页面查看Actions状态
- 设置邮件通知获取失败提醒
- 定期检查和更新依赖版本

### 定期维护任务

1. **更新GitHub Actions版本**
   - 定期更新 `actions/checkout`、`actions/setup-python` 等
   
2. **依赖库版本更新**
   - 监控RocksDB依赖库的更新
   - 测试新版本的兼容性

3. **性能基线更新**
   - 定期更新性能测试的基线数据
   - 监控性能回归趋势

---

通过这套完整的CI/CD配置，可以确保RocksDB项目的代码质量、构建稳定性和跨平台兼容性。配置支持自动化测试、详细报告和智能反馈，大大提升了开发效率和代码可靠性。 