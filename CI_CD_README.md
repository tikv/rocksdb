# ST-RocksDB CI/CD 技术文档

## 配置概览

- **主CI流水线** (`.github/workflows/ci.yml`) - 多平台构建+测试
- **PR审查流水线** (`.github/workflows/pr-review.yml`) - 安全扫描+质量检查  
- **本地构建脚本** (`scripts/local_build_test.sh`) - 一键验证

## CI 流水线

### 触发条件
- Push: `main`, `master`, `denjixu_dev` 分支
- Pull Request: 所有目标分支

### 构建矩阵

| 平台 | 编译器 | 构建类型 | 特性 |
|------|--------|----------|------|
| Ubuntu | GCC | Debug/Release | 标准构建 |
| Ubuntu | Clang | Debug/Release | 现代C++检查 |
| macOS | 系统默认 | Debug/Release | 跨平台验证 |

### 任务清单

```yaml
✅ 代码格式检查 (clang-format)
✅ 多平台构建验证  
✅ 单元测试执行 (make check)
✅ 性能基准测试 (PR时)
✅ 内存安全检查 (AddressSanitizer)
✅ 智能测试摘要生成
```

## PR 审查流程

### 深度检查项目

1. **安全扫描**: Trivy 漏洞检测
2. **跨平台测试**: Ubuntu 20.04/22.04 + macOS
3. **内存检查**: Valgrind 泄漏检测  
4. **性能回归**: 大变更时自动触发
5. **文档一致性**: API变更检查
6. **智能摘要**: 中文测试报告

### 流程控制

```bash
# PR变更分析
- 修改文件数 < 10: 快速检查
- 修改文件数 >= 10: 完整检查 + 性能测试
- 核心文件修改: 强制全面测试
```

## 本地开发

### 快速开始

```bash
# 一键构建测试
./scripts/local_build_test.sh

# 仅构建 (跳过测试)
./scripts/local_build_test.sh --skip-tests

# 并行构建
make -j$(nproc) static_lib
```

### 环境要求

**必需**:
- GCC >= 7 或 Clang >= 5 (C++17)
- make, cmake, git

**推荐**:  
- snappy, lz4, zstd, bzip2 (压缩)
- gflags (命令行解析)

### 依赖安装

```bash
# macOS
brew install cmake gflags snappy lz4 zstd

# Ubuntu
sudo apt-get install -y build-essential cmake \
    libgflags-dev libsnappy-dev zlib1g-dev \
    libbz2-dev liblz4-dev libzstd-dev

# 自动安装
./scripts/local_build_test.sh  # 自动检测并安装
```

## 开发工作流

### 日常开发
```bash
# 增量构建
make static_lib

# 运行特定测试
make db_test && ./db_test

# 格式检查
make check-format
```

### 提交前检查
```bash
# 完整验证
./scripts/local_build_test.sh

# 仅构建验证
make clean && make static_lib DEBUG_LEVEL=0
```

### PR 最佳实践
1. 本地测试通过
2. 遵循代码格式
3. 添加必要的测试用例
4. 查看CI生成的中文报告

## 性能优化

### 构建优化
```bash
# Release构建
make static_lib DEBUG_LEVEL=0

# 针对当前CPU优化
PORTABLE=0 make static_lib

# 使用 Ninja 加速
cmake -G Ninja && ninja
```

### CI 缓存策略
- 依赖包缓存: 24小时
- 编译缓存: 分编译器/平台
- 测试数据缓存: 实时清理

## 故障排除

### 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| 构建失败 | 依赖缺失 | 运行本地构建脚本 |
| 测试超时 | 资源不足 | 减少并行度 |
| 格式检查失败 | 代码风格 | `make check-format` |
| 内存错误 | 代码Bug | 查看AddressSanitizer日志 |

### 调试技巧

```bash
# 详细构建日志
make VERBOSE=1 static_lib

# 单独运行特定测试
./db_test --gtest_filter=*TestName*

# 检查依赖链接
ldd ./db_test  # Linux
otool -L ./db_test  # macOS
```

## 监控指标

### CI 性能指标
- **构建时间**: 5-15分钟 (正常)
- **测试覆盖率**: 定期检查
- **成功率**: >95% (目标)

### 资源使用
- **并行任务**: 最多6个job
- **内存限制**: 单任务 < 8GB
- **存储限制**: 缓存 < 2GB

## 扩展配置

### 添加新平台
1. 修改 `ci.yml` 中的 `matrix.os`
2. 更新依赖安装脚本
3. 测试新平台兼容性

### 自定义检查
1. 在 `.github/workflows/` 添加新文件
2. 配置触发条件和依赖
3. 测试工作流运行

### 分支保护
建议设置:
- 要求PR审查
- 要求状态检查通过
- 禁止直接推送到main 