#!/bin/bash

# 快速构建验证脚本
echo "🔧 开始验证修复后的构建..."

# 检查环境
echo "📋 环境信息:"
echo "系统: $(uname -s) $(uname -m)"
echo "编译器: $(clang --version | head -n1 2>/dev/null || echo 'Clang not found')"
echo "Make: $(make --version | head -n1 2>/dev/null || echo 'Make not found')"
echo ""

# 设置编译器为Clang
export CC=clang
export CXX=clang++
echo "✅ 设置编译器为 Clang"
echo ""

# 清理之前的构建
echo "🧹 清理构建缓存..."
make clean 2>/dev/null || true
echo ""

# 验证静态库构建
echo "📦 测试静态库构建..."
if make static_lib DEBUG_LEVEL=0 -j$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 2); then
    echo "✅ 静态库构建成功"
    ls -la librocksdb.a 2>/dev/null && echo "✅ 静态库文件存在"
else
    echo "❌ 静态库构建失败"
    exit 1
fi
echo ""

# 验证共享库构建
echo "📚 测试共享库构建..."
# 清理并重新构建共享库
make clean
rm -f make_config.mk
if make shared_lib DEBUG_LEVEL=0 LIB_MODE=shared -j$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 2); then
    echo "✅ 共享库构建成功"
    ls -la librocksdb.* 2>/dev/null | grep -E '\.(so|dylib)' && echo "✅ 共享库文件存在"
else
    echo "❌ 共享库构建失败"
    echo "🔍 检查make_config.mk配置:"
    cat make_config.mk | grep -E "(PLATFORM_SHARED|CC|CXX)" 2>/dev/null || echo "配置文件不存在"
    exit 1
fi
echo ""

# 显示构建结果
echo "📊 构建结果:"
echo "============="
ls -la librocksdb.* 2>/dev/null || echo "未找到库文件"
echo "============="
echo ""

echo "🎉 构建验证完成！"
echo ""
echo "💡 接下来可以："
echo "1. 提交修复: git add . && git commit -m 'fix: 修复VLA编译错误'"
echo "2. 推送验证: git push origin denjixu_dev"
echo "3. 查看CI状态: 在GitHub Actions页面观察结果" 