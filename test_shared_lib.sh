#!/bin/bash

# 共享库构建测试脚本
echo "🔧 测试共享库构建..."

# 检查环境
echo "📋 环境信息:"
echo "系统: $(uname -s) $(uname -m)"
echo "编译器: $(clang --version | head -n1 2>/dev/null || echo 'Clang not found')"
echo ""

# 设置编译器
export CC=clang
export CXX=clang++
echo "✅ 设置编译器为 Clang"

# 完全清理构建缓存
echo "🧹 完全清理构建缓存..."
make clean
rm -f librocksdb.*
echo ""

# 强制重新生成make_config.mk
echo "🔄 重新生成配置..."
rm -f make_config.mk
echo ""

# 构建共享库
echo "📚 构建共享库 (Release模式)..."
if make shared_lib DEBUG_LEVEL=0 LIB_MODE=shared -j$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 2); then
    echo "✅ 共享库构建成功"
else
    echo "❌ 共享库构建失败"
    echo ""
    echo "🔍 诊断信息:"
    echo "检查make_config.mk内容:"
    cat make_config.mk | grep -E "(PLATFORM_SHARED|CC|CXX)"
    exit 1
fi

# 验证生成的文件
echo ""
echo "📊 构建结果:"
echo "============="
ls -la librocksdb.* 2>/dev/null || echo "未找到库文件"

# 检查共享库符号
if ls librocksdb.*.dylib 2>/dev/null || ls librocksdb.*.so 2>/dev/null; then
    echo ""
    echo "🔍 共享库符号检查:"
    if command -v nm >/dev/null 2>&1; then
        SHARED_LIB=$(ls librocksdb.*.dylib 2>/dev/null || ls librocksdb.*.so 2>/dev/null | head -n1)
        echo "检查 $SHARED_LIB 的符号表..."
        nm -D "$SHARED_LIB" 2>/dev/null | head -5 || echo "无法读取符号表"
    fi
fi

echo "============="
echo ""

echo "🎉 共享库构建测试完成！"
echo ""
echo "�� 如果构建成功，说明PIC问题已解决" 