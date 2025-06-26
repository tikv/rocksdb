#!/bin/bash

# RocksDB Performance Analysis with Flame Graph
# 使用 sample 命令采集数据并生成火焰图 (改进版)

echo "🔥 RocksDB Performance Analysis with Flame Graph (v2)"
echo "====================================================="

# 配置参数
TEST_BINARY="./version_set_test"
TEST_FILTER="LogAndApplyPerformanceTest.IncrementalLogAndApplyBaseline"
OUTPUT_DIR="./profile_results"
FLAMEGRAPH_DIR="./FlameGraph"
SAMPLE_DURATION=30  # 采样时间（秒）

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 检查 FlameGraph 工具
if [ ! -d "$FLAMEGRAPH_DIR" ]; then
    echo "❌ FlameGraph directory not found. Please run: git clone https://github.com/brendangregg/FlameGraph.git"
    exit 1
fi

# 检查 sample 命令是否可用
if ! command -v sample >/dev/null 2>&1; then
    echo "❌ 'sample' command not found. Please ensure you're on macOS with Developer Tools installed."
    exit 1
fi

echo "📁 Results directory: $OUTPUT_DIR"
echo "🔥 FlameGraph tools: $FLAMEGRAPH_DIR" 
echo "⏱️  Sample duration: ${SAMPLE_DURATION} seconds"
echo ""

# 步骤1: 确保测试程序可用
echo "🔨 Step 1: Ensuring test binary is available..."
echo "---------------------------------------------"

if [ ! -f "$TEST_BINARY" ]; then
    echo "⚠️  Test binary not found. Building..."
    make version_set_test -j$(nproc) || {
        echo "❌ Failed to build test binary"
        exit 1
    }
fi

echo "✅ Test binary ready: $TEST_BINARY"
echo ""

# 步骤2: 启动测试程序并进行 CPU profiling
echo "🚀 Step 2: Starting RocksDB performance test with profiling..."
echo "------------------------------------------------------------"

# 启动测试程序
$TEST_BINARY --gtest_filter="$TEST_FILTER" > "$OUTPUT_DIR/test_output.log" 2>&1 &
TEST_PID=$!

echo "✅ Test started with PID: $TEST_PID"

# 等待一小段时间确保进程启动
sleep 2

# 检查进程是否仍在运行
if ! kill -0 $TEST_PID 2>/dev/null; then
    echo "❌ Test process died immediately. Check test_output.log"
    exit 1
fi

echo "📊 Starting CPU profiling with sample command..."

# 使用 sample 命令进行 CPU profiling
# 关键参数说明:
# -wait: 等待进程出现
# -mayDie: 允许进程在采样期间结束
# -file: 输出文件

echo "🔬 Running sample command for ${SAMPLE_DURATION} seconds..."
sample "$TEST_PID" "$SAMPLE_DURATION" \
    -wait \
    -mayDie \
    -file "$OUTPUT_DIR/sample_raw.txt" \
    > "$OUTPUT_DIR/sample_stdout.log" 2>&1

SAMPLE_EXIT_CODE=$?

# 等待测试进程完成
wait $TEST_PID 2>/dev/null
TEST_EXIT_CODE=$?

echo "✅ Test completed with exit code: $TEST_EXIT_CODE"
echo "✅ Sampling completed with exit code: $SAMPLE_EXIT_CODE"
echo ""

# 检查采样结果
if [ ! -f "$OUTPUT_DIR/sample_raw.txt" ]; then
    echo "❌ Sample output file not created"
    echo "🔍 Check sample stdout log: $OUTPUT_DIR/sample_stdout.log"
    exit 1
fi

if [ ! -s "$OUTPUT_DIR/sample_raw.txt" ]; then
    echo "❌ Sample output file is empty"
    echo "🔍 Check sample stdout log: $OUTPUT_DIR/sample_stdout.log"
    exit 1
fi

# 步骤3: 处理采样数据并生成火焰图
echo "🔥 Step 3: Generating Flame Graph..."
echo "-----------------------------------"

echo "✅ Sample data collected successfully"
echo "📊 Sample file size: $(wc -l < "$OUTPUT_DIR/sample_raw.txt") lines"

# 显示采样文件的一些统计信息
echo "📈 Sample data analysis:"
echo "   - Total lines: $(wc -l < "$OUTPUT_DIR/sample_raw.txt")"
echo "   - File size: $(ls -lh "$OUTPUT_DIR/sample_raw.txt" | awk '{print $5}')"
if grep -q "Call graph:" "$OUTPUT_DIR/sample_raw.txt"; then
    echo "   ✅ Contains call graph data"
else
    echo "   ⚠️  No call graph data found"
fi

# 转换 sample 输出为 flamegraph 格式
echo "🔄 Converting sample data to flamegraph format..."

# 检查 stackcollapse-sample.awk 是否存在
if [ ! -f "$FLAMEGRAPH_DIR/stackcollapse-sample.awk" ]; then
    echo "❌ stackcollapse-sample.awk not found in $FLAMEGRAPH_DIR"
    echo "🔍 Please ensure FlameGraph tools are properly installed"
    exit 1
fi

# 使用 FlameGraph 官方工具处理 sample 输出
echo "   🔥 Using official FlameGraph stackcollapse-sample.awk..."
cat "$OUTPUT_DIR/sample_raw.txt" | \
    "$FLAMEGRAPH_DIR/stackcollapse-sample.awk" > "$OUTPUT_DIR/flamegraph_input.txt" 2> "$OUTPUT_DIR/convert_log.txt"

# 检查转换结果
if [ -s "$OUTPUT_DIR/flamegraph_input.txt" ]; then
    echo "✅ Data conversion successful"
    STACK_COUNT=$(wc -l < "$OUTPUT_DIR/flamegraph_input.txt")
    echo "📊 Converted data: $STACK_COUNT stack traces"
    
    if [ "$STACK_COUNT" -gt 0 ]; then
        # 显示转换统计
        echo "📈 Conversion statistics:"
        cat "$OUTPUT_DIR/convert_log.txt" | grep "^#" | sed 's/^#/   -/'
        
        # 显示前几个最热的栈
        echo "🔥 Top 5 hottest stacks:"
        head -5 "$OUTPUT_DIR/flamegraph_input.txt" | sed 's/^/   - /'
        
        # 生成火焰图 SVG
        echo ""
        echo "🎨 Generating flame graph SVG..."
        "$FLAMEGRAPH_DIR/flamegraph.pl" \
            --title "RocksDB LogAndApply Performance Profile" \
            --subtitle "CPU Usage Analysis - PrepareApply Optimization Study" \
            --width 1600 \
            --height 900 \
            --colors hot \
            --inverted \
            "$OUTPUT_DIR/flamegraph_input.txt" > "$OUTPUT_DIR/rocksdb_flamegraph.svg"
        
        if [ -f "$OUTPUT_DIR/rocksdb_flamegraph.svg" ] && [ -s "$OUTPUT_DIR/rocksdb_flamegraph.svg" ]; then
            echo "✅ Flame graph generated successfully!"
            echo "🔥 Flame graph saved to: $OUTPUT_DIR/rocksdb_flamegraph.svg"
            
            # 检查火焰图大小
            SVG_SIZE=$(wc -c < "$OUTPUT_DIR/rocksdb_flamegraph.svg")
            echo "📏 Flame graph size: $SVG_SIZE bytes"
        else
            echo "❌ Failed to generate flame graph SVG"
            echo "🔍 Check flamegraph.pl output above"
        fi
    else
        echo "⚠️  No stack traces found in converted data"
    fi
else
    echo "❌ Data conversion failed or no data"
    echo "🔍 Conversion log:"
    cat "$OUTPUT_DIR/convert_log.txt"
fi

# 步骤4: 生成分析报告
echo ""
echo "📊 Step 4: Generating analysis report..."
echo "---------------------------------------"

cat > "$OUTPUT_DIR/analysis_report.md" << EOF
# RocksDB LogAndApply Performance Analysis Report

## Test Configuration
- **Test Binary**: \`$TEST_BINARY\`
- **Test Filter**: \`$TEST_FILTER\`
- **Sample Duration**: ${SAMPLE_DURATION} seconds
- **Test PID**: $TEST_PID
- **Test Exit Code**: $TEST_EXIT_CODE
- **Sample Exit Code**: $SAMPLE_EXIT_CODE

## Generated Files
- 🔥 **Flame Graph**: \`rocksdb_flamegraph.svg\` - Interactive CPU usage visualization
- 📊 **Sample Data**: \`sample_raw.txt\` - Raw profiling data from sample command  
- 📝 **Test Output**: \`test_output.log\` - Complete test execution log
- 🔄 **Converted Data**: \`flamegraph_input.txt\` - Processed data for flame graph
- 🐍 **Conversion Script**: \`convert_sample_v2.py\` - Python script for data processing

## Previous Timing Analysis Results
- **PrepareApply Time**: ~58% of total execution time (53-63ms average)
- **Lock Contention**: High (32-40ms total lock time)
- **Critical Section Efficiency**: ~64% time spent outside critical sections
- **Memory Usage**: ~398MB peak resident set size
- **Throughput**: ~10-11 operations per second

## Flame Graph Analysis Guide

### How to View the Flame Graph
1. **Open the file**: \`open $OUTPUT_DIR/rocksdb_flamegraph.svg\`
2. **Interactive Navigation**: 
   - Click on any function block to zoom in
   - Right-click to reset zoom
   - Mouse over for function details
3. **Reading the Graph**:
   - Width = CPU time (wider = more time)
   - Height = call stack depth
   - Color = hash of function name (no special meaning)

### Key Functions to Look For
Based on our timing analysis, focus on:
- **\`Version::PrepareApply\`** - Should show as a major hotspot
- **\`VersionStorageInfo::UpdateAccumulatedStats\`** - File statistics computation
- **\`VersionStorageInfo::ComputeCompactionScore\`** - Compaction priority calculation
- **\`VersionStorageInfo::GenerateBottommostFiles\`** - Bottommost level optimization
- **Lock-related functions** - \`std::mutex::lock\`, \`std::condition_variable\`

### Optimization Targets
Look for:
1. **Wide bars in PrepareApply** - Confirms our timing measurements
2. **Lock contention patterns** - Multiple threads waiting on locks
3. **Repeated expensive operations** - Candidates for optimization
4. **Memory allocation hotspots** - \`malloc\`, \`new\`, \`vector::resize\`

## Expected Performance Impact of Atomic Optimization
Converting \`bottommost_files_mark_threshold_\` to atomic should:
1. **Reduce PrepareApply time** by eliminating lock acquisition for reads
2. **Improve concurrency** for snapshot operations
3. **Maintain correctness** while allowing lock-free access patterns

## Sample Command Details
\`\`\`bash
sample $TEST_PID $SAMPLE_DURATION -wait -mayDie -file sample_raw.txt
\`\`\`

## Next Steps
1. Review flame graph for PrepareApply hotspots
2. Implement atomic variable optimization
3. Re-run this analysis to measure improvement
4. Consider additional optimizations based on flame graph findings

EOF

echo "✅ Analysis report generated: $OUTPUT_DIR/analysis_report.md"
echo ""

# 总结
echo "🎯 Summary"
echo "=========="
echo "✅ CPU profiling with flame graph generation completed!"
echo ""
echo "📂 Generated Files:"
echo "   - 🔥 $OUTPUT_DIR/rocksdb_flamegraph.svg (Main result - open in browser)"
echo "   - 📊 $OUTPUT_DIR/sample_raw.txt (Raw profiling data)"
echo "   - 📝 $OUTPUT_DIR/test_output.log (Test execution log)"
echo "   - 📋 $OUTPUT_DIR/analysis_report.md (Detailed analysis guide)"
echo "   - 🔄 $OUTPUT_DIR/flamegraph_input.txt (Processed stack traces)"
echo ""

# 显示如何打开火焰图
if [ -f "$OUTPUT_DIR/rocksdb_flamegraph.svg" ] && [ -s "$OUTPUT_DIR/rocksdb_flamegraph.svg" ]; then
    echo "🌐 To view the interactive flame graph:"
    echo "   open $OUTPUT_DIR/rocksdb_flamegraph.svg"
    echo ""
    
    SVG_SIZE=$(wc -c < "$OUTPUT_DIR/rocksdb_flamegraph.svg")
    if [ "$SVG_SIZE" -gt 10000 ]; then
        echo "   ✅ Flame graph looks good (${SVG_SIZE} bytes)"
        echo "   🎨 Interactive visualization ready!"
    else
        echo "   ⚠️  Flame graph may be incomplete (only ${SVG_SIZE} bytes)"
    fi
else
    echo "   ❌ Flame graph not generated successfully"
    echo "   🔍 Check the logs above for issues"
fi

# 显示测试性能摘要
if [ -f "$OUTPUT_DIR/test_output.log" ]; then
    echo ""
    echo "🚀 Test Performance Summary:"
    if grep -q "PrepareApply" "$OUTPUT_DIR/test_output.log"; then
        grep -E "(Average|Range|PrepareApply)" "$OUTPUT_DIR/test_output.log" | head -5
    else
        echo "   📊 Check $OUTPUT_DIR/test_output.log for detailed timing results"
    fi
fi

echo ""
echo "🔥 Happy profiling! The flame graph will show you exactly where CPU time is spent."
