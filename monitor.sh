#!/bin/bash
# 监控脚本 - 查看初始化进度

echo "=== A股数据获取系统状态 ==="
echo ""

echo "--- 运行中进程 ---"
ps aux | grep "python3 main.py" | grep -v grep || echo "无运行中的任务"
echo ""

echo "--- 股票数量统计 ---"
DATA_DIR="/home/leo/myworkspace/a-stock-fetcher/data"
if [ -d "$DATA_DIR" ]; then
    count=$(ls -1 "$DATA_DIR" 2>/dev/null | wc -l)
    echo "已获取股票数: $count"
else
    echo "数据目录不存在"
fi
echo ""

echo "--- 最新日志 ---"
tail -20 /home/leo/myworkspace/a-stock-fetcher/logs/fetcher.log 2>/dev/null || echo "暂无日志"
echo ""

echo "--- 状态文件 ---"
cat /home/leo/myworkspace/a-stock-fetcher/status.json 2>/dev/null || echo "暂无状态文件"
