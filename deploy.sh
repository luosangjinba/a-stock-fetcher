#!/bin/bash

# A股数据获取系统 - 一键部署脚本

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=========================================="
echo "  A股历史数据获取系统 - 一键部署"
echo "=========================================="
echo ""

# 1. 创建必要目录
echo "[1/6] 创建目录结构..."
mkdir -p data archive backup logs
echo "  ✅ 目录创建完成"

# 2. 安装依赖
echo ""
echo "[2/6] 安装Python依赖..."
pip3 install -r requirements.txt
echo "  ✅ 依赖安装完成"

# 3. Telegram配置
echo ""
echo "[3/6] Telegram通知配置（可选）"
echo "  如不需要Telegram通知，可直接回车跳过"
read -p "  TELEGRAM_BOT_TOKEN: " TELEGRAM_TOKEN
read -p "  TELEGRAM_CHAT_ID: " TELEGRAM_CHAT_ID

if [ -n "$TELEGRAM_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
    # 写入环境变量文件
    cat > .env << EOF
TELEGRAM_TOKEN=$TELEGRAM_TOKEN
TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID
EOF
    echo "  ✅ Telegram配置已保存到 .env"
    echo "  ℹ️  如需每次运行自动加载，请执行: source .env"
else
    echo "  ⏭️  跳过 Telegram 配置"
fi

# 4. 初始化数据库（可选）
echo ""
echo "[4/6] 初始化数据库"
read -p "  是否立即初始化数据库？(y/N): " INIT_DB
if [ "$INIT_DB" = "y" ] || [ "$INIT_DB" = "Y" ]; then
    echo "  ℹ️  初始化需要较长时间，可稍后手动执行: python3 main.py init"
fi

# 5. 设置定时任务
echo ""
echo "[5/6] 设置定时任务（cron）"
read -p "  是否设置每日17:10自动执行？(y/N): " SET_CRON
if [ "$SET_CRON" = "y" ] || [ "$SET_CRON" = "Y" ]; then
    # 检查cron是否已存在
    CRON_JOB="10 17 * * 1-5 cd $PROJECT_DIR && /usr/bin/python3 main.py daily >> $PROJECT_DIR/logs/cron.log 2>&1"
    
    # 移除旧的定时任务（如果存在）
    crontab -l 2>/dev/null | grep -v "main.py daily" | crontab -
    
    # 添加新的定时任务
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "  ✅ 定时任务已设置: 每日17:10执行"
    echo "  ℹ️  查看定时任务: crontab -l"
else
    echo "  ⏭️  跳过定时任务设置"
fi

# 6. 验证安装
echo ""
echo "[6/6] 验证安装..."
python3 -c "import akshare; import pandas; import yaml" && echo "  ✅ 依赖验证通过" || echo "  ❌ 依赖验证失败"

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "后续操作："
echo "  1. 初始化数据库: python3 main.py init"
echo "  2. 测试运行:     python3 main.py daily"
echo "  3. 查看日志:     tail -f logs/fetcher.log"
echo ""
