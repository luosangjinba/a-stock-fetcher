"""
A股数据健康检查模块

功能：
- 宕机告警：超过3天未运行则Telegram告警
- 健康检查：检查各股票数据完整性
- 通知：Telegram发送状态报告
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import config
from src.utils import logger, save_status, load_status


# Telegram配置（从环境变量读取）
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# 离线告警阈值（天）
OFFLINE_THRESHOLD_DAYS = 3


def check_offline_alert() -> bool:
    """
    检查是否需要发送宕机告警
    
    检查上次成功运行距今是否超过阈值，超过则发送Telegram告警
    
    Returns:
        是否触发告警
    """
    status = load_status()
    last_success = status.get("last_success", "")
    
    if not last_success:
        logger.info("首次运行，无历史成功记录")
        return False
    
    last_success_date = datetime.strptime(last_success, "%Y-%m-%d")
    days_offline = (datetime.now() - last_success_date).days
    
    if days_offline > OFFLINE_THRESHOLD_DAYS:
        message = f"⚠️ 服务器宕机告警\n\n" \
                  f"距上次成功运行已过去 <b>{days_offline} 天</b>\n" \
                  f"上次成功: {last_success}\n" \
                  f"阈值: {OFFLINE_THRESHOLD_DAYS} 天\n\n" \
                  f"请立即检查服务器状态！"
        send_telegram(message)
        logger.warning(f"服务器已离线 {days_offline} 天，发送告警")
        return True
    
    logger.info(f"上次成功运行: {last_success} ({days_offline} 天前)")
    return False


def send_telegram(message: str) -> bool:
    """
    发送Telegram消息
    
    Args:
        message: 要发送的消息内容
        
    Returns:
        是否发送成功
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram通知未配置 (TELEGRAM_TOKEN/TELEGRAM_CHAT_ID)")
        return False
    try:
        import requests
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        response = requests.post(url, data=data, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False


def notify_start():
    """发送增量更新开始通知"""
    send_telegram("🚀 A股数据增量更新已开始")


def notify_complete(duration_seconds: int, success_count: int, fail_count: int, new_count: int, removed_count: int):
    """
    发送增量更新完成通知
    
    Args:
        duration_seconds: 运行时长（秒）
        success_count: 成功获取的股票数
        fail_count: 获取失败的股票数
        new_count: 新股数量
        removed_count: 退市数量
    """
    minutes = duration_seconds // 60
    seconds = duration_seconds % 60
    
    lines = [
        "✅ 增量更新完成",
        f"⏱️ 耗时: {minutes}分{seconds}秒",
        f"✅ 成功: {success_count}",
        f"❌ 失败: {fail_count}",
        f"🆕 新股: {new_count}",
        f"🗑️ 退市: {removed_count}"
    ]
    send_telegram("\n".join(lines))


def notify_error(message: str):
    """
    发送错误通知
    
    Args:
        message: 错误消息
    """
    send_telegram(f"❌ 错误: {message}")


def notify_batch_complete(batch_num: int, total_batches: int, success_count: int, fail_count: int, duration_seconds: int):
    """
    发送批次完成通知
    
    Args:
        batch_num: 当前批次号
        total_batches: 总批次数
        success_count: 成功数
        fail_count: 失败数
        duration_seconds: 运行时长(秒)
    """
    minutes = duration_seconds // 60
    seconds = duration_seconds % 60
    
    lines = [
        f"📊 批次 {batch_num}/{total_batches} 完成",
        f"⏱️ 耗时: {minutes}分{seconds}秒",
        f"✅ 成功: {success_count}",
        f"❌ 失败: {fail_count}"
    ]
    send_telegram("\n".join(lines))


def main():
    print("开始数据健康检查...")

    data_dir = Path(config.data_dir)
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    current_hour = datetime.now().hour
    check_date = yesterday_str if current_hour < 15 else yesterday_str

    stocks = [d.name for d in data_dir.iterdir() if d.is_dir()]
    total_stocks = len(stocks)
    
    normal_count = 0
    suspended = []
    level_counts = {"15m": 0, "30m": 0, "60m": 0, "120m": 0, "daily": 0}

    for symbol in stocks:
        is_normal = True
        for level in ["15m", "30m", "60m", "120m", "daily"]:
            fpath = data_dir / symbol / f"{level}.csv"
            if fpath.exists():
                try:
                    with open(fpath) as f:
                        lines = f.readlines()
                        if len(lines) > 1:
                            level_counts[level] += len(lines) - 1
                            last_date = lines[-1].split(",")[0][:10]
                            if last_date != check_date:
                                suspended.append((symbol, last_date))
                                is_normal = False
                                break
                        else:
                            is_normal = False
                except:
                    is_normal = False
                    break
            else:
                is_normal = False
                break
        
        if is_normal:
            normal_count += 1

    result = {
        "total_stocks": total_stocks,
        "normal_stocks": normal_count,
        "suspended": suspended,
        "level_counts": level_counts
    }

    lines_msg = [
        "📊 A股数据健康检查",
        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"✅ 正常: {result['normal_stocks']}/{result['total_stocks']}",
        f"⚠️ 停牌: {len(result['suspended'])}",
    ]

    if result["suspended"]:
        for symbol, last_date in result["suspended"]:
            lines_msg.append(f"  {symbol}: {last_date}")

    lines_msg.append("")
    lines_msg.append("📈 数据量:")
    for level, count in result["level_counts"].items():
        avg = count // result["total_stocks"] if result["total_stocks"] else 0
        lines_msg.append(f"  {level}: {count:,} ({avg}/股)")

    report = "\n".join(lines_msg)
    print(report)

    send_telegram(report)
    
    save_status({
        "last_health_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "normal_stocks": result["normal_stocks"],
        "total_stocks": result["total_stocks"],
        "suspended_count": len(result["suspended"])
    })

    print("完成!")


if __name__ == "__main__":
    main()
