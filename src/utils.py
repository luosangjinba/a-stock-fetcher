import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

from .config import config


def setup_logging() -> logging.Logger:
    """
    配置日志系统

    同时输出到文件和控制台
    """
    log_file = config.log_file
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("astock_fetcher")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger


logger = setup_logging()


def get_trading_dates(start_date: str, end_date: str) -> List[str]:
    """
    获取指定日期区间的交易日列表

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        交易日列表
    """
    import akshare as ak
    df = ak.tool_trade_date_hist_sina()
    df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]
    return df["trade_date"].tolist()


def get_stock_list() -> List[Dict[str, str]]:
    """
    获取A股股票列表（简单封装）
    """
    import akshare as ak
    df = ak.stock_info_a_code_name()
    return df.to_dict("records")


def is_trading_day(date: Optional[str] = None) -> bool:
    """
    判断是否为交易日（周一到周五）

    Args:
        date: 日期字符串，默认今天

    Returns:
        是否为交易日（周一到周五）
    """
    if date is None:
        check_date = datetime.now()
    else:
        check_date = datetime.strptime(date, "%Y-%m-%d")
    # 周一到周五为交易日 (weekday: 0=周一, 4=周五)
    return check_date.weekday() < 5


def get_last_trading_day(date: Optional[str] = None) -> str:
    """
    获取指定日期之前的最近一个交易日

    Args:
        date: 参考日期，默认今天

    Returns:
        最近交易日，格式YYYY-MM-DD
    """
    if date is None:
        check_date = datetime.now()
    else:
        check_date = datetime.strptime(date, "%Y-%m-%d")

    import akshare as ak
    df = ak.tool_trade_date_hist_sina()
    df = df.sort_values("trade_date")

    # 向前查找最多10天
    for i in range(1, 10):
        result_date = (check_date - timedelta(days=i)).strftime("%Y-%m-%d")
        if result_date in df["trade_date"].values:
            return result_date
    return ""


def load_status() -> Dict[str, Any]:
    """
    加载运行状态

    Returns:
        状态字典
    """
    status_file = config.status_file
    if os.path.exists(status_file):
        with open(status_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "last_run": "",
        "last_success": "",
        "success_count": 0,
        "fail_list": [],
        "new_stocks": []
    }


def save_status(status: Dict[str, Any]) -> None:
    """
    保存运行状态

    Args:
        status: 状态字典
    """
    status_file = config.status_file
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)


def get_project_root() -> Path:
    """
    获取项目根目录
    """
    return Path(__file__).parent.parent


def rate_limit(interval: float) -> None:
    """
    请求限流：休眠指定秒数

    Args:
        interval: 休眠秒数
    """
    time.sleep(interval)
