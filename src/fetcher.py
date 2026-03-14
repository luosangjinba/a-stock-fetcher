import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
import akshare as ak

from .config import config
from .utils import logger, rate_limit


class StockFetcher:
    """A股历史数据获取器"""

    def __init__(self):
        self.request_interval = config.request_interval
        self.validate_rules = config.validate_rules

    def fetch_stock_list(self) -> List[Dict[str, str]]:
        """
        获取A股股票列表

        Returns:
            股票列表，每只股票包含code和name字段
        """
        logger.info("获取股票列表...")
        try:
            df = ak.stock_info_a_code_name()
            stock_list = df.to_dict("records")
            logger.info(f"获取到 {len(stock_list)} 只股票")
            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            raise

    def _get_date_range(self, period: str, days: int) -> tuple:
        """
        计算日期范围

        Args:
            period: 数据周期
            days: 向前天数

        Returns:
            (开始日期, 结束日期) 格式为YYYYMMDD
        """
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        return start_date, end_date

    def fetch_hist_data(
        self,
        symbol: str,
        period: str = "15m",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust: str = "qfq",
        days: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取个股历史数据

        Args:
            symbol: 股票代码 (如 "SH600000")
            period: 数据周期 ("15m", "30m", "60m", "daily")
            start_date: 开始日期 "YYYYMMDD"
            end_date: 结束日期 "YYYYMMDD"
            adjust: 复权类型 ("qfq" 前复权, "hfq" 后复权, "" 不复权)
            days: 获取最近N天的数据（仅分钟级有效）
        """
        try:
            rate_limit(self.request_interval)

            if days and not start_date:
                start_date, end_date = self._get_date_range(period, days)

            period_map = {
                "15m": "15m", "30m": "30m", "60m": "60m", "daily": "daily"
            }
            akshare_period = period_map.get(period, period)

            if akshare_period == "daily":
                symbol_code = symbol.replace("SH", "").replace("SZ", "")
                kwargs = {
                    "symbol": symbol_code,
                    "period": "daily",
                    "adjust": adjust
                }
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                df = ak.stock_zh_a_hist(**kwargs)
            else:
                symbol_code = symbol.replace("SH", "").replace("SZ", "")
                kwargs = {
                    "symbol": symbol_code,
                    "period": akshare_period,
                    "adjust": adjust
                }
                if start_date:
                    kwargs["start_date"] = start_date
                if end_date:
                    kwargs["end_date"] = end_date
                df = ak.stock_zh_a_hist_min_em(**kwargs)

            if df is None or df.empty:
                return None

            # 统一列名：日期列转为timestamp，收盘价转为close，成交量转为volume
            if akshare_period == "daily":
                if "日期" not in df.columns:
                    return None
                df = df.rename(columns={
                    "日期": "timestamp",
                    "收盘": "close",
                    "成交量": "volume"
                })
            else:
                if "时间" not in df.columns:
                    return None
                df = df.rename(columns={
                    "时间": "timestamp",
                    "收盘": "close",
                    "成交量": "volume"
                })

            # 只保留需要的列，并转换为正确的数据类型
            df = df[["timestamp", "close", "volume"]]
            df = df.dropna(subset=["close"])
            df["close"] = df["close"].astype(float)
            df["volume"] = df["volume"].astype(float)

            return df

        except Exception as e:
            logger.warning(f"获取 {symbol} {period} 数据失败: {e}")
            return None

    def validate_data(self, df: pd.DataFrame, period: str) -> bool:
        """
        校验数据条数是否在预期范围内

        Args:
            df: 待校验的数据
            period: 数据周期

        Returns:
            是否通过校验
        """
        if df is None or df.empty:
            return False

        period_key = period.replace("m", "") if period.endswith("m") else period
        if period_key not in self.validate_rules:
            return True

        min_count, max_count = self.validate_rules[period_key]
        return min_count <= len(df) <= max_count


fetcher = StockFetcher()
