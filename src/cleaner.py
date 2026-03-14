import pandas as pd
from typing import Optional

from .config import config
from .utils import logger


class DataCleaner:
    """数据清洗器"""

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗数据：
        1. 去除重复时间戳
        2. 按时间排序
        3. 前向填充收盘价
        4. 去除收盘价为空的记录
        5. 去除价格为0的记录

        Args:
            df: 原始数据

        Returns:
            清洗后的数据
        """
        if df is None or df.empty:
            return df

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.drop_duplicates(subset=["timestamp"], keep="last")
        df = df.sort_values("timestamp")

        # 前向填充收盘价（用前一个有效值填充）
        df["close"] = df["close"].ffill()

        df = df.dropna(subset=["close"])

        # 过滤价格为0的异常数据
        df = df[df["close"] > 0]

        return df

    def fill_missing_dates(
        self,
        df: pd.DataFrame,
        level: str,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        填充缺失的时间点

        Args:
            df: 数据DataFrame
            level: 数据级别
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            填充后的数据
        """
        if df is None or df.empty:
            return df

        if start_date is None:
            start_date = df["timestamp"].min()
        if end_date is None:
            end_date = df["timestamp"].max()

        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        # 根据周期生成完整时间序列
        if level == "daily":
            date_range = pd.date_range(start_date, end_date, freq="D")
        elif level == "15m":
            date_range = pd.date_range(start_date, end_date, freq="15min")
        elif level == "30m":
            date_range = pd.date_range(start_date, end_date, freq="30min")
        elif level == "60m":
            date_range = pd.date_range(start_date, end_date, freq="60min")
        else:
            return df

        # 合并完整时间序列，缺失位置用NaN填充
        full_range = pd.DataFrame({"timestamp": date_range})
        df = pd.merge(full_range, df, on="timestamp", how="left")

        # 前向填充价格，成交量填充为0
        df["close"] = df["close"].ffill()
        df["volume"] = df["volume"].fillna(0)

        return df


cleaner = DataCleaner()
