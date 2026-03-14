import pandas as pd
from typing import Optional


class DataAggregator:
    """数据聚合器：将15m数据聚合为更高周期"""

    def aggregate_15m_to_higher(self, df: pd.DataFrame, target_period: str) -> pd.DataFrame:
        """
        将15分钟数据聚合为更高周期

        Args:
            df: 15分钟数据
            target_period: 目标周期 (30m/60m/120m/daily)

        Returns:
            聚合后的数据
        """
        if df is None or df.empty:
            return df

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        # 周期映射
        if target_period == "30m":
            freq = "30min"
        elif target_period == "60m":
            freq = "60min"
        elif target_period == "120m":
            freq = "120min"
        elif target_period == "daily":
            freq = "1D"
        else:
            return df

        # 聚合：收盘价取最后一个，成交量求和
        df = df.set_index("timestamp")
        agg_df = df.resample(freq).agg({
            "close": "last",
            "volume": "sum"
        }).dropna()
        agg_df = agg_df.reset_index()

        return agg_df


aggregator = DataAggregator()
