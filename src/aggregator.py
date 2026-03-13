import pandas as pd
from typing import Optional


class DataAggregator:
    def aggregate_15m_to_higher(self, df: pd.DataFrame, target_period: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

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

        df = df.set_index("timestamp")
        agg_df = df.resample(freq).agg({
            "close": "last",
            "volume": "sum"
        }).dropna()
        agg_df = agg_df.reset_index()

        return agg_df


aggregator = DataAggregator()
