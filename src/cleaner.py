import pandas as pd
from typing import Optional

from .config import config
from .utils import logger


class DataCleaner:
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.drop_duplicates(subset=["timestamp"], keep="last")
        df = df.sort_values("timestamp")

        df["close"] = df["close"].ffill()

        df = df.dropna(subset=["close"])

        df = df[df["close"] > 0]

        return df

    def fill_missing_dates(
        self,
        df: pd.DataFrame,
        level: str,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
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

        full_range = pd.DataFrame({"timestamp": date_range})
        df = pd.merge(full_range, df, on="timestamp", how="left")

        df["close"] = df["close"].ffill()
        df["volume"] = df["volume"].fillna(0)

        return df


cleaner = DataCleaner()
