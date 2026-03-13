import os
import shutil
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import pandas as pd

from .config import config
from .utils import logger, get_project_root
from .aggregator import aggregator


class DataStorage:
    def __init__(self):
        self.root_dir = get_project_root() / config.root_dir
        self.archive_dir = get_project_root() / config.archive_dir
        self.backup_dir = get_project_root() / config.backup_dir

    def get_stock_file(self, symbol: str, level: str) -> Path:
        level_filename = config.get_level_filename(level)
        stock_dir = self.root_dir / symbol
        stock_dir.mkdir(parents=True, exist_ok=True)
        return stock_dir / level_filename

    def read_data(self, symbol: str, level: str) -> Optional[pd.DataFrame]:
        file_path = self.get_stock_file(symbol, level)
        if not file_path.exists():
            return None

        try:
            df = pd.read_csv(file_path)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            logger.error(f"读取 {symbol} {level} 数据失败: {e}")
            return None

    def get_data(self, symbol: str, level: str) -> Optional[pd.DataFrame]:
        return self.read_data(symbol, level)

    def write_data(
        self,
        symbol: str,
        level: str,
        df: pd.DataFrame,
        mode: str = "append"
    ) -> bool:
        file_path = self.get_stock_file(symbol, level)

        if df is None or df.empty:
            return False

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        if mode == "append" and file_path.exists():
            existing = self.read_data(symbol, level)
            if existing is not None:
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=["timestamp"], keep="last")
                df = df.sort_values("timestamp")
        elif mode == "overwrite" and file_path.exists():
            existing = self.read_data(symbol, level)
            if existing is not None:
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=["timestamp"], keep="last")
                df = df.sort_values("timestamp")

        df = df[["timestamp", "close", "volume"]]
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        try:
            df.to_csv(file_path, index=False)
            return True
        except Exception as e:
            logger.error(f"写入 {symbol} {level} 数据失败: {e}")
            return False

    def truncate_data(self, symbol: str, level: str) -> int:
        df = self.read_data(symbol, level)
        if df is None or df.empty:
            return 0

        if level == "daily":
            return len(df)
        
        max_rows = 512
        if len(df) > max_rows:
            df = df.tail(max_rows)
            file_path = self.get_stock_file(symbol, level)
            df.to_csv(file_path, index=False)

        return len(df)

    def archive_stock(self, symbol: str) -> bool:
        stock_dir = self.root_dir / symbol
        if not stock_dir.exists():
            return False

        archive_path = self.archive_dir / symbol
        try:
            shutil.move(str(stock_dir), str(archive_path))
            logger.info(f"已归档股票: {symbol}")
            return True
        except Exception as e:
            logger.error(f"归档股票 {symbol} 失败: {e}")
            return False

    def get_existing_stocks(self, level: str = None) -> List[str]:
        if not self.root_dir.exists():
            return []
        if level:
            return [d.name for d in self.root_dir.iterdir() if d.is_dir() and (d / f"{level}.csv").exists()]
        return [d.name for d in self.root_dir.iterdir() if d.is_dir()]

    def backup(self) -> bool:
        if not self.root_dir.exists():
            return False

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_{timestamp}.tar.gz"
        backup_path = self.backup_dir / backup_name

        self.backup_dir.mkdir(parents=True, exist_ok=True)

        try:
            with tarfile.open(backup_path, "w:gz") as tar:
                tar.add(self.root_dir, arcname="data")
            logger.info(f"数据库已备份: {backup_name}")
            return True
        except Exception as e:
            logger.error(f"备份失败: {e}")
            return False


storage = DataStorage()
