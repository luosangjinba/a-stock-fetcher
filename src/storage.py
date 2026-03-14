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
    """A股历史数据存储器"""

    def __init__(self):
        self.root_dir = get_project_root() / config.root_dir
        self.archive_dir = get_project_root() / config.archive_dir
        self.backup_dir = get_project_root() / config.backup_dir

    def get_stock_file(self, symbol: str, level: str) -> Path:
        """
        获取股票数据文件路径

        Args:
            symbol: 股票代码（如SH600000）
            level: 数据级别（15m/30m/60m/120m/daily）

        Returns:
            CSV文件路径
        """
        level_filename = config.get_level_filename(level)
        stock_dir = self.root_dir / symbol
        stock_dir.mkdir(parents=True, exist_ok=True)
        return stock_dir / level_filename
        level_filename = config.get_level_filename(level)
        stock_dir = self.root_dir / symbol
        stock_dir.mkdir(parents=True, exist_ok=True)
        return stock_dir / level_filename

    def read_data(self, symbol: str, level: str) -> Optional[pd.DataFrame]:
        """
        读取股票数据

        Args:
            symbol: 股票代码
            level: 数据级别

        Returns:
            DataFrame或None
        """
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
        """
        写入股票数据

        Args:
            symbol: 股票代码
            level: 数据级别
            df: 数据DataFrame
            mode: 写入模式，append=追加，overwrite=覆盖后合并

        Returns:
            是否写入成功
        """
        file_path = self.get_stock_file(symbol, level)

        if df is None or df.empty:
            return False

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        # 合并现有数据并去重，保留最新记录
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

        # 只保留需要的列，转换时间格式为字符串
        df = df[["timestamp", "close", "volume"]]
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        try:
            df.to_csv(file_path, index=False)
            return True
        except Exception as e:
            logger.error(f"写入 {symbol} {level} 数据失败: {e}")
            return False

    def truncate_data(self, symbol: str, level: str) -> int:
        """
        裁剪过期数据，保持数据文件大小合理

        Args:
            symbol: 股票代码
            level: 数据级别

        Returns:
            裁剪后的记录数
        """
        df = self.read_data(symbol, level)
        if df is None or df.empty:
            return 0

        # 日线数据保留全部，不裁剪
        if level == "daily":
            return len(df)
        
        # 分钟级数据最多保留512条（约8.5天的15m数据）
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
