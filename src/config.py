import os
import yaml
from pathlib import Path
from typing import Any, Dict, List, Tuple


class Config:
    """
    配置管理类

    从config.yaml加载配置，支持延迟加载和缓存
    """
    _config: Dict[str, Any] = None  # type: ignore

    @classmethod
    def _load(cls) -> None:
        """加载配置文件"""
        if cls._config is None:
            config_path = Path(__file__).parent.parent / "config" / "config.yaml"
            with open(config_path, "r", encoding="utf-8") as f:
                cls._config = yaml.safe_load(f)

    @property
    def root_dir(self) -> str:
        """数据存储根目录"""
        self._load()
        return self._config["data"]["root_dir"]

    @property
    def data_dir(self) -> str:
        """数据存储完整路径"""
        self._load()
        return str(Path(__file__).parent.parent / self._config["data"]["root_dir"])

    @property
    def archive_dir(self) -> str:
        """归档目录"""
        self._load()
        return self._config["data"]["archive_dir"]

    @property
    def backup_dir(self) -> str:
        """备份目录"""
        self._load()
        return self._config["data"]["backup_dir"]

    @property
    def levels(self) -> Dict[str, Any]:
        """数据级别配置"""
        self._load()
        return self._config["data"]["levels"]

    @property
    def run_time(self) -> str:
        """每日运行时间"""
        self._load()
        return self._config["fetcher"]["run_time"]

    @property
    def batch_size(self) -> int:
        """初始化批次大小"""
        self._load()
        return self._config["fetcher"]["init"]["batch_size"]

    @property
    def batch_interval(self) -> int:
        """批次间隔（秒）"""
        self._load()
        return self._config["fetcher"]["init"]["batch_interval"]

    @property
    def retry_times(self) -> int:
        """失败重试次数"""
        self._load()
        return self._config["fetcher"]["daily"]["retry_times"]

    @property
    def request_interval(self) -> float:
        """请求间隔（秒）"""
        self._load()
        return self._config["fetcher"]["daily"]["request_interval"]

    @property
    def validate_rules(self) -> Dict[str, List[int]]:
        """数据校验规则"""
        self._load()
        return self._config["fetcher"]["validate"]

    @property
    def log_file(self) -> str:
        """日志文件路径"""
        self._load()
        return self._config["logging"]["file"]

    @property
    def status_file(self) -> str:
        """状态文件路径"""
        self._load()
        return self._config["status"]["file"]

    @property
    def tushare_token(self) -> str:
        """Token from env TUSHARE_TOKEN or ~/.tushare_token"""
        self._load()
        token = os.environ.get("TUSHARE_TOKEN", "").strip()
        if token:
            return token
        
        token_file = Path.home() / ".tushare_token"
        if token_file.exists():
            return token_file.read_text().strip()
        
        return ""

    @property
    def industry_lookback_days(self) -> int:
        self._load()
        ind_cfg = self._config.get("industry", {})
        return ind_cfg.get("analysis", {}).get("lookback_days", 20)

    @property
    def industry_top_stocks(self) -> int:
        self._load()
        ind_cfg = self._config.get("industry", {})
        return ind_cfg.get("analysis", {}).get("top_stocks", 400)

    @property
    def industry_output_top(self) -> int:
        self._load()
        ind_cfg = self._config.get("industry", {})
        return ind_cfg.get("analysis", {}).get("output_top", 12)

    def get_level_days(self, level: str) -> int:
        """
        获取指定级别的数据保留天数

        Args:
            level: 数据级别

        Returns:
            保留天数
        """
        self._load()
        if level in self._config["data"]["levels"]:
            return self._config["data"]["levels"][level]["days"]
        # 默认值
        if level == "30m":
            return 64
        if level == "60m":
            return 128
        if level == "120m":
            return 256
        return 32

    def get_level_filename(self, level: str) -> str:
        """
        获取指定级别的文件名

        Args:
            level: 数据级别

        Returns:
            文件名
        """
        self._load()
        if level in self._config["data"]["levels"]:
            return self._config["data"]["levels"][level]["filename"]
        return "15m.csv"

    def get_validate_range(self, level: str) -> Tuple[int, int]:
        """
        获取指定级别的数据条数校验范围

        Args:
            level: 数据级别

        Returns:
            (最小条数, 最大条数)
        """
        self._load()
        return tuple(self._config["fetcher"]["validate"][level])


config = Config()
