import os
import yaml
from pathlib import Path
from typing import Any, Dict, List, Tuple


class Config:
    _config: Dict[str, Any] = None  # type: ignore

    @classmethod
    def _load(cls) -> None:
        if cls._config is None:
            config_path = Path(__file__).parent.parent / "config" / "config.yaml"
            with open(config_path, "r", encoding="utf-8") as f:
                cls._config = yaml.safe_load(f)

    @property
    def root_dir(self) -> str:
        self._load()
        return self._config["data"]["root_dir"]

    @property
    def data_dir(self) -> str:
        self._load()
        return str(Path(__file__).parent.parent / self._config["data"]["root_dir"])

    @property
    def archive_dir(self) -> str:
        self._load()
        return self._config["data"]["archive_dir"]

    @property
    def backup_dir(self) -> str:
        self._load()
        return self._config["data"]["backup_dir"]

    @property
    def levels(self) -> Dict[str, Any]:
        self._load()
        return self._config["data"]["levels"]

    @property
    def run_time(self) -> str:
        self._load()
        return self._config["fetcher"]["run_time"]

    @property
    def batch_size(self) -> int:
        self._load()
        return self._config["fetcher"]["init"]["batch_size"]

    @property
    def batch_interval(self) -> int:
        self._load()
        return self._config["fetcher"]["init"]["batch_interval"]

    @property
    def retry_times(self) -> int:
        self._load()
        return self._config["fetcher"]["daily"]["retry_times"]

    @property
    def request_interval(self) -> float:
        self._load()
        return self._config["fetcher"]["daily"]["request_interval"]

    @property
    def validate_rules(self) -> Dict[str, List[int]]:
        self._load()
        return self._config["fetcher"]["validate"]

    @property
    def log_file(self) -> str:
        self._load()
        return self._config["logging"]["file"]

    @property
    def status_file(self) -> str:
        self._load()
        return self._config["status"]["file"]

    def get_level_days(self, level: str) -> int:
        self._load()
        if level in self._config["data"]["levels"]:
            return self._config["data"]["levels"][level]["days"]
        if level == "30m":
            return 64
        if level == "60m":
            return 128
        if level == "120m":
            return 256
        return 32

    def get_level_filename(self, level: str) -> str:
        self._load()
        if level in self._config["data"]["levels"]:
            return self._config["data"]["levels"][level]["filename"]
        return "15m.csv"

    def get_validate_range(self, level: str) -> Tuple[int, int]:
        self._load()
        return tuple(self._config["fetcher"]["validate"][level])


config = Config()
