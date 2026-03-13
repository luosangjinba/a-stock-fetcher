#!/usr/bin/env python3
import os
import sys
import time
import signal
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from src.utils import logger, is_trading_day


class Scheduler:
    def __init__(self):
        self.run_time = config.run_time
        self.running = True

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("收到停止信号，退出调度...")
        self.running = False

    def wait_until(self, target_time: str) -> None:
        while self.running:
            now = datetime.now()
            target = datetime.strptime(target_time, "%H:%M").time()

            current_time = now.time()
            target_datetime = datetime.combine(now.date(), target)

            if current_time >= target:
                target_datetime = target_datetime + timedelta(days=1)

            seconds = (target_datetime - now).total_seconds()
            if seconds > 0:
                logger.info(f"等待 {int(seconds)} 秒后执行任务...")
                time.sleep(seconds)

            if self.running:
                break

    def run(self) -> None:
        logger.info(f"调度器启动，运行时间: {self.run_time}")

        while self.running:
            today = datetime.now().strftime("%Y-%m-%d")

            if is_trading_day(today):
                logger.info(f"今日 {today} 是交易日，开始执行...")

                os.chdir(PROJECT_ROOT)
                exit_code = os.system(f"{sys.executable} main.py daily")

                if exit_code == 0:
                    logger.info("每日任务执行成功")
                else:
                    logger.error(f"每日任务执行失败，退出码: {exit_code}")
            else:
                logger.info(f"今日 {today} 非交易日，跳过")

            self.wait_until(self.run_time)

        logger.info("调度器已停止")


if __name__ == "__main__":
    from datetime import timedelta

    scheduler = Scheduler()
    scheduler.run()
