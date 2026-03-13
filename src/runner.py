import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Set
import pandas as pd
import time

from .config import config
from .fetcher import fetcher
from .storage import storage
from .cleaner import cleaner
from .utils import logger, load_status, save_status, get_last_trading_day
from .health_check import notify_start, notify_complete, notify_error


class AStockRunner:
    def __init__(self):
        self.levels = list(config.levels.keys())
        self.status = load_status()

    def run_init(self) -> None:
        logger.info("=" * 50)
        logger.info("开始初始化数据库...")
        logger.info("=" * 50)

        stock_list = fetcher.fetch_stock_list()
        existing_stocks = set(storage.get_existing_stocks("15m"))

        batch_size = config.batch_size
        batch_interval = config.batch_interval

        total_batches = (len(stock_list) + batch_size - 1) // batch_size

        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i:i + batch_size]
            batch_num = i // batch_size + 1

            logger.info(f"处理批次 {batch_num}/{total_batches}")

            for stock in batch:
                symbol = stock.get("code", "")
                if not symbol:
                    continue

                symbol = self._normalize_symbol(symbol)
                if not symbol:
                    continue

                if symbol in existing_stocks:
                    continue

                self._fetch_all_levels(symbol)

            if i + batch_size < len(stock_list):
                logger.info(f"批次完成，休眠 {batch_interval} 秒...")
                time.sleep(batch_interval)

        storage.backup()
        logger.info("初始化完成！")

    def run_daily(self) -> None:
        start_time = time.time()
        
        from src.health_check import notify_start, check_offline_alert
        check_offline_alert()
        notify_start()
        
        logger.info("=" * 50)
        logger.info("步骤1: 开始每日增量更新...")
        logger.info("=" * 50)

        logger.info("=" * 50)
        logger.info("步骤2: 检查新股/退市股...")
        logger.info("=" * 50)

        stock_list = fetcher.fetch_stock_list()
        existing_stocks = set(storage.get_existing_stocks())
        current_stocks = {s for s in (self._normalize_symbol(s.get("code", "")) for s in stock_list) if s}

        new_stocks = current_stocks - existing_stocks
        removed_stocks = existing_stocks - current_stocks

        for symbol in new_stocks:
            logger.info(f"新股: {symbol}")
            self._fetch_all_levels(symbol)

        for symbol in removed_stocks:
            logger.info(f"退市: {symbol}，移至归档")
            storage.archive_stock(symbol)

        logger.info("=" * 50)
        logger.info("步骤3: 获取增量数据...")
        logger.info("=" * 50)

        fail_list = []
        success_count = 0

        batch_size = 2000
        batch_interval = 300  # 5分钟
        
        stock_symbols = [self._normalize_symbol(s.get("code", "")) for s in stock_list]
        stock_symbols = [s for s in stock_symbols if s]
        
        total_batches = (len(stock_symbols) + batch_size - 1) // batch_size
        
        for i in range(0, len(stock_symbols), batch_size):
            batch = stock_symbols[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            logger.info(f"处理批次 {batch_num}/{total_batches} ({len(batch)}只股票)")
            
            for symbol in batch:
                if self._fetch_all_levels(symbol):
                    success_count += 1
                else:
                    fail_list.append(symbol)
            
            if i + batch_size < len(stock_symbols):
                logger.info(f"批次完成，休眠 {batch_interval} 秒...")
                time.sleep(batch_interval)

        logger.info("=" * 50)
        logger.info("步骤4: 检查数据完整性并补全...")
        logger.info("跳过 (增量获取已包含最新数据)")
        # incomplete_list = self._check_data_completeness(current_stocks)
        # retry_fail_list = []
        # for symbol in incomplete_list:
        #     logger.info(f"尝试补全: {symbol}")
        #     if not self._fetch_all_levels(symbol):
        #         retry_fail_list.append(symbol)
        #         logger.warning(f"补全失败: {symbol}")
        # if retry_fail_list:
        #     logger.warning(f"无法补全的股票: {len(retry_fail_list)} 只")
        #     for symbol in retry_fail_list[:10]:
        #         logger.warning(f"  - {symbol}")

        logger.info("=" * 50)
        logger.info("步骤5: 数据降采样并生成报告...")
        logger.info("=" * 50)

        for level in self.levels:
            logger.info(f"清理 {level} 过期数据...")
            for symbol in current_stocks:
                storage.truncate_data(symbol, level)

        self._generate_data_report(current_stocks)

        logger.info("=" * 50)
        logger.info("步骤6: 备份数据...")
        logger.info("=" * 50)

        storage.backup()
        logger.info("✅ 数据已备份")

        self.status = {
            "last_run": datetime.now().strftime("%Y-%m-%d"),
            "last_success": datetime.now().strftime("%Y-%m-%d"),
            "success_count": success_count,
            "fail_list": fail_list[:100],
            "new_stocks": list(new_stocks)[:100]
        }
        save_status(self.status)

        duration = int(time.time() - start_time)
        
        notify_complete(duration, success_count, len(fail_list), len(new_stocks), len(removed_stocks))
        
        if len(fail_list) > 100:
            notify_error(f"大量股票获取失败: {len(fail_list)}只")
        
        logger.info(f"每日更新完成！成功: {success_count}, 失败: {len(fail_list)}")

    def _check_data_completeness(self, stocks: Set[str]) -> List[str]:
        incomplete = []
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        current_hour = datetime.now().hour
        check_date = yesterday_str if current_hour < 15 else yesterday_str

        for symbol in stocks:
            for level in self.levels:
                df = storage.read_data(symbol, level)
                if df is None or df.empty:
                    incomplete.append(symbol)
                    break

                last_date = df["timestamp"].max()
                last_date_str = str(last_date)[:10]
                if last_date_str != check_date:
                    incomplete.append(symbol)
                    break

        if incomplete:
            logger.info(f"发现 {len(incomplete)} 只股票数据不完整")

        return incomplete

    def _generate_data_report(self, stocks: Set[str]) -> None:
        report = {
            "total_stocks": len(stocks),
            "levels": {}
        }

        for level in self.levels:
            total_records = 0
            date_ranges = []

            for symbol in stocks:
                df = storage.read_data(symbol, level)
                if df is None or df.empty:
                    continue

                total_records += len(df)
                first_date = str(df["timestamp"].min())[:10]
                last_date = str(df["timestamp"].max())[:10]
                date_ranges.append((first_date, last_date))

            report["levels"][level] = {
                "total_records": total_records,
                "avg_records": total_records // len(stocks) if stocks else 0
            }

        logger.info("=" * 50)
        logger.info("数据报告:")
        logger.info(f"  总股票数: {report['total_stocks']}")
        for level, info in report["levels"].items():
            logger.info(f"  {level}: 总记录 {info['total_records']}, 平均 {info['avg_records']} 条/股")
        logger.info("=" * 50)

    def run_check_suspended(self) -> None:
        from pathlib import Path
        from datetime import date

        logger.info("=" * 50)
        logger.info("检查当日停牌股票...")
        logger.info("=" * 50)

        data_dir = Path(config.data_dir)

        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        today = datetime.now()
        current_hour = today.hour
        
        check_date = yesterday_str if current_hour < 15 else yesterday_str

        suspended_stocks = []
        normal_stocks = 0

        for stock_dir in data_dir.iterdir():
            if not stock_dir.is_dir():
                continue

            f15m = stock_dir / "15m.csv"
            if not f15m.exists():
                continue

            try:
                lines = open(f15m).readlines()
                if len(lines) < 2:
                    continue
                last_date = lines[-1].split(",")[0][:10]

                if last_date != check_date:
                    symbol = stock_dir.name
                    suspended_stocks.append((symbol, last_date))
                else:
                    normal_stocks += 1
            except Exception:
                continue

        logger.info(f"正常交易股票: {normal_stocks}")
        logger.info(f"停牌/未更新股票: {len(suspended_stocks)}")

        if suspended_stocks:
            logger.info("\n停牌/未更新股票列表:")
            for symbol, last_date in sorted(suspended_stocks):
                logger.info(f"  {symbol}: 最后数据 {last_date}")

        self.status = {
            "last_run": datetime.now().strftime("%Y-%m-%d"),
            "suspended_count": len(suspended_stocks),
            "suspended_stocks": [s[0] for s in suspended_stocks]
        }
        save_status(self.status)

    def run_fix_missing(self) -> None:
        logger.info("=" * 50)
        logger.info("检查并补全缺失数据...")
        logger.info("=" * 50)

        last_run = self.status.get("last_run", "")
        if not last_run:
            logger.warning("无历史运行记录，跳过补全")
            return

        last_date = datetime.strptime(last_run, "%Y-%m-%d")
        today = datetime.now()

        missing_days = (today - last_date).days
        if missing_days <= 1:
            logger.info("数据已是最新，无需补全")
            return

        logger.info(f"发现 {missing_days} 天数据缺失，开始补全...")

        stock_list = fetcher.fetch_stock_list()
        for stock in stock_list[:100]:
            symbol = self._normalize_symbol(stock.get("code", ""))
            if not symbol:
                continue

            for level in self.levels:
                self._fetch_and_update(symbol, level, last_run, today.strftime("%Y%m%d"))

        storage.backup()
        logger.info("补全完成！")

    def _fetch_all_levels(self, symbol: str) -> bool:
        today = datetime.now().strftime("%Y%m%d")
        all_success = True

        # 15m: 增量获取
        base_df = storage.read_data(symbol, "15m")
        if base_df is not None and not base_df.empty:
            last_date = str(base_df["timestamp"].max())[:10].replace("-", "")
            logger.info(f"{symbol} 15m 增量获取: {last_date} -> {today}")
            self._fetch_and_update(symbol, "15m", last_date, today, None)
        else:
            # 新股：获取全部可用数据
            logger.info(f"{symbol} 15m 全量获取")
            self._fetch_and_update(symbol, "15m", None, today, None)

        # 30m/60m/120m/daily: 从15m聚合生成
        self._generate_aggregated_levels(symbol)

        return all_success

    def _generate_aggregated_levels(self, symbol: str) -> bool:
        from .aggregator import aggregator

        base_df = storage.read_data(symbol, "15m")
        if base_df is None or base_df.empty:
            return False

        for level in ["30m", "60m", "120m", "daily"]:
            agg_df = aggregator.aggregate_15m_to_higher(base_df, level)
            if agg_df is not None and not agg_df.empty:
                storage.write_data(symbol, level, agg_df, mode="overwrite")
                logger.info(f"{symbol} {level}: 生成 {len(agg_df)} 条聚合数据")

        return True

    def _fetch_and_update(
        self,
        symbol: str,
        level: str,
        start_date: str = None,
        end_date: str = None,
        days: int = None
    ) -> bool:
        try:
            df = fetcher.fetch_hist_data(
                symbol=symbol,
                period=level,
                start_date=start_date,
                end_date=end_date,
                days=days
            )

            if df is None or df.empty:
                logger.warning(f"{symbol} {level}: 无数据")
                return False

            if not fetcher.validate_data(df, level):
                logger.warning(f"{symbol} {level}: 数据校验失败，条数 {len(df)}")

            df = cleaner.clean(df)
            storage.write_data(symbol, level, df, mode="append")

            logger.info(f"{symbol} {level}: 获取 {len(df)} 条")
            return True

        except Exception as e:
            logger.error(f"{symbol} {level}: 获取失败 - {e}")
            return False

    def _normalize_symbol(self, code: str) -> str:
        code = code.strip()
        if code.startswith("6"):
            return f"SH{code}"
        elif code.startswith(("0", "3")):
            return f"SZ{code}"
        elif code.startswith(("8", "9")):
            return None  # 北交所、上海B股，不获取
        return code


def main():
    parser = argparse.ArgumentParser(description="A股历史数据获取系统")
    parser.add_argument(
        "mode",
        choices=["init", "daily", "fix-missing", "check-suspended"],
        help="运行模式: init=初始化, daily=每日更新, fix-missing=补全缺失, check-suspended=检查停牌"
    )
    parser.add_argument(
        "--level",
        choices=["15m", "30m", "60m", "daily"],
        help="指定数据级别"
    )

    args = parser.parse_args()

    runner = AStockRunner()

    if args.mode == "init":
        runner.run_init()
    elif args.mode == "daily":
        runner.run_daily()
    elif args.mode == "fix-missing":
        runner.run_fix_missing()
    elif args.mode == "check-suspended":
        runner.run_check_suspended()


if __name__ == "__main__":
    main()
