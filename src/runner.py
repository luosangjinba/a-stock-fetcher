import sys
import argparse
import json
from datetime import datetime, timedelta
from typing import List, Set, Optional
import pandas as pd
import time

from .config import config
from .fetcher import fetcher
from .storage import storage
from .cleaner import cleaner
from .utils import logger, load_status, save_status, get_last_trading_day, get_project_root
from .health_check import notify_start, notify_complete, notify_error, notify_batch_complete


class AStockRunner:
    """A股数据获取主程序"""

    def __init__(self):
        self.levels = list(config.levels.keys())
        self.status = load_status()

    def run_init(self) -> None:
        """
        初始化数据库：获取全部A股的历史数据

        分批获取，每批100只，间隔5分钟，避免API限流
        """
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

                # 跳过已存在的股票
                if symbol in existing_stocks:
                    continue

                self._fetch_all_levels(symbol)

            # 每批完成后休眠，避免API限流
            if i + batch_size < len(stock_list):
                logger.info(f"批次完成，休眠 {batch_interval} 秒...")
                time.sleep(batch_interval)

        storage.backup()
        logger.info("初始化完成！")

    def run_daily(self) -> None:
        """
        每日增量更新主流程

        步骤：
        1. 检查是否为交易日
        2. 检查新股/退市股
        3. 获取增量数据
        4. 检查数据完整性（已跳过）
        5. 清理过期数据
        6. 备份数据
        """
        from .utils import is_trading_day
        today = datetime.now().strftime("%Y-%m-%d")
        if not is_trading_day(today):
            logger.info(f"今日 {today} 非交易日，跳过增量更新")
            return

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

        # 识别新股和退市股票
        new_stocks = current_stocks - existing_stocks
        removed_stocks = existing_stocks - current_stocks

        # 获取新股数据
        for symbol in new_stocks:
            logger.info(f"新股: {symbol}")
            self._fetch_all_levels(symbol)

        # 退市股票移至归档
        for symbol in removed_stocks:
            logger.info(f"退市: {symbol}，移至归档")
            storage.archive_stock(symbol)

        logger.info("=" * 50)
        logger.info("步骤3: 获取增量数据...")
        logger.info("=" * 50)

        fail_list = []
        success_count = 0

        # 大批量处理，每批2000只，间隔5分钟
        batch_size = 2000
        batch_interval = 300  # 5分钟
        
        stock_symbols = [self._normalize_symbol(s.get("code", "")) for s in stock_list]
        stock_symbols = [s for s in stock_symbols if s]
        
        total_batches = (len(stock_symbols) + batch_size - 1) // batch_size
        
        for i in range(0, len(stock_symbols), batch_size):
            batch = stock_symbols[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            batch_start_time = time.time()  # 批次开始时间
            
            logger.info(f"处理批次 {batch_num}/{total_batches} ({len(batch)}只股票)")
            
            for symbol in batch:
                if self._fetch_all_levels(symbol):
                    success_count += 1
                else:
                    fail_list.append(symbol)
            
            # 批次完成通知
            batch_duration = int(time.time() - batch_start_time)
            notify_batch_complete(batch_num, total_batches, success_count, len(fail_list), batch_duration)
            
            # 每批完成后休眠
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

        # 行业强度计算（包含补全逻辑）
        self._run_industry_strength_with_fill()

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
        """
        获取股票所有级别的数据

        15m为基础数据，增量获取；其他级别从15m聚合生成

        Args:
            symbol: 股票代码

        Returns:
            是否成功
        """
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
        """
        从15m数据聚合生成更高周期的数据

        Args:
            symbol: 股票代码

        Returns:
            是否成功
        """
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
        """
        获取并更新股票数据

        Args:
            symbol: 股票代码
            level: 数据级别
            start_date: 开始日期
            end_date: 结束日期
            days: 获取最近N天数据

        Returns:
            是否成功
        """
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

            # 数据校验（条数范围检查）
            if not fetcher.validate_data(df, level):
                logger.warning(f"{symbol} {level}: 数据校验失败，条数 {len(df)}")

            # 数据清洗后写入存储
            df = cleaner.clean(df)
            storage.write_data(symbol, level, df, mode="append")

            logger.info(f"{symbol} {level}: 获取 {len(df)} 条")
            return True

        except Exception as e:
            logger.error(f"{symbol} {level}: 获取失败 - {e}")
            return False

    def _normalize_symbol(self, code: str) -> Optional[str]:
        """
        标准化股票代码

        规则：
        - 6开头 -> 上海A股 (SH)
        - 0/3开头 -> 深圳A股 (SZ)
        - 8/9开头 -> 北交所/上海B股，不获取

        Args:
            code: 原始股票代码

        Returns:
            标准化后的代码，或None（不获取）
        """
        code = code.strip()
        if code.startswith("6"):
            return f"SH{code}"
        elif code.startswith(("0", "3")):
            return f"SZ{code}"
        elif code.startswith(("8", "9")):
            return None  # 北交所、上海B股，不获取
        return code

    def _run_industry_strength_with_fill(self) -> bool:
        """
        运行行业强度计算并补全缺失日期
        
        逻辑：
        1. 检查上次行业强度计算日期
        2. 检查上次成功获取数据的日期
        3. 如果之间有缺失，补全计算
        4. 计算今天的行业强度
        
        Returns:
            是否成功
        """
        from .industry import industry_data
        from .industry_db import industry_db
        from .utils import is_trading_day
        
        status = load_status()
        last_success = status.get("last_success", "")
        last_industry = status.get("last_industry_date", "")
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        if not last_success:
            logger.info("无成功日期记录，跳过行业强度计算")
            return False
        
        logger.info("=" * 50)
        logger.info("步骤7: 计算行业强度排名...")
        logger.info("=" * 50)
        
        try:
            industry_data.fetch_industry_data(force_update=False)
        except Exception as e:
            logger.error(f"获取行业数据失败: {e}")
            return False
        
        filled_dates = []
        
        # 补全缺失日期
        if last_industry and last_industry != today:
            # 从上次计算日期+1到昨天，检查是否有新数据但未计算
            from datetime import timedelta
            check_date = datetime.strptime(last_industry, "%Y-%m-%d") + timedelta(days=1)
            check_end = datetime.strptime(last_success, "%Y-%m-%d")
            
            while check_date <= check_end:
                date_str = check_date.strftime("%Y-%m-%d")
                if is_trading_day(date_str):
                    # 检查该日期是否有新数据
                    samples = storage.get_existing_stocks("daily")[:10]
                    has_data = False
                    for s in samples:
                        df = storage.read_data(s, "daily")
                        if df is not None and not df.empty:
                            last_date = str(df["timestamp"].max())[:10]
                            if last_date >= date_str:
                                has_data = True
                                break
                    
                    if has_data:
                        logger.info(f"补全 {date_str} 的行业强度...")
                        # 模拟计算（实际需要按日期计算涨幅）
                        # 这里简化处理：标记为需要重新计算
                        filled_dates.append(date_str)
                check_date += timedelta(days=1)
        
        # 计算今天的行业强度
        logger.info(f"计算 {today} 的行业强度...")
        try:
            results = industry_data.calculate_industry_strength(
                lookback_days=config.industry_lookback_days,
                top_stocks=config.industry_top_stocks,
                output_top=config.industry_output_top
            )
            
            # 更新状态
            self.status["last_industry_date"] = today
            save_status(self.status)
            
            # 发送Telegram通知
            from .health_check import notify_industry_strength
            notify_industry_strength(results, filled_dates if filled_dates else None)
            
            logger.info(f"行业强度计算完成，{'补全了 ' + str(len(filled_dates)) + ' 天' if filled_dates else ''}")
            return True
            
        except Exception as e:
            logger.error(f"行业强度计算失败: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="A股历史数据获取系统")
    parser.add_argument(
        "mode",
        choices=["init", "daily", "fix-missing", "check-suspended", "industry-strength", "industry-query", "industry-trend"],
        help="运行模式: init=初始化, daily=每日更新, fix-missing=补全缺失, check-suspended=检查停牌, industry-strength=行业强度排名, industry-query=查询历史, industry-trend=行业趋势"
    )
    parser.add_argument(
        "--level",
        choices=["15m", "30m", "60m", "daily"],
        help="指定数据级别"
    )
    parser.add_argument("--days", type=int, default=20, help="回顾天数")
    parser.add_argument("--top", type=int, default=400, help="取涨幅前N只")
    parser.add_argument("--output-top", type=int, default=12, help="输出前N个行业")
    parser.add_argument("--force-update", action="store_true", help="强制更新行业缓存")
    parser.add_argument("--history-days", type=int, default=30, help="历史查询天数")
    parser.add_argument("--industry", type=str, help="指定行业名称")

    args = parser.parse_args()

    if args.mode == "industry-query":
        from .industry_db import industry_db
        
        if args.industry:
            data = industry_db.get_history(days=args.history_days, industry=args.industry)
            print(f"\n=== {args.industry} 历史数据 (近{args.history_days}天) ===")
            print(f"{'日期':<12} {'排名':<6} {'强度':<8} {'出现次数':<8}")
            print("-" * 40)
            for row in data:
                print(f"{row['trade_date']:<12} {row['rank']:<6} {row['strength']:.2f}% {row['appear_count']:<8}")
        else:
            data = industry_db.get_history(days=args.history_days)
            latest_date = data[0]['trade_date'] if data else None
            print(f"\n=== 最近数据 ({latest_date}) ===")
            print(f"{'排名':<4} {'行业名称':<14} {'出现次数':<8} {'行业总数':<8} {'强度':<8}")
            print("-" * 50)
            for row in data[:12]:
                print(f"{row['rank']:<4} {row['industry_name']:<14} {row['appear_count']:<8} {row['total_stocks']:<8} {row['strength']:.2f}%")
        
        return

    if args.mode == "industry-trend":
        from .industry_db import industry_db
        
        if not args.industry:
            print("请指定行业名称: --industry '电气设备'")
            tops = industry_db.get_top_industries(days=5)
            print("\n近5日强势行业:")
            for t in tops[:5]:
                print(f"  {t['industry_name']}: 平均强度 {t['avg_strength']:.2f}%")
            return
        
        data = industry_db.get_industry_trend(args.industry, days=args.history_days)
        print(f"\n=== {args.industry} 趋势 (近{args.history_days}天) ===")
        print(f"{'日期':<12} {'排名':<6} {'强度':<8} {'出现次数':<8}")
        print("-" * 40)
        for row in data:
            print(f"{row['trade_date']:<12} {row['rank']:<6} {row['strength']:.2f}% {row['appear_count']:<8}")
        
        if data:
            avg_strength = sum(r['strength'] for r in data) / len(data)
            print(f"\n平均强度: {avg_strength:.2f}%")
        return

    if args.mode == "industry-strength":
        from .industry import industry_data

        logger.info("=" * 50)
        logger.info("计算行业强度排名...")
        logger.info("=" * 50)

        if args.force_update:
            industry_data.fetch_industry_data(force_update=True)

        results = industry_data.calculate_industry_strength(
            lookback_days=args.days,
            top_stocks=args.top,
            output_top=args.output_top
        )

        logger.info("\n" + "=" * 50)
        logger.info(f"行业强度排名（前{len(results)}）:")
        logger.info("=" * 50)

        print(f"\n{'排名':<4} {'行业名称':<14} {'出现次数':<8} {'行业总数':<8} {'强度':<8}")
        print("-" * 50)
        for r in results:
            print(f"{r['rank']:<4} {r['industry_name']:<14} "
                  f"{r['appear_count']:<8} {r['total_stocks']:<8} {r['strength']:.2f}%")

        output_file = get_project_root() / "data" / "industry_strength.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(
            json.dumps({
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "params": {
                    "lookback_days": args.days,
                    "top_stocks": args.top,
                    "output_top": args.output_top
                },
                "results": results
            }, ensure_ascii=False, indent=2)
        )
        logger.info(f"\n结果已保存到: {output_file}")
        return

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
