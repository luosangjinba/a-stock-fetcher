import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import tushare as ts

from .config import config
from .storage import storage
from .utils import logger, get_project_root
from .industry_db import industry_db


class IndustryData:
    """行业数据管理类 - 使用stock_basic.industry实现100%覆盖率"""

    def __init__(self):
        self.token = config.tushare_token
        if not self.token:
            raise ValueError("TUSHARE_TOKEN not set. Set via env or ~/.tushare_token")
        
        self.pro = ts.pro_api(self.token)
        self.cache_dir = get_project_root() / "data" / "industry"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.industries_file = self.cache_dir / "industries.json"
        self.members_file = self.cache_dir / "members.json"
        self.last_update_file = self.cache_dir / "last_update.txt"

    def is_cache_valid(self, days: int = 7) -> bool:
        if not self.last_update_file.exists():
            return False
        last_update = datetime.strptime(
            self.last_update_file.read_text().strip(),
            "%Y-%m-%d"
        )
        return (datetime.now() - last_update).days < days

    def save_cache(self, members: Dict) -> None:
        self.members_file.write_text(json.dumps(members, ensure_ascii=False, indent=2))
        
        industries = {}
        for code, ind in members.items():
            ind_name = ind.get('industry', '')
            if ind_name not in industries:
                industries[ind_name] = {'name': ind_name, 'stocks': []}
            industries[ind_name]['stocks'].append(code)
        
        for ind in industries.values():
            ind['stock_count'] = len(ind['stocks'])
        
        self.industries_file.write_text(json.dumps(industries, ensure_ascii=False, indent=2))
        self.last_update_file.write_text(datetime.now().strftime("%Y-%m-%d"))

        logger.info(f"行业缓存已更新: {len(industries)} 行业, {len(members)} 股票")

    def load_cache(self) -> Tuple[Dict, Dict]:
        members = json.loads(self.members_file.read_text())
        industries = json.loads(self.industries_file.read_text())
        return industries, members

    def fetch_industry_data(self, force_update: bool = False) -> Tuple[Dict, Dict]:
        if not force_update and self.is_cache_valid():
            logger.info("使用行业缓存数据")
            return self.load_cache()

        logger.info("从Tushare获取行业数据...")
        try:
            stocks_df = self.pro.stock_basic(exchange='', list_status='L')
            
            members = {}
            for _, row in stocks_df.iterrows():
                ts_code = row['ts_code']
                if ts_code.endswith('.SH'):
                    code = f"SH{ts_code.replace('.SH', '')}"
                elif ts_code.endswith('.SZ'):
                    code = f"SZ{ts_code.replace('.SZ', '')}"
                else:
                    code = ts_code
                members[code] = {'industry': row['industry'], 'name': row['name']}
            
            self.save_cache(members)
            return self.load_cache()

        except Exception as e:
            logger.error(f"获取行业数据失败: {e}")
            if self.members_file.exists():
                logger.warning("使用过期缓存")
                return self.load_cache()
            raise

    def calculate_gain(self, symbol: str, days: int = 20) -> Optional[float]:
        df = storage.read_data(symbol, "daily")
        if df is None or len(df) < 2:
            return None

        df = df.sort_values("timestamp")

        if len(df) < days:
            return None

        latest = df.iloc[-1]["close"]
        old = df.iloc[-days]["close"]

        if old == 0:
            return None

        return (latest - old) / old * 100

    def get_top_gainers(self, days: int = 20, top_n: int = 400) -> List[Tuple[str, float]]:
        logger.info(f"计算近{days}日涨幅...")

        stocks = storage.get_existing_stocks("daily")
        gains = []

        for i, symbol in enumerate(stocks):
            if i % 500 == 0 and i > 0:
                logger.info(f"进度: {i}/{len(stocks)}")

            gain = self.calculate_gain(symbol, days)
            if gain is not None:
                gains.append((symbol, gain))

        gains.sort(key=lambda x: x[1], reverse=True)
        result = gains[:top_n]

        logger.info(f"涨幅计算完成: {len(result)} 只股票")
        return result

    def calculate_industry_strength(
        self,
        lookback_days: int = 20,
        top_stocks: int = 400,
        output_top: int = 12
    ) -> List[Dict]:
        industries, members = self.fetch_industry_data()

        top_gainer_list = self.get_top_gainers(lookback_days, top_stocks)

        industry_counter = Counter()
        for symbol, gain in top_gainer_list:
            if symbol in members:
                ind_name = members[symbol].get('industry')
                if ind_name:
                    industry_counter[ind_name] += 1

        results = []
        for ind_name, count in industry_counter.items():
            total_stocks = industries.get(ind_name, {}).get('stock_count', 0)

            if total_stocks > 0:
                strength = count / total_stocks * 100
                results.append({
                    'industry_name': ind_name,
                    'appear_count': count,
                    'total_stocks': total_stocks,
                    'strength': round(strength, 2)
                })

        results.sort(key=lambda x: x['strength'], reverse=True)

        for i, r in enumerate(results):
            r['rank'] = i + 1

        final_results = results[:output_top]
        
        params = {
            'lookback_days': lookback_days,
            'top_stocks': top_stocks,
            'output_top': output_top
        }
        industry_db.save_results(final_results, params)
        
        return final_results


industry_data = IndustryData()
