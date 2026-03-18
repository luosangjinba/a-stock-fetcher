import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from .utils import logger, get_project_root


class IndustryDB:
    """行业强度历史数据库"""

    def __init__(self):
        self.db_path = get_project_root() / "data" / "industry_strength.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS industry_strength (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    industry_name TEXT NOT NULL,
                    appear_count INTEGER NOT NULL,
                    total_stocks INTEGER NOT NULL,
                    strength REAL NOT NULL,
                    lookback_days INTEGER DEFAULT 20,
                    top_stocks INTEGER DEFAULT 400,
                    created_at TEXT NOT NULL,
                    UNIQUE(trade_date, rank, industry_name)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_trade_date 
                ON industry_strength(trade_date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_industry_name 
                ON industry_strength(industry_name)
            """)
            logger.info(f"数据库初始化: {self.db_path}")

    def save_results(self, results: List[Dict], params: Dict) -> int:
        trade_date = datetime.now().strftime("%Y-%m-%d")
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        saved_count = 0
        with sqlite3.connect(self.db_path) as conn:
            for r in results:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO industry_strength 
                        (trade_date, rank, industry_name, appear_count, total_stocks, 
                         strength, lookback_days, top_stocks, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        trade_date,
                        r['rank'],
                        r['industry_name'],
                        r['appear_count'],
                        r['total_stocks'],
                        r['strength'],
                        params.get('lookback_days', 20),
                        params.get('top_stocks', 400),
                        created_at
                    ))
                    saved_count += 1
                except Exception as e:
                    logger.warning(f"保存失败: {e}")
            
            conn.commit()
        
        logger.info(f"保存 {saved_count} 条记录到数据库")
        return saved_count

    def get_history(self, days: int = 30, industry: Optional[str] = None) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            if industry:
                rows = conn.execute("""
                    SELECT * FROM industry_strength 
                    WHERE trade_date >= date('now', '-' || ? || ' days')
                    AND industry_name = ?
                    ORDER BY trade_date DESC, rank ASC
                """, (days, industry)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM industry_strength 
                    WHERE trade_date >= date('now', '-' || ? || ' days')
                    ORDER BY trade_date DESC, rank ASC
                """, (days,)).fetchall()
            
            return [dict(row) for row in rows]

    def get_latest(self, date: Optional[str] = None) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            if date:
                rows = conn.execute("""
                    SELECT * FROM industry_strength 
                    WHERE trade_date = ?
                    ORDER BY rank ASC
                """, (date,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM industry_strength 
                    WHERE trade_date = (SELECT MAX(trade_date) FROM industry_strength)
                    ORDER BY rank ASC
                """).fetchall()
            
            return [dict(row) for row in rows]

    def get_industry_trend(self, industry: str, days: int = 30) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            rows = conn.execute("""
                SELECT trade_date, rank, strength, appear_count
                FROM industry_strength 
                WHERE industry_name = ?
                AND trade_date >= date('now', '-' || ? || ' days')
                ORDER BY trade_date ASC
            """, (industry, days)).fetchall()
            
            return [dict(row) for row in rows]

    def get_top_industries(self, days: int = 5) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            rows = conn.execute("""
                SELECT industry_name, 
                       AVG(strength) as avg_strength,
                       AVG(appear_count) as avg_appear_count,
                       COUNT(*) as days_count
                FROM industry_strength 
                WHERE trade_date >= date('now', '-' || ? || ' days')
                AND rank <= 5
                GROUP BY industry_name
                ORDER BY avg_strength DESC
                LIMIT 10
            """, (days,)).fetchall()
            
            return [dict(row) for row in rows]


industry_db = IndustryDB()
