#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.fetcher import fetcher
from src.storage import storage
from src.cleaner import cleaner
from src.utils import logger
import time

def fetch_15m_all():
    stock_list = fetcher.fetch_stock_list()
    print(f"总股票数: {len(stock_list)}")
    
    success = 0
    fail = 0
    
    for i, stock in enumerate(stock_list):
        symbol = stock.get("code", "")
        if not symbol:
            continue
        
        if symbol.startswith("6"):
            symbol = f"SH{symbol}"
        elif symbol.startswith(("0", "3")):
            symbol = f"SZ{symbol}"
        else:
            continue
        
        # 检查是否已有15m数据
        if os.path.exists(f"data/{symbol}/15m.csv"):
            continue
        
        try:
            df = fetcher.fetch_hist_data(symbol, "15m", days=32)
            if df is not None and not df.empty:
                df = cleaner.clean(df)
                storage.write_data(symbol, "15m", df, mode="write")
                success += 1
                print(f"[{i+1}/{len(stock_list)}] {symbol}: {len(df)} 条")
            else:
                fail += 1
                print(f"[{i+1}/{len(stock_list)}] {symbol}: 无数据")
        except Exception as e:
            fail += 1
            print(f"[{i+1}/{len(stock_list)}] {symbol}: 错误 - {e}")
        
        time.sleep(1)
    
    print(f"\n完成! 成功: {success}, 失败: {fail}")

if __name__ == "__main__":
    fetch_15m_all()
