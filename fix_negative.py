#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent / "data"

def fix_negative_prices():
    count = 0
    for stock_dir in DATA_DIR.iterdir():
        if not stock_dir.is_dir():
            continue
        
        for csv_file in stock_dir.glob("*.csv"):
            df = pd.read_csv(csv_file)
            
            original_len = len(df)
            df = df[df["close"] > 0]
            
            if len(df) < original_len:
                df.to_csv(csv_file, index=False)
                count += 1
                print(f"修复: {stock_dir.name}/{csv_file.name} (删除 {original_len - len(df)} 条)")
    
    print(f"\n共修复 {count} 个文件")

if __name__ == "__main__":
    fix_negative_prices()
