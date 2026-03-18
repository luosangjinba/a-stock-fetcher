# AGENTS.md - A股历史数据获取系统

## 项目概述

基于AKShare的A股分钟级及日线级历史行情数据库，支持15m/30m/60m/120m/daily多周期数据获取与存储。

## 目录结构

```
a-stock-fetcher/
├── config/config.yaml    # 配置文件
├── data/                # 数据目录
├── src/
│   ├── config.py        # 配置管理
│   ├── fetcher.py       # 数据获取
│   ├── storage.py       # 数据存储
│   ├── cleaner.py       # 数据清洗
│   ├── aggregator.py    # 数据聚合
│   ├── runner.py        # 主程序
│   ├── health_check.py  # 健康检查+Telegram通知
│   ├── industry.py      # 行业强度计算
│   ├── industry_db.py   # 行业强度数据库
│   └── utils.py         # 工具函数
├── main.py              # 命令行入口
├── deploy.sh            # 一键部署脚本
├── requirements.txt     # 依赖
└── README.md
```

## 运行命令

### 安装
```bash
pip3 install -r requirements.txt
chmod +x deploy.sh && ./deploy.sh
```

### 命令行模式
```bash
python3 main.py init              # 初始化数据库（首次）
python3 main.py daily            # 每日增量更新（含行业强度）
python3 main.py fix-missing     # 补全缺失数据
python3 main.py check-suspended  # 检查停牌股票
python3 main.py health_check     # 健康检查+Telegram报告

# 行业强度相关
python3 main.py industry-strength      # 计算行业强度
python3 main.py industry-query         # 查询行业强度历史
python3 main.py industry-trend         # 查询行业趋势
python3 main.py industry-trend --industry "电气设备"  # 指定行业
```

### 定时任务
```bash
crontab -e
10 17 * * 1-5 cd /path/to/a-stock-fetcher && python3 main.py daily >> logs/cron.log 2>&1
```

## 环境变量

```bash
export TUSHARE_TOKEN="your_token"      # 必需：行业数据
export TELEGRAM_TOKEN="your_bot_token"  # 可选：Telegram通知
export TELEGRAM_CHAT_ID="your_chat_id" # 可选：Telegram通知
```

## Build / Lint / Test

### 验证安装
```bash
python3 -c "import akshare; import pandas; import yaml; import tushare; print('OK')"
```

### 语法检查
```bash
python3 -m py_compile src/*.py
```

### 运行完整流程
```bash
python3 main.py daily
```

### 调试模块
```bash
# 测试行业强度
python3 -c "from src.industry import industry_data; print(industry_data.fetch_industry_data())"

# 测试数据库
python3 -c "from src.industry_db import industry_db; print(industry_db.get_latest())"
```

## 代码风格

### 通用规范
- Python 3.11+
- f-string 字符串格式化
- 类型提示（typing 模块）
- 中文注释和文档

### 命名约定
| 类型 | 方式 | 示例 |
|------|------|------|
| 类名 | PascalCase | `DataStorage` |
| 函数/变量 | snake_case | `fetch_stock_list`, `batch_size` |
| 常量 | UPPER_SNAKE | `MAX_ROWS = 512` |
| 模块 | snake_case | `fetcher.py` |

### 导入顺序
1. 标准库 (`sys`, `os`, `datetime`, `pathlib`)
2. 第三方库 (`pandas`, `akshare`, `pyyaml`, `tushare`)
3. 本地模块 (`from .config import`)

```python
import sys
from datetime import datetime
from typing import List, Optional
import pandas as pd
import akshare as ak

from .config import config
from .utils import logger
```

### 类设计
```python
class DataStorage:
    def __init__(self):
        self.root_dir = get_project_root() / config.root_dir

storage = DataStorage()  # 单例实例
```

### 函数设计
```python
def fetch_hist_data(
    self,
    symbol: str,
    period: str = "15m",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Optional[pd.DataFrame]:
```

### 错误处理
- 业务异常：捕获并记录日志，返回 None
- 严重错误：抛出异常
- 禁止裸 `except`，必须捕获具体异常类型
- 禁止使用 `as any`、`@ts-ignore` 等类型抑制

```python
try:
    df = pd.read_csv(file_path)
except Exception as e:
    logger.error(f"读取失败: {e}")
    return None
```

### 日志规范
```python
from .utils import logger

logger.info(f"获取 {len(stock_list)} 只股票")
logger.warning(f"数据为空: {symbol}")
logger.error(f"获取失败: {e}")
```

### 配置管理
```python
from .config import config

batch_size = config.batch_size
levels = config.levels
```

### 数据处理
- pandas DataFrame 为主要数据结构
- 列名统一：`timestamp`, `close`, `volume`
- 写入前排序、去重

```python
df = df.sort_values("timestamp")
df = df.drop_duplicates(subset=["timestamp"], keep="last")
```

### 注意事项
- 8开头（北交所）、9开头（上海B股）不获取
- 只获取：6开头（上交所）、0/3开头（深交所）A股
- 价格使用前复权（qfq）处理
- 请求间隔 0.5 秒避免 API 限流
- 敏感信息（token）必须从环境变量读取，禁止硬编码
