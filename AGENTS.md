# AGENTS.md - A股历史数据获取系统

## 项目概述

基于AKShare的A股分钟级及日线级历史行情数据库，支持15m/30m/60m/120m/daily多周期数据获取与存储。

## 目录结构

```
a-stock-fetcher/
├── config/config.yaml          # 配置文件
├── data/                       # 数据目录
├── src/
│   ├── __init__.py
│   ├── config.py               # 配置管理
│   ├── fetcher.py              # 数据获取
│   ├── storage.py              # 数据存储
│   ├── cleaner.py              # 数据清洗
│   ├── aggregator.py           # 数据聚合
│   ├── runner.py               # 主程序
│   ├── health_check.py         # 数据健康检查
│   └── utils.py                # 工具函数
├── main.py                     # 命令行入口
├── requirements.txt            # 依赖
└── README.md
```

## 运行命令

### 安装依赖
```bash
pip3 install -r requirements.txt
```

### 命令行模式
```bash
# 初始化数据库（首次运行，每批100只，间隔5分钟）
python3 main.py init

# 每日增量更新（自动跳过非交易日）
python3 main.py daily

# 补全缺失数据（如漏跑某天）
python3 main.py fix-missing

# 数据健康检查
python3 main.py check-suspended

# 获取15分钟数据
python3 fetch_15m.py

# 修复负数价格
python3 fix_negative.py
```

### 测试
- **本项目无测试框架**，无单元测试或集成测试
- 手动验证方式：运行 `python3 main.py daily` 检查数据获取是否正常

### 代码检查
- 无lint配置（如flake8、pylint）
- 无type check配置（如mypy）
- 建议使用 VSCode/PyCharm 内置检查

## 代码风格指南

### 通用规范
- Python 3.11+
- 使用 f-string 进行字符串格式化
- 使用类型提示（typing 模块）
- 中文注释和文档（项目为中文A股数据系统）

### 命名约定

| 类型 | 命名方式 | 示例 |
|------|----------|------|
| 类名 | PascalCase | `class DataStorage` |
| 函数/变量 | snake_case | `def fetch_stock_list`, `batch_size` |
| 常量 | UPPER_SNAKE_CASE | `MAX_ROWS = 512` |
| 模块名 | snake_case | `fetcher.py`, `storage.py` |

### 文件头部导入顺序
1. 标准库（`sys`, `os`, `datetime`, `pathlib` 等）
2. 第三方库（`pandas`, `akshare`, `pyyaml`）
3. 本地模块（`from .config import`, `from .utils import`）

```python
import sys
import time
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
import akshare as ak

from .config import config
from .utils import logger
```

### 类设计
- 使用单例模式导出实例（小写模块名 = 实例）
- 配置类使用类方法 + 属性

```python
class DataStorage:
    def __init__(self):
        self.root_dir = get_project_root() / config.root_dir

storage = DataStorage()  # 单例实例
```

### 函数设计
- 使用类型提示定义参数和返回值
- 使用 Optional 表示可为 None 的参数
- 简单函数不使用装饰器

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
- 业务异常捕获并记录日志，返回 None
- 严重错误（如配置加载失败）抛出异常
- 避免裸 except，捕获具体异常类型

```python
try:
    df = pd.read_csv(file_path)
except Exception as e:
    logger.error(f"读取数据失败: {e}")
    return None
```

### 日志规范
- 使用统一的 logger 实例（从 utils.py 导入）
- 日志格式：`logger.info()`, `logger.warning()`, `logger.error()`

```python
from .utils import logger

logger.info(f"获取 {len(stock_list)} 只股票")
logger.warning(f"数据为空: {symbol}")
logger.error(f"获取失败: {e}")
```

### 配置管理
- 所有配置从 `config/config.yaml` 读取
- 通过 `src/config.py` 的 Config 类访问
- 使用 @property 暴露配置项

```python
from .config import config

batch_size = config.batch_size
levels = config.levels
```

### 数据处理规范
- 使用 pandas DataFrame 作为主要数据结构
- 列名统一：timestamp, close, volume
- 数据写入前排序、去重

```python
df = df.sort_values("timestamp")
df = df.drop_duplicates(subset=["timestamp"], keep="last")
```

### 代码组织
- 每个模块对应一个功能（fetcher, storage, cleaner, aggregator）
- runner.py 负责主流程编排
- utils.py 放置通用工具函数

### 注意事项
- 8开头（北交所）、9开头（上海B股）不获取
- 只获取：6开头（上交所）、0/3开头（深交所）A股
- 价格使用前复权（qfq）处理
- 请求间隔 0.5 秒避免 API 限流

## 关键依赖

- akshare >= 1.12.0 - 股票数据API
- pandas >= 2.0.0 - 数据处理
- pyyaml >= 6.0 - 配置文件解析
- python-dateutil >= 2.8.0 - 日期处理
