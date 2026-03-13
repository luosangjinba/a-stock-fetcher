# A股历史数据获取系统

基于AKShare的A股分钟级及日线级历史行情数据库。

## 功能特性

- **多周期数据**：支持15m/30m/60m/120m/daily
- **增量获取**：每日只获取当日新数据（约16条15m）
- **交易日检查**：自动跳过周末及假期，避免无效API调用
- **分批处理**：每日增量分批执行，避免API限流
- **自动聚合**：30m/60m/120m/daily通过15m自动聚合计算
- **数据清洗**：去重、排序，空值填充、过滤负价格
- **滚动清理**：自动剔除过期数据
- **自动备份**：每次运行后自动备份
- **定时调度**：支持每日自动执行
- **Telegram通知**：增量更新开始/完成/异常通知

## 数据存储

```
data/
├── SH600000/
│   ├── 15m.csv      # 15分钟数据（保留512行）
│   ├── 30m.csv      # 30分钟数据（保留512行）
│   ├── 60m.csv      # 60分钟数据（保留512行）
│   ├── 120m.csv     # 120分钟数据（保留512行）
│   └── daily.csv    # 日线数据（永久保留）
├── SH600036/
│   └── ...
└── ...
```

## 目录结构

```
a-stock-fetcher/
├── config/
│   └── config.yaml          # 配置文件
├── data/                    # 数据目录
├── archive/                 # 退市股票归档
├── backup/                  # 备份目录
├── logs/                    # 日志目录
├── src/
│   ├── __init__.py
│   ├── config.py            # 配置管理
│   ├── fetcher.py           # 数据获取
│   ├── storage.py           # 数据存储
│   ├── cleaner.py           # 数据清洗
│   ├── aggregator.py        # 数据聚合
│   ├── runner.py            # 主程序
│   ├── health_check.py      # 数据健康检查
│   └── utils.py            # 工具函数
├── main.py                  # 命令行入口
├── requirements.txt        # 依赖
└── README.md
```

## 安装

```bash
pip3 install -r requirements.txt
```

## 使用方法

### 命令行模式

```bash
# 初始化数据库（首次运行，每批100只，间隔5分钟）
python3 main.py init

# 每日增量更新（自动跳过非交易日）
python3 main.py daily

# 补全缺失数据（如漏跑某天）
python3 main.py fix-missing

# 数据健康检查（检查完整性并发送Telegram报告）
python3 main.py health_check
```

### Telegram通知

每日增量更新会发送通知：
- 🚀 开始更新
- ✅ 更新完成（包含耗时、成功/失败数、新股/退市数）
- ❌ 异常告警

### 定时任务（cron）

使用 cron 设置周一到周五每日17:10自动执行：

```bash
# 编辑crontab
crontab -e

# 添加以下行（每日17:10执行）
10 17 * * 1-5 cd /home/leo/myworkspace/a-stock-fetcher && /usr/bin/python3 main.py daily >> /home/leo/myworkspace/a-stock-fetcher/logs/cron.log 2>&1
```

查看定时任务：
```bash
crontab -l
```

### 数据获取脚本

```bash
# 获取15分钟数据
python3 fetch_15m.py

# 修复负数价格
python3 fix_negative.py
```

## 配置说明

修改 `config/config.yaml`：

```yaml
data:
  root_dir: "data"           # 数据目录
  archive_dir: "archive"     # 归档目录
  backup_dir: "backup"      # 备份目录
  
  levels:
    15m:
      rows: 512              # 保留512行
      filename: "15m.csv"
    30m:
      rows: 512
      filename: "30m.csv"
    60m:
      rows: 512
      filename: "60m.csv"
    120m:
      rows: 512
      filename: "120m.csv"
    daily:
      rows: 99999            # 永久保留
      filename: "daily.csv"

fetcher:
  run_time: "17:10"         # 每日运行时间
  init:
    batch_size: 100          # 初始化每批数量
    batch_interval: 300     # 初始化批次间隔(秒)
  daily:
    batch_size: 2000         # 每日增量每批数量
    batch_interval: 300    # 每日增量批次间隔(秒)
    retry_times: 2           # 重试次数
    request_interval: 0.5  # 请求间隔(秒)
```

## 数据级别说明

| 级别 | 保留策略 | 数据来源 | 说明 |
|------|----------|----------|------|
| 15m | 512行 | API增量获取 | 每日约16条，新股获取全部历史 |
| 30m | 512行 | 15m聚合 | 从15m计算 |
| 60m | 512行 | 15m聚合 | 从15m计算 |
| 120m | 512行 | 15m聚合 | 从15m计算 |
| daily | 永久保留 | 15m聚合 | 从15m计算 |

**增量更新逻辑**：
- 现有股票：只获取当日数据（约16条15m）
- 新股：获取全部可用历史数据
- 30m/60m/120m/daily：从15m实时聚合

**注意**：
- 30m/60m/120m/daily 通过15m自动聚合计算
- 8开头（北交所）、9开头（上海B股）不获取
- 只获取：6开头（上交所）、0/3开头（深交所）A股
- 价格已做前复权（qfq）处理

## API调用

```python
from src.fetcher import fetcher
from src.storage import storage

# 获取股票列表
stocks = fetcher.fetch_stock_list()

# 获取日线数据
df = fetcher.fetch_hist_data('SH600000', 'daily')

# 获取15分钟数据
df = fetcher.fetch_hist_data('SH600000', '15m')

# 读取本地数据
df = storage.read_data('SH600000', 'daily')

# 获取聚合数据（自动计算30m/60m/120m）
df = storage.get_data('SH600000', '30m')
```

## 监控

```bash
# 查看数据统计
ls data/ | wc -l

# 查看日志
tail -f logs/fetcher.log

# 数据健康检查
python3 -m src.health_check
```

## 常见问题

1. **分钟数据获取失败**
   - 检查网络连接
   - AKShare接口可能暂时不可用

2. **数据出现负价格**
   - 运行 `python3 fix_negative.py` 修复

3. **备份文件过大**
   - 可手动删除旧备份：`rm backup/backup_*.tar.gz`

## 容错机制

### 数据保留策略

| 级别 | 保留行数 | 交易日 | 日历天 |
|------|----------|--------|--------|
| 15m | 512 | ~32天 | ~45天 |
| 30m/60m/120m | 512 | ~32天 | ~45天 |
| daily | 永久 | - | - |

### AKShare 限制

- 15m 数据每次请求最多返回 **~512行**（约32个交易日）
- 超过32个交易日的历史数据**无法获取**

### 最大停止天数

**理论上限：~30个交易日（约1个月）**

- 停止 ≤30天：可通过 `python3 main.py fix-missing` 补全
- 停止 >30天：丢失30天前的分钟级数据，只能接受数据断层

### 宕机告警

- 每次运行前自动检查 `last_success` 日期
- 超过 **3天** 未成功运行 → 发送 Telegram 告警
- 告警阈值可在 `src/health_check.py` 中修改 `OFFLINE_THRESHOLD_DAYS`

### 数据恢复

```bash
# 补全缺失数据（如漏跑某天）
python3 main.py fix-missing
```

## 依赖

- akshare >= 1.12.0
- pandas >= 2.0.0
- pyyaml >= 6.0
- python-dateutil >= 2.8.0
