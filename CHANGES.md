# 变更记录

## 2026-03-19

### 行业强度分析

- 新增 `src/industry.py` 模块：行业强度计算
- 新增 `src/industry_db.py` 模块：SQLite历史数据库
- 算法：获取近20日涨幅前400只 → 匹配行业 → 统计出现次数/行业总数 = 强度
- 数据源：Tushare stock_basic.industry（覆盖率100%）
- 自动补全：检测缺失日期，自动补全历史数据

### 命令行新增

- `python3 main.py industry-strength` - 计算行业强度
- `python3 main.py industry-query` - 查询历史
- `python3 main.py industry-trend` - 查询行业趋势

### 集成到每日流程

- 每日增量完成后自动计算行业强度
- 自动保存到SQLite数据库
- 自动推送到Telegram

### 通知增强

- 新增 `notify_industry_strength()` 函数
- 行业强度计算完成后推送排名到Telegram

## 2026-03-16

### 批次完成通知
- 新增 `notify_batch_complete()` 函数
- 每批次处理完成后推送Telegram通知
- 包含：批次号、耗时、成功数、失败数

### 一键部署脚本
- 新增 `deploy.sh` 交互式部署脚本
- 自动创建目录结构
- 自动安装依赖
- 自动配置Telegram（可选）
- 自动设置cron定时任务（可选）

### 代码优化
- 修复 `storage.py` 重复代码
- `health_check.py` 补充函数文档注释
- README 添加每日增量更新流程说明

## 2026-03-14

### 交易日检查
- 每日增量更新启动时自动检查今日是否为交易日
- 非交易日（周末、假期）直接跳过，不执行API调用
- 使用AKShare的真实A股交易日历

## 2026-03-12 ~ 2026-03-13

### 1. Bug修复
- 初始化检查改为以15m.csv是否存在判断（而非目录）

### 2. 增量获取优化
- 15m改为增量获取（最后日期->当日，约16条）
- 新股获取全部可用历史数据
- 移除daily API获取，改为从15m聚合生成
- 30m/60m/120m/daily均从15m聚合计算

### 3. Telegram通知
- 增量更新开始通知：`🚀 A股数据增量更新已开始`
- 增量更新完成通知：耗时、成功数、失败数、新股数、退市数
- 异常告警通知：大量股票获取失败时

### 4. 分批处理
- 每批2000只股票
- 每批间隔5分钟

### 5. 数据保留策略
- 15m/30m/60m/120m：保留512行
- daily：永久保留（不做截断）
- 先获取数据，再聚合，再截断

### 6. Cron时间
- 从17:30改为17:00（A股16:30数据稳定）

### 7. 股票过滤规则
- 6开头：上交所（SH）
- 0、3开头：深交所（SZ）
- 8开头：北交所（不获取）
- 9开头：上海B股（不获取）

### 8. 复权类型
- qfq（前复权）

## 配置变更

### config/config.yaml
```yaml
data:
  levels:
    15m:
      days: 32
    30m:
      days: 32
    60m:
      days: 32
    120m:
      days: 32
    daily:
      days: 99999  # 永久保留

fetcher:
  run_time: "17:00"
  init:
    batch_size: 100
    batch_interval: 300
  daily:
    batch_size: 2000
    batch_interval: 300
    retry_times: 2
    request_interval: 0.5
```

## 每日流程

1. 步骤1: 检查是否交易日 → 非交易日直接跳过
2. 步骤2: 开始增量更新 → Telegram通知开始
3. 步骤3: 检查新股/退市股
4. 步骤4: 分批获取增量数据（2000只/批，间隔5分钟）
   - 15m增量获取
   - 30m/60m/120m/daily从15m聚合
4. 步骤4: 检查完整性并补全
5. 步骤5: 数据降采样（分钟级保留512行，daily永久）
6. 步骤6: 备份数据
7. Telegram通知完成

## 新增文件
- src/health_check.py：数据健康检查脚本
- CHANGES.md：本变更记录

## 预计耗时
- 5191只股票
- 每只0.5秒
- 约1.5-2小时完成
