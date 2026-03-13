# old 脚本执行流程与依赖说明

## 1. 目录内关键文件

- `old/run_old.sh`：日常执行入口（封装了 `validate_old.py` 调用）
- `old/validate_old.py`：核心校验脚本（读规则、拼 SQL、跑 Hive、导出 CSV）
- `old/generate_rules_from_hive.py`：从 Hive Metastore(MySQL) 生成规则文件
- `old/rules.generated.yml`：规则配置（表、where、指标、主键）
- `old/env.yml`：环境配置（Hive 连接、默认输出路径、validation_db）
- `old/requirements.txt`：Python 依赖

## 2. 总体流程

1. （可选）生成规则文件  
   运行 `generate_rules_from_hive.py`，从 Metastore 抽取表结构并输出 `rules.generated.yml`。
2. （可选）初始化校验库  
   运行 `validate_old.py --init-schema --init-only`，创建校验相关表（`old_summary/new_summary/compare_result/runs`）。
3. 日常执行  
   运行 `run_old.sh <run_id> <biz_date> [output_csv]`。
4. 导出结果  
   `validate_old.py` 将计算结果导出为 CSV（默认 `output/old_summary.csv`，可覆盖）。

## 3. 脚本之间依赖关系

## 3.1 依赖链路

`generate_rules_from_hive.py` -> `rules.generated.yml` -> `validate_old.py` <- `run_old.sh`  
`env.yml` -> `validate_old.py`  
`validate_old.py` -> Hive -> `output/*.csv`

## 3.2 输入/输出视角

- `generate_rules_from_hive.py`
  - 输入：Metastore MySQL 元数据
  - 输出：`old/rules.generated.yml`
- `run_old.sh`
  - 输入：`run_id`、`biz_date`、可选输出路径
  - 调用：`old/validate_old.py`
- `validate_old.py`
  - 输入：`--config`（规则）、`--env`（环境）、`--run-id`、`--biz-date`
  - 输出：校验结果 CSV

## 4. 核心参数说明

## 4.1 run_old.sh

- 用法：`bash old/run_old.sh <run_id> <biz_date> [output_csv]`
- 作用：固定传入
  - `--config old/rules.generated.yml`
  - `--env old/env.yml`
  - `--run-id`
  - `--biz-date`
  - `--output`

## 4.2 validate_old.py

- 常用参数
  - `--config`：规则文件（默认 `old/rules.generated.yml`）
  - `--env`：环境文件（默认 `old/env.yml`）
  - `--run-id`：批次 ID
  - `--biz-date`：业务日期（会替换规则中的 `${biz_date}`）
  - `--output`：输出 CSV 路径
  - `--init-schema`：初始化校验表
  - `--init-only`：只初始化，不执行计算
  - `--dry-run`：仅打印 SQL，不执行

## 4.3 generate_rules_from_hive.py

- 常用参数
  - `--database`：必填，Hive 库名
  - `--tables`：可选，指定表名列表
  - `--output`：规则输出路径（默认 `old/rules.example.yml`）
  - `--default-where`：默认过滤条件（默认 `dt='${biz_date}'`）
  - `--partition-cols`：分区列（默认 `dt`）

## 5. 配置依赖

`old/env.yml` 中关键项：

- `validation_db`：初始化 schema 时使用的库
- `clusters.old.host/port/username`：old 集群 Hive 连接信息
- `paths.old_summary`：默认输出文件路径

## 6. 运行示例

## 6.1 生成规则

```bash
python old/generate_rules_from_hive.py \
  --database dwd \
  --output old/rules.generated.yml \
  --default-where "dt='${biz_date}'"
```

## 6.2 初始化校验表

```bash
python old/validate_old.py \
  --env old/env.yml \
  --init-schema \
  --init-only \
  --run-id init_001
```

## 6.3 执行 old 流程

```bash
bash old/run_old.sh full_20260309_01 2026-03-09 output/old_summary_full_20260309_01.csv
```

## 7. 依赖安装

```bash
pip install -r old/requirements.txt
```

依赖包括：

- `PyYAML`
- `PyHive[hive]`
- `PyMySQL`
