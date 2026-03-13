---
name: HiveSpark迁移校验闭环（Phase版）
overview: 为旧/新两套 Hive/Spark 集群提供可复用的数据迁移校验流程：旧集群离线计算校验摘要并导出文件，新集群导入旧摘要、计算新摘要、逐项对比并将结果落到新集群独立校验库，CLI 一键触发、可重跑可追溯。
todos:
  - id: schema-design
    content: Phase1：在新集群创建校验 schema（validation_db）与 runs/old_summary/new_summary/compare_result 四张表，并确定分区/字段口径。
    status: completed
  - id: yaml-spec
    content: Phase1：确定 YAML 规则规范与一份样例（含 where、keys、metrics、可选分区维度）。
    status: completed
  - id: old-cli
    content: Phase2：实现旧集群 CLI：按 YAML 在 Hive 计算摘要并导出 Parquet/CSV（先跑通 1 表）。
    status: completed
  - id: new-cli-import
    content: Phase3：实现新集群导入：读取旧摘要文件并写入 validation_db.old_summary（支持幂等重跑）。
    status: completed
  - id: new-cli-compute
    content: Phase3：实现新集群计算：按 YAML 在 Hive 计算摘要写入 validation_db.new_summary（支持幂等重跑）。
    status: completed
  - id: compare-and-report
    content: Phase3：实现新集群对比与落库：生成 compare_result，并可选输出 HTML/Markdown 报告（支持幂等重跑）。
    status: completed
  - id: perf-hardening
    content: Phase4：测试联调与性能加固：分区级计算、并行度/重试策略、压测与上线运行手册。
    status: completed
isProject: false
---

## Phase1 基础环境搭建 + 规则/表结构定稿

目标：在新集群落地独立校验库，确定统一口径的规则规范与表结构，为后续旧/新两侧复用同一套规则生成逻辑打基础。

交付：

- 校验 schema 与表结构（runs/old_summary/new_summary/compare_result）。
- 一份可执行的规则样例（覆盖至少 2-3 张表更佳，含 keys 与金额类 sum 指标）。
- CLI 运行约定（参数、run_id、输出路径与命名）。

验收：

- 校验库表可创建、可按 run_id 分区写入/查询。
- 规则文件可解析并生成可运行的 Hive SQL（可先只验证编译与语法）。

## Phase2 旧集群数据查询导出（先跑通 1 表）

目标：在旧集群完成“只读计算摘要 + 导出摘要文件”的最小闭环，先用 1 张表跑通口径与性能基线。

交付：

- 旧集群 CLI：读 rules.yml，逐表生成 Hive SQL，执行 Hive 查询，产出摘要结果并导出 CSV/Parquet。
- 旧集群摘要文件：`old_summary_<run_id>.csv` 或 `old_summary_<run_id>.parquet`。

验收：

- 1 张表跑通，摘要文件包含 run_id、table_name、check_type、metric_name、value、computed_at、where_hash 等字段。
- 若配置 keys，可产出主键重复数，口径稳定。

## Phase3 新集群：旧导入 + 新计算 + 对比落库（含幂等/可重跑）

目标：新集群侧完成“导入旧摘要 → 计算新摘要 → 对比 → 落库”的全链路，并支持同一 run_id 幂等重跑。

交付：

- 新集群 CLI 子命令：
- import：导入旧 Parquet/CSV 到 validation_db.old_summary。
- compute：按 YAML 计算新摘要写入 validation_db.new_summary。
- compare：对比写入 validation_db.compare_result（更新 validation_db.runs 状态）。
- report（可选）：输出 HTML/Markdown 报告。
- 幂等策略说明：同一 run_id 重跑时的覆盖策略（分区重写、先删后写或去重写入）。

验收：

- 同一 run_id 连续执行 import/compute/compare 两次，结果表无重复与脏数据。
- 缺失/不相等/空值等异常可写入 compare_result，且 run 状态可追踪。

## Phase4 测试联调（冒烟→全量/压测→上线运行手册）

目标：先冒烟跑通，再推进到全量与压测，最终形成可交接的运行手册与故障处置流程。

交付：

- 冒烟用例（1 张表）run 记录、旧/新摘要、对比结果与报告。
- 全量/压测报告：耗时、资源使用、瓶颈点与参数建议。
- 上线运行手册：run_id 规则、旧摘要准备、导入/计算/对比流程、通过/失败判定、重跑与回滚说明。

验收：

- 冒烟：全链路 PASS/FAIL 可解释，结果落库完整。
- 全量：在可接受窗口内完成（按 SLO），失败可定位到表/分区/指标。
- 手册：非开发同学可按步骤复现执行与查看结果。
