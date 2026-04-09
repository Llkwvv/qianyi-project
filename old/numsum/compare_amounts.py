#!/usr/bin/env python3
"""
金额比对与 Hive 校验工具。

整合能力:
1. `query`: 执行 SQL 文件并导出展开后的指标 CSV。
2. `ingest-old`: 将旧集群导出的 CSV 载入目标 Hive 表。
3. `run-new`: 在目标集群执行 SQL 并将结果写入 Hive 表。
4. `run-all`: 依次执行 `ingest-old` 和 `run-new`。
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_CONFIG = "env_config.json"
DEFAULT_SUMMARY_TABLE = "metrics_summary"
DEFAULT_JDBC_URL = "jdbc:hive2://localhost:10000/default"


def load_env_config(config_path: str = DEFAULT_CONFIG) -> Dict[str, Any]:
    """加载环境配置文件。"""
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_cluster_config(config: Dict[str, Any], cluster: str) -> Dict[str, Any]:
    """获取集群配置。"""
    clusters = config.get("clusters", {})
    if cluster not in clusters:
        raise ValueError(f"集群配置中未找到: {cluster}")
    return clusters[cluster]


def get_validation_db(config: Dict[str, Any]) -> str:
    """兼容老配置字段，返回校验库名。"""
    return (
        config.get("validation_db")
        or config.get("insert_mysql", {}).get("db")
        or config.get("import_mysql", {}).get("db")
        or "validation_db"
    )


def replace_placeholder(sql: str, data_dt: str) -> str:
    """替换模板中的 {{data_dt}} 占位符。"""
    return sql.replace("{{data_dt}}", data_dt)


def parse_sql_file(sql_file: str) -> List[str]:
    """读取 SQL 文件，按分号拆分出每条语句。"""
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()
    return [stmt.strip() for stmt in content.split(";") if stmt.strip()]


def run_subprocess(
    cmd: List[str],
    timeout: int = 300,
) -> Tuple[Optional[str], Optional[str], int]:
    """执行本地命令并返回 stdout、stderr、returncode。"""
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        print("命令执行超时")
        return None, None, -1
    except Exception as exc:
        print(f"命令执行错误: {exc}")
        return None, None, -1


def parse_jdbc_output(output: str) -> Tuple[Optional[List[str]], Optional[List[List[str]]]]:
    """解析 beeline JDBC 输出。"""
    if not output:
        return None, None

    lines: List[str] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith((">", "0:", ".", "Connecting", "Connected", "Hive on", "Hadoop job", "Query", "jdbc:")):
            continue
        if "No rows affected" in line or "row selected" in line or "rows selected" in line:
            continue
        if line.startswith(("+", "=")):
            continue
        if line.startswith("|") and all(ch in "| +-=" or ch.isspace() for ch in line):
            continue
        lines.append(line)

    if len(lines) < 2:
        return None, None

    def parse_line(line: str) -> List[str]:
        normalized = line.strip()
        if normalized.startswith("|"):
            normalized = normalized[1:]
        if normalized.endswith("|"):
            normalized = normalized[:-1]
        return [col.strip() for col in normalized.split("|")]

    headers = parse_line(lines[0])
    rows: List[List[str]] = []
    for line in lines[1:]:
        cols = parse_line(line)
        if len(cols) == len(headers):
            rows.append(cols)

    if not rows:
        return None, None
    return headers, rows


def get_jdbc_url(cluster_config: Dict[str, Any]) -> str:
    """从配置中解析 JDBC URL。"""
    jdbc_url = cluster_config.get("jdbc_url") or cluster_config.get("beeline_url")
    if jdbc_url:
        return jdbc_url

    hive_host = cluster_config.get("hive_host") or cluster_config.get("host") or "localhost"
    hive_port = cluster_config.get("port", 10000)
    database = cluster_config.get("database", "default")
    return f"jdbc:hive2://{hive_host}:{hive_port}/{database}"


def execute_sql_via_jdbc(
    sql: str,
    cluster_config: Dict[str, Any],
    fetch_result: bool = True,
    timeout: int = 300,
) -> Tuple[Optional[List[str]], Optional[List[List[str]]]]:
    """统一通过 JDBC 执行 SQL。"""
    jdbc_url = get_jdbc_url(cluster_config)
    username = cluster_config.get("username") or cluster_config.get("user") or ""
    password = cluster_config.get("password") or ""

    cmd = [
        "beeline",
        "-u",
        jdbc_url,
        "--silent=true",
        "--showHeader=false",
        "--outputformat=tsv2",
    ]
    if username:
        cmd.extend(["-n", str(username)])
    if password:
        cmd.extend(["-p", str(password)])

    sql_to_run = sql if not fetch_result else f"set hive.cli.print.header=true;{sql}"
    cmd.extend(["-e", sql_to_run])

    stdout, stderr, returncode = run_subprocess(cmd, timeout=timeout)
    if returncode != 0:
        print(f"执行失败: {stderr}")
        return None, None

    if not fetch_result:
        return [], []

    return parse_jdbc_output(stdout or "")


def expand_metrics(
    headers: List[str],
    data: List[List[str]],
    cluster: str,
) -> Tuple[Optional[List[str]], Optional[List[List[str]]]]:
    """
    动态展开指标列。

    保留基础列: table_name, partition_col, computed_at, data_dt。
    其余列均视为 metric 列，每个 metric 列展开为一行。
    """
    if not headers or not data:
        return None, None

    base_columns = ["table_name", "partition_col", "computed_at", "data_dt"]
    metric_columns = [col for col in headers if col not in base_columns]
    if not metric_columns:
        return None, None

    col_idx = {col: idx for idx, col in enumerate(headers)}
    expanded_rows: List[List[str]] = []

    for row in data:
        for metric_col in metric_columns:
            metric_idx = col_idx.get(metric_col)
            if metric_idx is None:
                continue

            expanded_rows.append(
                [
                    cluster,
                    row[col_idx["table_name"]] if "table_name" in col_idx and col_idx["table_name"] < len(row) else "",
                    row[col_idx["partition_col"]] if "partition_col" in col_idx and col_idx["partition_col"] < len(row) else "",
                    metric_col,
                    row[metric_idx] if metric_idx < len(row) else "",
                    row[col_idx["computed_at"]] if "computed_at" in col_idx and col_idx["computed_at"] < len(row) else "",
                    row[col_idx["data_dt"]] if "data_dt" in col_idx and col_idx["data_dt"] < len(row) else "",
                ]
            )

    if not expanded_rows:
        return None, None

    expanded_headers = ["cluster", "table_name", "partition_col", "metric_name", "value", "computed_at", "data_dt"]
    return expanded_headers, expanded_rows


def export_to_csv(rows: List[List[str]], output_csv: str) -> None:
    """导出数据到 CSV 文件，使用 tab 分隔且不写表头。"""
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerows(rows)

    print(f"结果已导出: {output_csv}")
    print(f"共 {len(rows)} 行")


def execute_hive_insert(
    cluster_config: Dict[str, Any],
    rows: List[List[str]],
    validation_db: str,
    table_name: str,
    overwrite: bool = False,
) -> bool:
    """执行 Hive INSERT 批量写入。"""
    if not rows:
        return True

    values_list: List[str] = []
    for row in rows:
        formatted_values: List[str] = []
        for val in row:
            if val is None or val == "" or str(val).upper() == "NULL":
                formatted_values.append("NULL")
            else:
                escaped_val = str(val).replace("'", "''")
                formatted_values.append(f"'{escaped_val}'")
        values_list.append(f"({','.join(formatted_values)})")

    overwrite_clause = "OVERWRITE" if overwrite else "INTO"
    sql = f"INSERT {overwrite_clause} TABLE {validation_db}.{table_name} VALUES\n" + ",\n".join(values_list) + ";"
    print(f"执行 INSERT: {len(rows)} 行")
    headers, data = execute_sql_via_jdbc(sql, cluster_config, fetch_result=False)
    return headers is not None and data is not None


def execute_hive_load_data(
    cluster_config: Dict[str, Any],
    file_path: str,
    validation_db: str,
    table_name: str,
    overwrite: bool = False,
) -> bool:
    """执行 Hive LOAD DATA 命令。"""
    overwrite_clause = "OVERWRITE" if overwrite else ""
    sql = f"LOAD DATA LOCAL INPATH '{file_path}' {overwrite_clause} INTO TABLE {validation_db}.{table_name};"
    print(f"执行 LOAD DATA: {sql}")
    headers, data = execute_sql_via_jdbc(sql, cluster_config, fetch_result=False)
    return headers is not None and data is not None


def resolve_cluster_config(config: Dict[str, Any], cluster: str) -> Dict[str, Any]:
    """缺失集群配置时给出更清晰的错误。"""
    try:
        return get_cluster_config(config, cluster)
    except ValueError as exc:
        raise ValueError(f"{exc}。当前可用集群: {', '.join(config.get('clusters', {}).keys()) or '无'}") from exc


def collect_query_results(
    sql_file: str,
    data_dt: str,
    cluster_name: str,
    cluster_config: Dict[str, Any],
) -> Tuple[Optional[List[str]], List[List[str]]]:
    """执行 SQL 文件中的多条语句，并返回展开后的结果。"""
    statements = parse_sql_file(sql_file)
    print(f"读取 SQL 文件: {sql_file}")
    print(f"共 {len(statements)} 条语句")

    all_rows: List[List[str]] = []
    final_headers: Optional[List[str]] = None

    for idx, stmt in enumerate(statements, start=1):
        print(f"\n执行第 {idx} 条语句...")
        actual_sql = replace_placeholder(stmt, data_dt)
        print(f"SQL: {actual_sql}")

        headers, rows = execute_sql_via_jdbc(actual_sql, cluster_config, fetch_result=True)
        if not headers or not rows:
            print(f"第 {idx} 条语句返回空结果")
            continue

        print(f"返回 {len(rows)} 行，列: {headers}")
        expanded_headers, expanded_rows = expand_metrics(headers, rows, cluster_name)
        if not expanded_headers or not expanded_rows:
            print(f"第 {idx} 条语句没有可展开指标")
            continue

        final_headers = expanded_headers
        all_rows.extend(expanded_rows)
        print(f"展开为 {len(expanded_rows)} 行")

    return final_headers, all_rows


def cmd_query(args: argparse.Namespace) -> int:
    """执行 SQL 并导出 CSV。"""
    if not os.path.exists(args.sql_file):
        print(f"SQL 文件不存在: {args.sql_file}")
        return 1

    if not args.output_csv:
        today = datetime.now().strftime("%Y%m%d")
        args.output_csv = f"/data/transfer_agent/data/upload/{today}/old_summary.csv"
        print(f"使用默认输出路径: {args.output_csv}")

    config = load_env_config(args.config)
    cluster_config = resolve_cluster_config(config, args.cluster)

    final_headers, final_rows = collect_query_results(args.sql_file, args.data_dt, args.cluster, cluster_config)
    if not final_headers or not final_rows:
        print("没有结果可导出")
        return 1

    export_to_csv(final_rows, args.output_csv)
    return 0


def cmd_ingest_old(args: argparse.Namespace) -> int:
    """将旧集群导出的 CSV 载入目标 Hive 表。"""
    config = load_env_config(args.config)
    cluster_config = resolve_cluster_config(config, args.cluster)
    validation_db = get_validation_db(config)
    csv_path = args.csv or config.get("paths", {}).get("old_summary") or os.path.join("output", "old_summary.csv")

    if not os.path.exists(csv_path):
        print(f"CSV 文件不存在: {csv_path}")
        return 1

    success = execute_hive_load_data(
        cluster_config,
        csv_path,
        validation_db,
        DEFAULT_SUMMARY_TABLE,
        overwrite=args.overwrite,
    )

    if not success:
        print("LOAD DATA 执行失败")
        return 1

    print("CSV 数据已成功载入 Hive 表")
    return 0


def cmd_run_new(args: argparse.Namespace) -> int:
    """在目标集群执行 SQL，并写入 Hive 汇总表。"""
    config = load_env_config(args.config)
    cluster_config = resolve_cluster_config(config, args.cluster)
    validation_db = get_validation_db(config)

    if not args.sql_file or not os.path.exists(args.sql_file):
        print(f"SQL 文件不存在: {args.sql_file}")
        return 1
    if not args.data_dt:
        print("请通过 --data-dt 指定分区日期")
        return 1

    final_headers, final_rows = collect_query_results(args.sql_file, args.data_dt, "new", cluster_config)
    if not final_headers or not final_rows:
        print("没有结果可写入")
        return 1

    if not execute_hive_insert(
        cluster_config,
        final_rows,
        validation_db,
        DEFAULT_SUMMARY_TABLE,
        overwrite=args.overwrite,
    ):
        print("INSERT 执行失败")
        return 1

    if args.output_csv:
        export_to_csv(final_rows, args.output_csv)

    print(f"完成! 共处理 {len(final_rows)} 行")
    return 0


def cmd_run_all(args: argparse.Namespace) -> int:
    """依次执行 ingest-old 和 run-new。"""
    ingest_args = argparse.Namespace(
        config=args.config,
        csv=args.csv,
        cluster=args.cluster,
        overwrite=args.overwrite,
    )
    run_new_args = argparse.Namespace(
        config=args.config,
        sql_file=args.sql_file,
        data_dt=args.data_dt,
        cluster=args.cluster,
        overwrite=args.overwrite,
        output_csv=args.output_csv,
    )

    print("========== 步骤 1: ingest-old ==========")
    ingest_code = cmd_ingest_old(ingest_args)
    if ingest_code != 0:
        return ingest_code

    print("\n========== 步骤 2: run-new ==========")
    return cmd_run_new(run_new_args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="金额比对与 Hive 校验工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s query --sql-file sql/metrics_queries.sql --data-dt 2024-01-01 --cluster old
  %(prog)s ingest-old --csv output/old_summary.csv --cluster old
  %(prog)s run-new --sql-file sql/metrics_queries.sql --data-dt 2024-01-01 --cluster old
  %(prog)s run-all --csv output/old_summary.csv --sql-file sql/metrics_queries.sql --data-dt 2024-01-01 --cluster old
        """,
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="配置文件路径")

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    parser_query = subparsers.add_parser("query", help="执行 SQL 文件并导出展开后的指标 CSV")
    parser_query.add_argument("--sql-file", required=True, help="SQL 文件路径")
    parser_query.add_argument("--data-dt", required=True, help="分区日期，替换模板中的 {{data_dt}}")
    parser_query.add_argument("--output-csv", help="输出 CSV 文件路径")
    parser_query.add_argument("--cluster", default="old", help="集群名称")
    parser_query.set_defaults(func=cmd_query)

    parser_ingest = subparsers.add_parser("ingest-old", help="将旧集群导出的 CSV 载入目标 Hive 表")
    parser_ingest.add_argument("--csv", help="旧集群导出的 CSV 文件路径")
    parser_ingest.add_argument("--cluster", default="old", help="目标集群名称")
    parser_ingest.add_argument("--overwrite", action="store_true", help="覆盖已有数据")
    parser_ingest.set_defaults(func=cmd_ingest_old)

    parser_run = subparsers.add_parser("run-new", help="在目标集群执行 SQL 并将结果写入 Hive 表")
    parser_run.add_argument("--sql-file", required=True, help="SQL 文件路径")
    parser_run.add_argument("--data-dt", required=True, help="分区日期")
    parser_run.add_argument("--cluster", default="old", help="集群名称")
    parser_run.add_argument("--overwrite", action="store_true", help="覆盖已有数据")
    parser_run.add_argument("--output-csv", help="可选，额外导出结果 CSV")
    parser_run.set_defaults(func=cmd_run_new)

    parser_all = subparsers.add_parser("run-all", help="依次执行 ingest-old 和 run-new")
    parser_all.add_argument("--csv", help="旧集群导出的 CSV 文件路径")
    parser_all.add_argument("--sql-file", required=True, help="SQL 文件路径")
    parser_all.add_argument("--data-dt", required=True, help="分区日期")
    parser_all.add_argument("--cluster", default="old", help="集群名称")
    parser_all.add_argument("--overwrite", action="store_true", help="覆盖已有数据")
    parser_all.add_argument("--output-csv", help="可选，额外导出 run-new 结果 CSV")
    parser_all.set_defaults(func=cmd_run_all)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except ValueError as exc:
        print(exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
