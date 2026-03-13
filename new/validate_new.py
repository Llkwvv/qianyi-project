"""
New cluster validation CLI (PyHive).

Subcommands:
  import  - import old summary CSV into validation_db.old_summary
  compute - compute new summary into validation_db.new_summary
  compare - compare old/new and write validation_db.compare_result
  report  - generate markdown report from compare_result
"""

import argparse
import csv
import hashlib
import os
import sys
import time
from typing import Dict, List, Optional

import yaml


def escape_sql_string(text: str) -> str:
    return text.replace("'", "''")


def generate_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def file_md5(path: str) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def load_yaml(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_env(path: str) -> Dict:
    data = load_yaml(path)
    return data or {}


def get_cluster_env(env: Dict, name: Optional[str] = None) -> Dict:
    clusters = env.get("clusters") or {}
    if name and name in clusters:
        return clusters[name] or {}
    return env.get("hive") or {}


def get_validation_db(env: Dict) -> str:
    return env.get("validation_db", "validation_db")


def build_summary_sql(
    config: dict,
    run_id: str,
    biz_date: str = None,
    include_run_id: bool = True,
) -> str:
    tables = config.get("tables", [])
    default_where = config.get("global", {}).get("default_where", "1=1")

    parts: List[str] = []
    for t in tables:
        if not t.get("enabled", True):
            continue

        table = t["table"]
        where = t.get("where", default_where)
        partition_cols = t.get("partition_cols", [])
        if partition_cols:
            if biz_date:
                where = where.replace("${biz_date}", biz_date)
        else:
            if "${biz_date}" in where or where == default_where:
                where = "1=1"
        keys = t.get("keys", [])
        metrics = t.get("metrics", [])

        where_hash = generate_hash(where)
        where_escaped = escape_sql_string(where)
        partition_spec_expr = _build_partition_spec_expr(partition_cols)
        group_by_sql = _build_group_by_sql(partition_cols)

        for m in metrics:
            name, expr = m["name"], m["expr"]
            expr_escaped = escape_sql_string(expr)
            select_cols = [
                f"'{table}' as table_name",
                "'metric' as check_type",
                f"'{name}' as metric_name",
                f"'{expr_escaped}' as metric_expr",
                f"cast({expr} as string) as value",
                f"'{where_escaped}' as where_clause",
                f"'{where_hash}' as where_hash",
                f"{partition_spec_expr} as partition_spec",
                "current_timestamp() as computed_at",
            ]
            if include_run_id:
                select_cols.insert(0, f"'{run_id}' as run_id")
            parts.append(
                f"SELECT {', '.join(select_cols)} FROM {table} WHERE {where}{group_by_sql}"
            )

        if keys:
            keys_expr = ", ".join(keys)
            select_cols = [
                f"'{table}' as table_name",
                "'pk_dup' as check_type",
                "'pk_dup_count' as metric_name",
                f"'count(*) - count(distinct {keys_expr})' as metric_expr",
                f"cast(count(*) - count(distinct {keys_expr}) as string) as value",
                f"'{where_escaped}' as where_clause",
                f"'{where_hash}' as where_hash",
                f"{partition_spec_expr} as partition_spec",
                "current_timestamp() as computed_at",
            ]
            if include_run_id:
                select_cols.insert(0, f"'{run_id}' as run_id")
            parts.append(
                f"SELECT {', '.join(select_cols)} FROM {table} WHERE {where}{group_by_sql}"
            )

    return "\nUNION ALL\n".join(parts)


def sql_literal(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return f"'{escape_sql_string(value)}'"


def _build_partition_spec_expr(partition_cols: List[str]) -> str:
    if not partition_cols:
        return "''"
    parts = [f"concat('{col}=', cast({col} as string))" for col in partition_cols]
    return f"concat_ws('/', {', '.join(parts)})"


def _build_group_by_sql(partition_cols: List[str]) -> str:
    if not partition_cols:
        return ""
    return f" GROUP BY {', '.join(partition_cols)}"


def connect_hive(host: str, port: int, username: str):
    try:
        from pyhive import hive
    except ImportError:
        print("Please install pyhive first: pip install pyhive[hive]")
        sys.exit(1)
    return hive.connect(host=host, port=port, username=username)


def apply_hive_settings(cursor, settings) -> None:
    for setting in settings:
        cursor.execute(f"set {setting}")


def execute_sql_with_retry(
    host: str,
    port: int,
    username: str,
    hive_settings,
    sql: str,
    retries: int,
    retry_wait: int,
) -> None:
    attempt = 0
    while True:
        try:
            conn = connect_hive(host, port, username)
            cursor = conn.cursor()
            apply_hive_settings(cursor, hive_settings)
            cursor.execute(sql)
            cursor.close()
            conn.close()
            return
        except Exception as exc:
            attempt += 1
            print(f"[ERROR] Hive execution failed: {exc}")
            if attempt > retries:
                raise
            print(f"[RETRY] attempt {attempt}/{retries} in {retry_wait}s")
            time.sleep(retry_wait)


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def build_import_sql(
    db: str,
    table: str,
    run_id: str,
    rows: List[Dict[str, str]],
) -> str:
    if not rows:
        return ""

    select_parts = []
    for row in rows:
        computed_at = row.get("computed_at")
        computed_at_sql = (
            f"cast({sql_literal(computed_at)} as timestamp)"
            if computed_at is not None
            else "cast(NULL as timestamp)"
        )
        select_parts.append(
            "SELECT "
            + ", ".join(
                [
                    sql_literal(row.get("table_name")),
                    sql_literal(row.get("check_type")),
                    sql_literal(row.get("metric_name")),
                    sql_literal(row.get("metric_expr")),
                    sql_literal(row.get("value")),
                    sql_literal(row.get("where_clause")),
                    sql_literal(row.get("where_hash")),
                    sql_literal(row.get("partition_spec")),
                    computed_at_sql,
                ]
            )
        )

    union_sql = "\nUNION ALL\n".join(select_parts)
    return (
        f"INSERT OVERWRITE TABLE {db}.{table} PARTITION (run_id='{run_id}')\n"
        f"{union_sql}"
    )


def import_old_summary(args, env: Dict) -> None:
    cluster_env = get_cluster_env(env, "new")
    host = args.host or cluster_env.get("host")
    port = args.port or cluster_env.get("port")
    username = args.username or cluster_env.get("username")
    db = args.db or get_validation_db(env)

    if not host or not port or not username:
        print("Missing Hive connection info. Please set host/port/username in new/env.yml.")
        sys.exit(2)

    rows = read_csv_rows(args.input)
    if rows and "run_id" in rows[0]:
        mismatched = [r for r in rows if r.get("run_id") and r.get("run_id") != args.run_id]
        if mismatched:
            print("Warning: run_id in CSV does not match --run-id, proceeding with provided run_id.")
    sql = build_import_sql(db, "old_summary", args.run_id, rows)
    if not sql:
        print("No rows found in CSV, nothing to import.")
        return

    print(f"Importing {len(rows)} rows into {db}.old_summary (run_id={args.run_id})")
    if args.dry_run:
        print(sql)
        return

    execute_sql_with_retry(
        host,
        port,
        username,
        args.hive_set,
        sql,
        args.retries,
        args.retry_wait,
    )
    print("Import done")


def compute_new_summary(args, env: Dict) -> None:
    cluster_env = get_cluster_env(env, "new")
    host = args.host or cluster_env.get("host")
    port = args.port or cluster_env.get("port")
    username = args.username or cluster_env.get("username")
    db = args.db or get_validation_db(env)

    if not host or not port or not username:
        print("Missing Hive connection info. Please set host/port/username in new/env.yml.")
        sys.exit(2)

    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    select_sql = build_summary_sql(
        config, args.run_id, args.biz_date, include_run_id=False
    )
    sql = (
        f"INSERT OVERWRITE TABLE {db}.new_summary PARTITION (run_id='{args.run_id}')\n"
        f"{select_sql}"
    )
    print(f"Generated SQL with {select_sql.count('SELECT')} queries")
    if args.dry_run:
        print(sql)
        return

    execute_sql_with_retry(
        host,
        port,
        username,
        args.hive_set,
        sql,
        args.retries,
        args.retry_wait,
    )
    print("Compute done")


def compare_old_new(args, env: Dict) -> None:
    cluster_env = get_cluster_env(env, "new")
    host = args.host or cluster_env.get("host")
    port = args.port or cluster_env.get("port")
    username = args.username or cluster_env.get("username")
    db = args.db or get_validation_db(env)

    if not host or not port or not username:
        print("Missing Hive connection info. Please set host/port/username in new/env.yml.")
        sys.exit(2)

    compare_sql = f"""
INSERT OVERWRITE TABLE {db}.compare_result PARTITION (run_id='{args.run_id}')
SELECT
  COALESCE(o.table_name, n.table_name) AS table_name,
  COALESCE(o.check_type, n.check_type) AS check_type,
  COALESCE(o.metric_name, n.metric_name) AS metric_name,
  COALESCE(o.partition_spec, n.partition_spec) AS partition_spec,
  CASE
    WHEN o.value IS NULL THEN 'FAIL'
    WHEN n.value IS NULL THEN 'FAIL'
    WHEN o.value = n.value THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  o.value AS old_value,
  n.value AS new_value,
  CASE
    WHEN o.value IS NOT NULL AND n.value IS NOT NULL
         AND o.value RLIKE '^-?\\\\d+(\\\\.\\\\d+)?$'
         AND n.value RLIKE '^-?\\\\d+(\\\\.\\\\d+)?$'
      THEN CAST(n.value AS DOUBLE) - CAST(o.value AS DOUBLE)
    ELSE NULL
  END AS diff,
  CASE
    WHEN o.value IS NULL THEN 'missing_old'
    WHEN n.value IS NULL THEN 'missing_new'
    WHEN o.value = n.value THEN ''
    ELSE 'not_equal'
  END AS reason,
  current_timestamp() AS compared_at
FROM
  (SELECT * FROM {db}.old_summary WHERE run_id = '{args.run_id}') o
FULL OUTER JOIN
  (SELECT * FROM {db}.new_summary WHERE run_id = '{args.run_id}') n
ON o.table_name = n.table_name
 AND o.check_type = n.check_type
 AND o.metric_name = n.metric_name
 AND o.partition_spec = n.partition_spec
 AND o.where_hash = n.where_hash
""".strip()

    if args.dry_run:
        print(compare_sql)
        return

    execute_sql_with_retry(
        host,
        port,
        username,
        args.hive_set,
        compare_sql,
        args.retries,
        args.retry_wait,
    )

    config_hash = ""
    if args.config:
        config_hash = file_md5(args.config)

    runtime = env.get("runtime") or {}
    env_tag = args.env_tag or runtime.get("env") or ""
    created_by = args.created_by or runtime.get("created_by") or ""
    note = args.note or ""

    runs_sql = f"""
INSERT OVERWRITE TABLE {db}.runs
SELECT * FROM {db}.runs WHERE run_id <> '{args.run_id}'
UNION ALL
SELECT
  '{args.run_id}' AS run_id,
  {sql_literal(args.batch_id)} AS batch_id,
  {sql_literal(env_tag)} AS env,
  current_timestamp() AS start_time,
  current_timestamp() AS end_time,
  CASE
    WHEN SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) > 0 THEN 'FAILED'
    ELSE 'SUCCESS'
  END AS status,
  '{config_hash}' AS config_hash,
  {sql_literal(created_by)} AS created_by,
  {sql_literal(note)} AS note
FROM {db}.compare_result
WHERE run_id = '{args.run_id}'
""".strip()

    execute_sql_with_retry(
        host,
        port,
        username,
        args.hive_set,
        runs_sql,
        args.retries,
        args.retry_wait,
    )
    print("Compare done")


def fetch_rows(conn, sql: str) -> List[Dict[str, str]]:
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows


def generate_report(args, env: Dict) -> None:
    cluster_env = get_cluster_env(env, "new")
    host = args.host or cluster_env.get("host")
    port = args.port or cluster_env.get("port")
    username = args.username or cluster_env.get("username")
    db = args.db or get_validation_db(env)

    if not host or not port or not username:
        print("Missing Hive connection info. Please set host/port/username in new/env.yml.")
        sys.exit(2)

    output = (
        args.output
        or env.get("paths", {}).get("report")
        or "output/compare_report.md"
    )

    conn = connect_hive(host, port, username)
    cursor = conn.cursor()
    apply_hive_settings(cursor, args.hive_set)
    cursor.close()
    rows = fetch_rows(
        conn,
        f"""
SELECT
  table_name,
  check_type,
  metric_name,
  partition_spec,
  status,
  old_value,
  new_value,
  diff,
  reason
FROM {db}.compare_result
WHERE run_id = '{args.run_id}'
ORDER BY status DESC, table_name, check_type, metric_name
""".strip(),
    )
    conn.close()

    total = len(rows)
    failed = len([r for r in rows if r.get("status") == "FAIL"])
    passed = len([r for r in rows if r.get("status") == "PASS"])

    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write(f"# Compare Report (run_id={args.run_id})\n\n")
        f.write(f"- Total: {total}\n")
        f.write(f"- PASS: {passed}\n")
        f.write(f"- FAIL: {failed}\n\n")
        f.write("|table|check_type|metric|partition|status|old|new|diff|reason|\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for r in rows:
            f.write(
                "|{table_name}|{check_type}|{metric_name}|{partition_spec}|{status}|{old_value}|{new_value}|{diff}|{reason}|\n".format(
                    table_name=r.get("table_name", ""),
                    check_type=r.get("check_type", ""),
                    metric_name=r.get("metric_name", ""),
                    partition_spec=r.get("partition_spec", ""),
                    status=r.get("status", ""),
                    old_value=r.get("old_value", ""),
                    new_value=r.get("new_value", ""),
                    diff=r.get("diff", ""),
                    reason=r.get("reason", ""),
                )
            )
    print(f"Report written to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="New cluster validation (PyHive)")
    parser.add_argument("--env", default="new/env.yml", help="Env config file")
    parser.add_argument("--db", help="Validation DB name override")
    parser.add_argument("--host", help="Hive host")
    parser.add_argument("--port", type=int, help="Hive port")
    parser.add_argument("--username", help="Hive username")
    parser.add_argument("--dry-run", action="store_true", help="Only print SQL")
    parser.add_argument(
        "--hive-set",
        action="append",
        default=[],
        help="Hive session setting, e.g. hive.exec.parallel=true (repeatable)",
    )
    parser.add_argument("--retries", type=int, default=0, help="Retry count on failure")
    parser.add_argument("--retry-wait", type=int, default=3, help="Retry wait seconds")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_import = subparsers.add_parser("import", help="Import old summary CSV")
    p_import.add_argument("--run-id", required=True, help="Run ID")
    p_import.add_argument("--input", required=True, help="CSV file path")

    p_compute = subparsers.add_parser("compute", help="Compute new summary")
    p_compute.add_argument("--config", required=True, help="YAML rules file")
    p_compute.add_argument("--run-id", required=True, help="Run ID")
    p_compute.add_argument("--biz-date", help="Business date")

    p_compare = subparsers.add_parser("compare", help="Compare old vs new")
    p_compare.add_argument("--run-id", required=True, help="Run ID")
    p_compare.add_argument("--config", help="Rules file for config hash")
    p_compare.add_argument("--batch-id", help="Batch ID")
    p_compare.add_argument("--env-tag", help="Environment tag")
    p_compare.add_argument("--created-by", help="Created by")
    p_compare.add_argument("--note", help="Note")

    p_report = subparsers.add_parser("report", help="Generate markdown report")
    p_report.add_argument("--run-id", required=True, help="Run ID")
    p_report.add_argument("--output", help="Markdown output path")

    args = parser.parse_args()
    env = load_env(args.env)

    invalid_settings = [s for s in args.hive_set if "=" not in s]
    if invalid_settings:
        print(f"Invalid --hive-set (expected key=value): {invalid_settings}")
        sys.exit(2)

    if args.command == "import":
        import_old_summary(args, env)
    elif args.command == "compute":
        compute_new_summary(args, env)
    elif args.command == "compare":
        compare_old_new(args, env)
    elif args.command == "report":
        generate_report(args, env)
    else:
        parser.error("Unknown command")


if __name__ == "__main__":
    main()
