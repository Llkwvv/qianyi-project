"""
Old cluster validation CLI (PyHive).

Reads JSON rules, executes Hive SQL, and exports CSV summary.
Example:
  python old/validate_old.py --config old/rules.generated.json --env old/env.yml --run-id run_001 --biz-date 2026-03-05
  python old/validate_old.py --env old/env.yml --init-schema --init-only --run-id init_001
"""

import argparse
import csv
import hashlib
import os
import sys
import time
from typing import Dict, List, Optional

try:
    import yaml  # Still needed for env.yml
    import json  # For rules file
except ImportError:
    print("Please install pyyaml first: pip install -r old/requirements.txt")
    sys.exit(1)


def escape_sql_string(text: str) -> str:
    return text.replace("'", "''")


def generate_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def load_yaml(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_env(path: str) -> Dict:
    data = load_yaml(path)
    return data or {}


def get_cluster_env(env: Dict, name: Optional[str] = None) -> Dict:
    clusters = env.get("clusters") or {}
    if name and name in clusters:
        return clusters[name] or {}
    return env.get("hive") or {}


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


def _build_partition_spec_expr(partition_cols: List[str]) -> str:
    if not partition_cols:
        return "''"
    parts = [f"concat('{col}=', cast({col} as string))" for col in partition_cols]
    return f"concat_ws('/', {', '.join(parts)})"


def _build_group_by_sql(partition_cols: List[str]) -> str:
    if not partition_cols:
        return ""
    return f" GROUP BY {', '.join(partition_cols)}"


def apply_hive_settings(cursor, settings) -> None:
    for setting in settings:
        cursor.execute(f"set {setting}")


def build_hive_connect_candidates(
    host: str,
    port: int,
    username: str,
    cluster_env: Dict,
    args,
) -> List[Dict]:
    auth = (args.auth or cluster_env.get("auth") or "AUTO").upper()
    password = args.password or cluster_env.get("password")
    database = args.database or cluster_env.get("database") or "default"

    base_kwargs = {
        "host": host,
        "port": port,
        "username": username,
        "database": database,
    }

    candidates: List[Dict] = []
    if auth == "AUTO":
        # Match the simplest verified form first: hive.connect(host, port, username, database)
        candidates.append({**base_kwargs})
        candidates.append({**base_kwargs, "auth": "NOSASL"})
        candidates.append({**base_kwargs, "auth": "NONE"})
    else:
        connect_kwargs = {**base_kwargs, "auth": auth}
        if auth in {"LDAP", "CUSTOM"} and password:
            connect_kwargs["password"] = password
        candidates.append(connect_kwargs)
    return candidates


def open_hive_connection(hive, connect_candidates: List[Dict]):
    errors = []
    for candidate in connect_candidates:
        candidate_auth = (candidate.get("auth") or "").upper()
        try:
            print(
                f"Trying Hive auth mode: {candidate_auth or 'DEFAULT'} "
                f"({candidate.get('host')}:{candidate.get('port')})"
            )
            conn = hive.connect(**candidate)
            print(f"Hive auth selected: {candidate_auth or 'DEFAULT'}")
            return conn, candidate_auth or "DEFAULT"
        except Exception as exc:
            errors.append((candidate_auth or "DEFAULT", str(exc)))
            print(f"[WARN] Hive auth {candidate_auth or 'DEFAULT'} failed: {exc}")
            continue

    details = "; ".join([f"{mode}: {msg}" for mode, msg in errors]) or "unknown error"
    raise RuntimeError(f"All Hive auth attempts failed. Details: {details}")


def build_schema_init_statements(db: str) -> List[str]:
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"""
CREATE TABLE IF NOT EXISTS {db}.old_summary (
  table_name STRING,
  check_type STRING,
  metric_name STRING,
  metric_expr STRING,
  value STRING,
  where_clause STRING,
  where_hash STRING,
  partition_spec STRING,
  computed_at TIMESTAMP
)
PARTITIONED BY (run_id STRING)
STORED AS ORC
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {db}.new_summary (
  table_name STRING,
  check_type STRING,
  metric_name STRING,
  metric_expr STRING,
  value STRING,
  where_clause STRING,
  where_hash STRING,
  partition_spec STRING,
  computed_at TIMESTAMP
)
PARTITIONED BY (run_id STRING)
STORED AS ORC
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {db}.compare_result (
  table_name STRING,
  check_type STRING,
  metric_name STRING,
  partition_spec STRING,
  status STRING,
  old_value STRING,
  new_value STRING,
  diff DOUBLE,
  reason STRING,
  compared_at TIMESTAMP
)
PARTITIONED BY (run_id STRING)
STORED AS ORC
""".strip(),
        f"""
CREATE TABLE IF NOT EXISTS {db}.runs (
  run_id STRING,
  batch_id STRING,
  env STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  config_hash STRING,
  created_by STRING,
  note STRING
)
STORED AS ORC
""".strip(),
    ]


def init_schema(
    db: str,
    connect_candidates: List[Dict],
    hive_settings,
    retries: int,
    retry_wait: int,
) -> None:
    try:
        from pyhive import hive
    except ImportError:
        print("Please install pyhive first: pip install pyhive[hive]")
        sys.exit(1)

    statements = build_schema_init_statements(db)
    attempt = 0
    while True:
        try:
            print(
                "Initializing schema in "
                f"{db} on Hive: {connect_candidates[0].get('host')}:{connect_candidates[0].get('port')}"
            )
            conn, _ = open_hive_connection(hive, connect_candidates)
            cursor = conn.cursor()
            apply_hive_settings(cursor, hive_settings)
            for stmt in statements:
                cursor.execute(stmt)
            cursor.close()
            conn.close()
            print("Schema init done")
            return
        except Exception as exc:
            attempt += 1
            print(f"[ERROR] Schema init failed: {exc}")
            if attempt > retries:
                raise
            print(f"[RETRY] attempt {attempt}/{retries} in {retry_wait}s")
            time.sleep(retry_wait)


def run_hive(
    sql: str,
    output: str,
    connect_candidates: List[Dict],
    hive_settings,
    retries: int,
    retry_wait: int,
) -> None:
    """Execute Hive SQL and write CSV output."""
    try:
        from pyhive import hive
    except ImportError:
        print("Please install pyhive first: pip install pyhive[hive]")
        sys.exit(1)

    attempt = 0
    while True:
        try:
            print(
                f"Connecting to Hive: {connect_candidates[0].get('host')}:{connect_candidates[0].get('port')}"
            )
            conn, _ = open_hive_connection(hive, connect_candidates)
            cursor = conn.cursor()
            apply_hive_settings(cursor, hive_settings)

            print("Executing SQL...")
            cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description]
            with open(output, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for row in cursor.fetchall():
                    writer.writerow(row)

            cursor.close()
            conn.close()
            print(f"Wrote result to {output}")
            return
        except Exception as exc:
            attempt += 1
            print(f"[ERROR] Hive execution failed: {exc}")
            if attempt > retries:
                raise
            print(f"[RETRY] attempt {attempt}/{retries} in {retry_wait}s")
            time.sleep(retry_wait)


def main() -> None:
    parser = argparse.ArgumentParser(description="Old cluster validation (PyHive)")
    parser.add_argument("--config", default="old/rules.generated.yml", help="YAML rules file")
    parser.add_argument("--env", default="old/env.yml", help="Env config file")
    parser.add_argument("--run-id", default="run_001", help="Run ID")
    parser.add_argument("--biz-date", help="Business date")
    parser.add_argument("--output", help="Output CSV path")
    parser.add_argument("--db", help="Validation DB override")
    parser.add_argument("--init-schema", action="store_true", help="Initialize validation DB schema")
    parser.add_argument("--init-only", action="store_true", help="Initialize schema only, then exit")
    parser.add_argument("--host", help="Hive host")
    parser.add_argument("--port", type=int, help="Hive port")
    parser.add_argument("--username", help="Hive username")
    parser.add_argument("--auth", help="Hive auth mode: NOSASL/NONE/LDAP/CUSTOM")
    parser.add_argument("--password", help="Hive password (for LDAP/CUSTOM)")
    parser.add_argument("--database", help="Hive default database for connection")
    parser.add_argument("--dry-run", action="store_true", help="Only print SQL")
    parser.add_argument(
        "--hive-set",
        action="append",
        default=[],
        help="Hive session setting, e.g. hive.exec.parallel=true (repeatable)",
    )
    parser.add_argument("--retries", type=int, default=0, help="Retry count on failure")
    parser.add_argument("--retry-wait", type=int, default=3, help="Retry wait seconds")
    args = parser.parse_args()

    invalid_settings = [s for s in args.hive_set if "=" not in s]
    if invalid_settings:
        print(f"Invalid --hive-set (expected key=value): {invalid_settings}")
        sys.exit(2)

    env = load_env(args.env)
    cluster_env = get_cluster_env(env, "old")
    host = args.host or cluster_env.get("host")
    port = args.port or cluster_env.get("port")
    username = args.username or cluster_env.get("username")
    db = args.db or env.get("validation_db", "validation_db")
    output = args.output or env.get("paths", {}).get("old_summary") or "output/old_summary.csv"

    if not host or not port or not username:
        print("Missing Hive connection info. Please set host/port/username in old/env.yml.")
        sys.exit(2)
    connect_candidates = build_hive_connect_candidates(host, port, username, cluster_env, args)

    if args.init_schema:
        init_schema(
            db,
            connect_candidates,
            args.hive_set,
            args.retries,
            args.retry_wait,
        )
        if args.init_only:
            return

    if not args.config:
        print("Missing --config. Please provide rules YAML file.")
        sys.exit(2)

    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    sql = build_summary_sql(config, args.run_id, args.biz_date, include_run_id=True)
    print(f"Generated SQL with {sql.count('SELECT')} queries")

    if args.dry_run:
        print(sql)
        return

    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    run_hive(
        sql,
        output,
        connect_candidates,
        args.hive_set,
        args.retries,
        args.retry_wait,
    )
    print("Done")


if __name__ == "__main__":
    main()
