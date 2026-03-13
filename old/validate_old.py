"""
Old cluster validation CLI (PyHive).

Reads JSON rules, executes Hive SQL in batches with concurrency,
writes to Hive table first, then exports CSV summary.

Example:
  python old/validate_old.py --config /tmp/test_rules.json --env old/env_config.json --run-id run_001 --biz-date 2026-03-05
  python old/validate_old.py --config /tmp/test_rules.json --env old/env_config.json --init-schema --init-only --run-id init_001
"""

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

def escape_sql_string(text: str) -> str:
    return text.replace("'", "''")


def generate_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def load_json(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    # Remove // comments
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip comment-only lines
        if stripped.startswith('//'):
            continue
        # Remove inline // comments
        if '//' in line:
            idx = line.find('//')
            before = line[:idx].rstrip()
            if before:
                cleaned_lines.append(before)
        else:
            cleaned_lines.append(line.rstrip())

    return json.loads('\n'.join(cleaned_lines))


def load_env(path: str) -> Dict:
    """Load environment config from JSON."""
    return load_json(path)


def get_cluster_env(env: Dict, name: Optional[str] = None) -> Dict:
    clusters = env.get("clusters") or {}
    if name and name in clusters:
        return clusters[name] or {}
    return env.get("hive") or {}


def filter_tables(config: dict) -> List[dict]:
    """Filter tables based on mode settings."""
    tables = config.get("tables", [])
    mode = config.get("mode", {})

    include_all = mode.get("include_all", True)
    exclude_tables = set(mode.get("exclude_tables", []))
    selected_tables = set(mode.get("selected_tables", []))

    if not include_all:
        # White-list mode: only selected_tables
        return [t for t in tables if t.get("enabled", True) and f"{t['table']}" in selected_tables]
    else:
        # Include all mode: exclude excluded_tables
        return [t for t in tables if t.get("enabled", True) and f"{t['table']}" not in exclude_tables]


def build_summary_sql(
    table: dict,
    run_id: str,
    biz_date: str = None,
    default_where: str = "1=1",
) -> str:
    """Build SQL for a single table."""
    t = table
    table_name = t["table"]
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

    parts: List[str] = []

    for m in metrics:
        name, expr = m["name"], m["expr"]
        expr_escaped = escape_sql_string(expr)
        select_cols = [
            f"'{table_name}' as table_name",
            "'metric' as check_type",
            f"'{name}' as metric_name",
            f"'{expr_escaped}' as metric_expr",
            f"cast({expr} as string) as value",
            f"'{where_escaped}' as where_clause",
            f"'{where_hash}' as where_hash",
            f"{partition_spec_expr} as partition_spec",
            "current_timestamp() as computed_at",
            f"'{run_id}' as run_id",
        ]
        parts.append(
            f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE {where}{group_by_sql}"
        )

    if keys:
        keys_expr = ", ".join(keys)
        select_cols = [
            f"'{table_name}' as table_name",
            "'pk_dup' as check_type",
            "'pk_dup_count' as metric_name",
            f"'count(*) - count(distinct {keys_expr})' as metric_expr",
            f"cast(count(*) - count(distinct {keys_expr}) as string) as value",
            f"'{where_escaped}' as where_clause",
            f"'{where_hash}' as where_hash",
            f"{partition_spec_expr} as partition_spec",
            "current_timestamp() as computed_at",
            f"'{run_id}' as run_id",
        ]
        parts.append(
            f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE {where}{group_by_sql}"
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


def check_kerberos_ticket() -> bool:
    """Check if user has valid Kerberos ticket."""
    import subprocess
    try:
        result = subprocess.run(
            ["klist"],
            capture_output=True,
            text=True,
            timeout=5
        )
        # If klist succeeds and shows valid tickets, return True
        if result.returncode == 0 and "Valid" in result.stdout:
            return True
    except Exception:
        pass
    return False


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

    # Check for Kerberos authentication
    if auth == "AUTO":
        if check_kerberos_ticket():
            print("Kerberos ticket found, using GSSAPI auth")
            candidates.append({**base_kwargs, "auth": "GSSAPI"})
        # Also try other methods
        candidates.append({**base_kwargs})
        candidates.append({**base_kwargs, "auth": "NOSASL"})
        candidates.append({**base_kwargs, "auth": "NONE"})
    elif auth == "KERBEROS":
        if check_kerberos_ticket():
            candidates.append({**base_kwargs, "auth": "GSSAPI"})
        else:
            raise RuntimeError("Kerberos auth requested but no valid ticket found. Run 'kinit' first.")
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
  computed_at TIMESTAMP,
  run_id STRING
)
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
  computed_at TIMESTAMP,
  run_id STRING
)
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
  compared_at TIMESTAMP,
  run_id STRING
)
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


def execute_batch(
    sql: str,
    connect_candidates: List[Dict],
    hive_settings,
    retries: int,
    retry_wait: int,
    batch_num: int,
    lock: threading.Lock,
    table_name: str = None,
) -> tuple:
    """Execute a batch SQL and return (columns, rows)."""
    try:
        from pyhive import hive
    except ImportError:
        print("Please install pyhive first: pip install pyhive[hive]")
        sys.exit(1)

    attempt = 0
    while True:
        try:
            conn, _ = open_hive_connection(hive, connect_candidates)
            cursor = conn.cursor()
            apply_hive_settings(cursor, hive_settings)

            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            cursor.close()
            conn.close()

            with lock:
                print(f"Table {table_name}: fetched {len(rows)} rows")
            return columns, rows

        except Exception as exc:
            attempt += 1
            with lock:
                print(f"[ERROR] Table {table_name} failed: {exc}")
            if attempt > retries:
                raise
            time.sleep(retry_wait)


def run_hive(
    sql: str,
    output: str,
    connect_candidates: List[Dict],
    hive_settings,
    retries: int,
    retry_wait: int,
) -> None:
    """Execute Hive SQL and write CSV output (for dry-run or small queries)."""
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
    parser.add_argument("--config", required=True, help="JSON rules file")
    parser.add_argument("--env", default="old/env_config.json", help="Env config file (JSON)")
    parser.add_argument("--run-id", required=True, help="Run ID")
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
    parser.add_argument("--threads", type=int, default=10, help="Number of concurrent threads")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of tables per UNION ALL batch")
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
        print("Missing Hive connection info. Please set host/port/username in env config.")
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

    # Load rules from JSON
    config = load_json(args.config)

    # Filter tables based on mode
    tables = filter_tables(config)
    default_where = config.get("global", {}).get("default_where", "1=1")

    print(f"Total tables to process: {len(tables)}")
    print(f"Concurrency: {args.threads} threads")

    if args.dry_run:
        for i, t in enumerate(tables):
            sql = build_summary_sql(t, args.run_id, args.biz_date, default_where)
            print(f"\n=== Table {i + 1}: {t['table']} ===")
            print(sql)
        return

    # Create thread lock for printing
    lock = threading.Lock()

    # Execute tables in parallel (one SQL per table)
    start_time = time.time()
    all_rows = []
    columns = None
    failed_tables = []

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {}
        for i, t in enumerate(tables):
            sql = build_summary_sql(t, args.run_id, args.biz_date, default_where)
            future = executor.submit(
                execute_batch,
                sql,
                connect_candidates,
                args.hive_set,
                args.retries,
                args.retry_wait,
                i + 1,
                lock,
                t["table"],
            )
            futures[future] = t["table"]

        # Wait for all to complete and collect results
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                result = future.result()
                if result:
                    batch_columns, batch_rows = result
                    all_rows.extend(batch_rows)
                    if columns is None:
                        columns = batch_columns
            except Exception as exc:
                print(f"[ERROR] Table {table_name} failed: {exc}")
                failed_tables.append(table_name)
                raise

    elapsed = time.time() - start_time
    print(f"All batches completed in {elapsed:.2f} seconds, total rows: {len(all_rows)}")

    # Write to CSV directly
    print("Writing to CSV...")
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)

    # Use default columns if not set
    if columns is None:
        columns = ["table_name", "check_type", "metric_name", "metric_expr", "value",
                   "where_clause", "where_hash", "partition_spec", "computed_at", "run_id"]

    with open(output, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in all_rows:
            writer.writerow(row)

    print(f"Wrote {len(all_rows)} rows to {output}")
    print("Done")


if __name__ == "__main__":
    main()
