"""
Execute pre-generated SQL files for old cluster validation (PyHive).

Example:
  python old/execute_sql.py --sql-dir /tmp/sql_output --env old/env_config.json --run-id run_001
"""

import argparse
import csv
import glob
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional


def load_json(path: str) -> Dict:
    """Load JSON file."""
    import json
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    # Remove // comments
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('//'):
            continue
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

    if auth == "AUTO":
        if check_kerberos_ticket():
            print("Kerberos ticket found, using GSSAPI auth")
            candidates.append({**base_kwargs, "auth": "GSSAPI"})
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


def execute_batch(
    sql: str,
    connect_candidates: List[Dict],
    hive_settings,
    retries: int,
    retry_wait: int,
    batch_num: int,
    lock: threading.Lock,
    name: str = None,
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
                print(f"{name}: fetched {len(rows)} rows")
            return columns, rows

        except Exception as exc:
            attempt += 1
            with lock:
                print(f"[ERROR] {name} failed: {exc}")
            if attempt > retries:
                raise
            time.sleep(retry_wait)


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute pre-generated SQL files (PyHive)")
    parser.add_argument("--sql-dir", required=True, help="Directory containing pre-generated SQL files")
    parser.add_argument("--env", default="old/env_config.json", help="Env config file (JSON)")
    parser.add_argument("--run-id", required=True, help="Run ID")
    parser.add_argument("--output", help="Output CSV path")
    parser.add_argument("--db", help="Validation DB override")
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
    output = args.output or env.get("paths", {}).get("old_summary") or "output/old_summary.csv"

    if not host or not port or not username:
        print("Missing Hive connection info. Please set host/port/username in env config.")
        sys.exit(2)
    connect_candidates = build_hive_connect_candidates(host, port, username, cluster_env, args)

    # Load SQL files
    sql_files = glob.glob(os.path.join(args.sql_dir, "*.sql"))
    if not sql_files:
        print(f"No SQL files found in {args.sql_dir}")
        sys.exit(1)

    # Read all SQL files
    sql_tasks = []
    for sql_file in sorted(sql_files):
        db_name = os.path.basename(sql_file).replace(".sql", "")
        with open(sql_file, "r", encoding="utf-8") as f:
            content = f.read()
            # Remove leading comments
            lines = content.split("\n")
            sql_lines = []
            for line in lines:
                if line.startswith("--"):
                    continue
                sql_lines.append(line)
            sql = "\n".join(sql_lines).strip()
            if sql:
                sql_tasks.append((db_name, sql))

    print(f"Loaded {len(sql_tasks)} SQL files from {args.sql_dir}")
    print(f"Concurrency: {args.threads} threads")

    if args.dry_run:
        for name, sql in sql_tasks:
            print(f"\n=== {name} ===")
            print(sql)
        return

    # Create thread lock for printing
    lock = threading.Lock()

    # Execute SQL in parallel
    start_time = time.time()
    all_rows = []
    columns = None

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {}
        for i, (name, sql) in enumerate(sql_tasks):
            future = executor.submit(
                execute_batch,
                sql,
                connect_candidates,
                args.hive_set,
                args.retries,
                args.retry_wait,
                i + 1,
                lock,
                name,
            )
            futures[future] = name

        # Wait for all to complete and collect results
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                if result:
                    batch_columns, batch_rows = result
                    all_rows.extend(batch_rows)
                    if columns is None:
                        columns = batch_columns
            except Exception as exc:
                print(f"[ERROR] {name} failed: {exc}")
                raise

    elapsed = time.time() - start_time
    print(f"All batches completed in {elapsed:.2f} seconds, total rows: {len(all_rows)}")

    # Write to CSV
    print("Writing to CSV...")
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)

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
