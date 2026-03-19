#!/usr/bin/env python3
import argparse
import json
import os
import sys

try:
    import pymysql
except ImportError:
    print("ERROR: 请安装 PyMySQL: pip install pymysql")
    sys.exit(1)

def build_insert_sql(table_name, metrics, partition_cols, data_dt, config_file):
    metric_selects = [f"cast({m['expr']} as string) as {m['name']}" for m in metrics]
    select_cols = [f"'{table_name}' as table_name"]
    select_cols.extend(metric_selects)
    select_cols.extend(partition_cols)

    if partition_cols:
        partition_expr = "concat_ws('/', " + ", ".join([f"concat('{pc}=', cast({pc} as string))" for pc in partition_cols]) + ")"
    else:
        partition_expr = "''"
    select_cols.append(f"{partition_expr} as partition_spec")
    select_cols.append("current_timestamp() as computed_at")
    select_cols.append(f"'{data_dt}' as data_dt")

    select_sql = f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE 1=1"

    with open(config_file, "r") as f:
        config = json.load(f)
    validation_db = config.get("validation_db", "validation_db")

    return f"INSERT INTO TABLE {validation_db}.old_summary {select_sql}"

def generate_sql_file(table_rules, data_dt, config_file, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    sql_statements = []

    for rule in table_rules:
        sql = build_insert_sql(rule["table"], rule["metrics"], rule["partition_cols"], data_dt, config_file)
        sql_statements.append(sql)

    sql_file = os.path.join(output_dir, "metrics_queries.sql")
    with open(sql_file, "w") as f:
        f.write(f"-- Data Date: {data_dt}\n")
        f.write(f"-- Total INSERT statements: {len(sql_statements)}\n\n")
        for sql in sql_statements:
            f.write(sql + ";\n")

    print(f"生成完成: {sql_file}")

def get_partition_columns(config_file, database, table):
    with open(config_file, "r") as f:
        config = json.load(f)
    conn = pymysql.connect(host=config["metastore_mysql"]["host"], port=config["metastore_mysql"]["port"],
                          user=config["metastore_mysql"]["user"], password=config["metastore_mysql"]["password"],
                          database=config["metastore_mysql"]["db"], charset="utf8mb4",
                          cursorclass=pymysql.cursors.Cursor)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT p.PKEY_NAME FROM PARTITION_KEYS p JOIN TBLS t ON t.TBL_ID = p.TBL_ID JOIN DBS d ON t.DB_ID = d.DB_ID WHERE d.NAME = %s AND t.TBL_NAME = %s ORDER BY p.INTEGER_IDX", (database, table))
            return [row[0] for row in cur.fetchall() if row[0]]
    finally:
        conn.close()

def get_decimal_precision_and_scale(type_str):
    if '(' in type_str:
        content = type_str[type_str.find('(')+1:type_str.rfind(')')]
        parts = [p.strip() for p in content.split(',')]
        if len(parts) >= 1:
            return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    return 18, 2

def get_table_columns(config_file, table_list):
    with open(config_file, "r") as f:
        config = json.load(f)
    conn = pymysql.connect(host=config["metastore_mysql"]["host"], port=config["metastore_mysql"]["port"],
                          user=config["metastore_mysql"]["user"], password=config["metastore_mysql"]["password"],
                          database=config["metastore_mysql"]["db"], charset="utf8mb4",
                          cursorclass=pymysql.cursors.Cursor)
    num_tables = len(table_list)
    placeholders = ','.join(['(%s,%s)'] * num_tables)
    sql = f"SELECT d.NAME, t.TBL_NAME, c.COLUMN_NAME, c.TYPE_NAME FROM TBLS t JOIN DBS d ON t.DB_ID = d.DB_ID JOIN SDS s ON t.SD_ID = s.SD_ID JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID WHERE (d.NAME, t.TBL_NAME) IN ({placeholders}) ORDER BY d.NAME, t.TBL_NAME, c.INTEGER_IDX"
    params = []
    for db_name, tbl_name in table_list:
        params.extend([db_name, tbl_name])
    columns_by_table = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for db_name, tbl_name, col_name, type_name in cur.fetchall():
                db = (db_name or "").strip()
                t = (tbl_name or "").strip()
                c = (col_name or "").strip()
                dt = (type_name or "").strip().lower()
                if db and t and c:
                    key = (db, t)
                    columns_by_table.setdefault(key, []).append({"name": c, "type": dt})
    finally:
        conn.close()
    return columns_by_table

def build_table_rule(config_file, database, table, columns):
    full_name = f"{database}.{table}"
    rule = {
        "table": full_name,
        "partition_cols": [],
        "metrics": [{"name": "row_count", "expr": "count(1)"}]
    }
    try:
        rule["partition_cols"] = get_partition_columns(config_file, database, table)
    except:
        pass
    for col in columns:
        if col["type"].startswith("decimal") or col["type"].startswith("numeric"):
            precision, scale = get_decimal_precision_and_scale(col["type"])
            metric_name = f"{col['name']}_sum"
            expr = f"sum(cast({col['name']} as decimal({precision},{scale})))"
            rule["metrics"].append({"name": metric_name, "expr": expr})
    return rule

def load_table_list(path):
    tables = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '.' in line:
                db, tbl = line.split('.', 1)
                tables.append((db.strip(), tbl.strip()))
    return tables

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-list", required=True)
    parser.add_argument("--data-dt", required=True)
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    tables = load_table_list(args.table_list)
    columns_by_table = get_table_columns(args.config, tables)
    rules = []
    for db, tbl in sorted(columns_by_table.keys()):
        cols = columns_by_table[(db, tbl)]
        rule = build_table_rule(args.config, db, tbl, cols)
        rules.append(rule)

    generate_sql_file(rules, args.data_dt, args.config, args.output_dir)

if __name__ == "__main__":
    main()