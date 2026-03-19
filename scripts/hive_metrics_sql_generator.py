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

def build_select_sql(table_name, metrics, partition_cols, data_dt, config_file):
    metric_selects = [f"cast({m['expr']} as string) as {m['name']}" for m in metrics]
    select_cols = [f"'{table_name}' as table_name"]
    select_cols.extend(metric_selects)

    # 直接将分区字段作为 partition_spec 字段，不重复添加原始分区字段
    if partition_cols:
        # 为分区字段添加别名，作为 partition_spec
        partition_selects = [f"{pc} as partition_spec" for pc in partition_cols]
        select_cols.extend(partition_selects)
    else:
        select_cols.append("'' as partition_spec")

    select_cols.append("current_timestamp() as computed_at")
    select_cols.append(f"'{data_dt}' as data_dt")

    select_sql = f"SELECT {', '.join(select_cols)} FROM {table_name}"

    return select_sql

def generate_sql_file(table_rules, data_dt, config_file, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    sql_statements = []

    for rule in table_rules:
        sql = build_select_sql(rule["table"], rule["metrics"], rule["partition_cols"], data_dt, config_file)
        sql_statements.append(sql)

    sql_file = os.path.join(output_dir, "metrics_queries.sql")
    with open(sql_file, "w") as f:
        f.write(f"-- Data Date: {data_dt}\n")
        f.write(f"-- Total SELECT statements: {len(sql_statements)}\n\n")
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

    # 从配置文件中获取分区列（如果有的话）
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        # 这里需要修改以支持从增强版JSON中获取分区列
        # 为了简化，我们暂时使用从Metastore获取的方式
        try:
            rule["partition_cols"] = get_partition_columns(config_file, database, table)
        except:
            pass
    except:
        pass

    for col in columns:
        # 优先检查字段类型（来自Metastore）
        if "type" in col:
            if col["type"].startswith("decimal") or col["type"].startswith("numeric"):
                precision, scale = get_decimal_precision_and_scale(col["type"])
                metric_name = f"{col['name']}_sum"
                expr = f"sum(cast({col['name']} as decimal({precision},{scale})))"
                rule["metrics"].append({"name": metric_name, "expr": expr})
        else:
            # 如果没有类型信息，但字段名包含数值相关的关键词，则作为数值字段处理
            col_name = col["name"].lower()
            if any(keyword in col_name for keyword in ["amount", "price", "value", "count", "num", "qty", "sum"]):
                metric_name = f"{col['name']}_sum"
                expr = f"sum({col['name']})"
                rule["metrics"].append({"name": metric_name, "expr": expr})
    return rule

def load_table_list(path):
    tables = []
    with open(path, encoding="utf-8") as f:
        # 支持 JSON 格式输入
        try:
            data = json.load(f)
            if "tables" in data:
                for table_info in data["tables"]:
                    if "name" in table_info and "." in table_info["name"]:
                        db, tbl = table_info["name"].split('.', 1)
                        tables.append((db.strip(), tbl.strip()))
        except json.JSONDecodeError:
            # 如果不是 JSON 格式，则回退到原来的文本格式
            f.seek(0)
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '.' in line:
                    db, tbl = line.split('.', 1)
                    tables.append((db.strip(), tbl.strip()))
    return tables

def get_table_columns_from_json(path, table_list):
    """从增强版 JSON 文件中获取字段信息"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    columns_by_table = {}
    partition_cols_by_table = {}

    # 遍历所有表
    for table_info in data["tables"]:
        table_name = table_info["name"]
        if "." in table_name:
            db, tbl = table_name.split('.', 1)

            # 检查是否是我们需要的表
            if (db, tbl) in table_list:
                # 获取分区列
                partition_cols = table_info.get("partition_cols", [])
                partition_cols_by_table[(db, tbl)] = partition_cols

                fields = table_info.get("fields", [])
                column_list = []
                for field in fields:
                    if isinstance(field, dict):
                        # 增强版 JSON 格式，包含字段类型
                        column_list.append({
                            "name": field["name"],
                            "type": field.get("type", "")
                        })
                    else:
                        # 简单字段名格式
                        column_list.append({
                            "name": field,
                            "type": ""
                        })
                columns_by_table[(db, tbl)] = column_list

    return columns_by_table, partition_cols_by_table

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-list", required=True)
    parser.add_argument("--data-dt", required=True)
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    tables = load_table_list(args.table_list)

    # 尝试使用增强版 JSON 处理方式
    try:
        columns_by_table, partition_cols_by_table = get_table_columns_from_json(args.table_list, tables)
    except:
        # 如果失败，回退到原有的数据库方式
        columns_by_table = get_table_columns(args.config, tables)
        partition_cols_by_table = {}

    rules = []
    for db, tbl in sorted(columns_by_table.keys()):
        cols = columns_by_table[(db, tbl)]
        rule = build_table_rule(args.config, db, tbl, cols)

        # 如果从JSON中获得了分区列信息，则使用它
        if (db, tbl) in partition_cols_by_table:
            rule["partition_cols"] = partition_cols_by_table[(db, tbl)]

        rules.append(rule)

    generate_sql_file(rules, args.data_dt, args.config, args.output_dir)

if __name__ == "__main__":
    main()