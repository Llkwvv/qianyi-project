#!/usr/bin/env python3
"""
简单的 Hive 查询脚本，用于验证数据
"""

import subprocess
import sys
import os

def execute_hive_query(query, ssh_config):
    """通过 SSH 执行 Hive 查询"""
    ssh_host = ssh_config.get('ssh_host')
    ssh_port = ssh_config.get('ssh_port', 22)
    ssh_user = ssh_config.get('ssh_user')

    try:
        # 创建临时 SQL 文件
        remote_sql_file = f"/tmp/verify_query_{os.getpid()}.sql"
        write_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "cat > {remote_sql_file}"'

        write_process = subprocess.Popen(
            write_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        sql_with_engine = "set hive.execution.engine=mr;\n" + query
        stdout, stderr = write_process.communicate(input=sql_with_engine, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return None

        # 执行查询 - 使用 beeline
        beeline_url = ssh_config.get('beeline_url', 'jdbc:hive2://localhost:10000/default')
        ssh_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "beeline -u \'{beeline_url}\' -n {ssh_user} -f {remote_sql_file}"'
        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        # 清理临时文件
        subprocess.run(
            f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "rm -f {remote_sql_file}"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if result.returncode != 0:
            print(f"执行失败: {result.stderr}")
            return None

        return result.stdout.strip()

    except Exception as e:
        print(f"执行错误: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python verify_data.py <查询语句>")
        sys.exit(1)

    query = sys.argv[1]

    # SSH 配置（硬编码用于测试）
    ssh_config = {
        'ssh_host': '172.20.10.6',
        'ssh_port': 22,
        'ssh_user': 'atguigu',
        'beeline_url': 'jdbc:hive2://localhost:10000/default'
    }

    result = execute_hive_query(query, ssh_config)
    if result:
        print(result)
    else:
        print("查询执行失败")
        sys.exit(1)
