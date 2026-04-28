# PyHive 连接测试报告

## 测试时间
2026-04-24

## 环境信息
- 本机 IP: 172.20.10.3
- Hive 服务器: hadoop102 (双 IP: 172.20.10.6 / 192.168.10.102)
- Hive 端口: 10000
- 用户: atguigu

## 测试结果

### 1. 网络连通性
```
端口 10000 (Thrift) 可达 ✓
Thrift Binary Transport 可用 ✓
```

### 2. PyHive 连接测试
| 测试项 | 结果 |
|--------|------|
| 创建连接 (auth=NONE) | ✓ 成功 |
| 设置 Tez 引擎 | ✓ 成功 |
| 执行查询 | ✓ 成功 (0.20s) |

### 3. 测试命令
```python
from pyhive import hive

conn = hive.Connection(
    host='172.20.10.6',
    port=10000,
    username='atguigu',
    auth='NONE'
)

cursor = conn.cursor()
cursor.execute('SET hive.execution.engine=tez')
cursor.execute('SELECT count(*) as cnt FROM gmall.dim_activity_full WHERE dt = "2023-06-10"')
result = cursor.fetchall()  # [(5,)]
```

## 结论
PyHive 可以成功连接到 Hive 集群并执行查询。

## 配置注意事项
1. 使用 `auth='NONE'` 认证模式
2. 主机地址使用 172.20.10.6 (而非 192.168.10.102)
3. 端口使用 10000 (Thrift 端口)
