# 飞书多维表格与数据库双向实时同步部署指南

## 系统架构

本方案通过以下技术实现双向"准实时"同步：

1. **飞书 → 数据库**：轮询检测变更（每5-10秒）
2. **数据库 → 飞书**：数据库触发器 + 同步队列
3. **防循环同步**：基于哈希值和时间戳的去重机制

## 部署步骤

### 1. 环境准备

```bash
# 安装依赖
pip install feishu-bitable-db pymysql redis loguru

# 安装 Redis（用于分布式锁，可选）
sudo apt-get install redis-server
```

### 2. 数据库准备

执行 `database_triggers.sql` 创建必要的表和触发器：

```bash
mysql -u root -p your_database < database_triggers.sql
```

### 3. 飞书应用配置

1. 创建飞书企业自建应用
2. 获取 App ID 和 App Secret
3. 申请以下权限：
   - `drive:drive`
   - `bitable:app`
4. 在飞书多维表格中添加应用

### 4. 配置文件

创建 `config.json`：

```json
{
    "feishu": {
        "app_id": "cli_xxxxx",
        "app_secret": "xxxxx"
    },
    "database": {
        "host": "localhost",
        "user": "root",
        "password": "your_password",
        "database": "your_database"
    },
    "sync": {
        "poll_interval": 5,
        "table_mapping": {
            "MyDB:users": "users",
            "MyDB:orders": "orders"
        }
    }
}
```

### 5. 启动同步服务

```python
# run_sync.py
import json
from sync_service import RealtimeSyncService

# 加载配置
with open('config.json') as f:
    config = json.load(f)

# 创建并启动服务
service = RealtimeSyncService(
    app_id=config['feishu']['app_id'],
    app_secret=config['feishu']['app_secret'],
    db_config=config['database']
)

# 配置表映射
service.table_mapping = config['sync']['table_mapping']
service.poll_interval = config['sync']['poll_interval']

# 启动
service.start()

# 保持运行
try:
    import time
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    service.stop()
```

### 6. 使用 systemd 管理服务（Linux）

创建 `/etc/systemd/system/feishu-sync.service`：

```ini
[Unit]
Description=Feishu Database Sync Service
After=network.target mysql.service

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/your/project
ExecStart=/usr/bin/python3 /path/to/your/project/run_sync.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl enable feishu-sync
sudo systemctl start feishu-sync
sudo systemctl status feishu-sync
```

## 监控和运维

### 1. 查看同步状态

```sql
-- 查看待同步队列
SELECT * FROM sync_queue WHERE status = 'pending';

-- 查看最近的同步日志
SELECT * FROM sync_log ORDER BY created_at DESC LIMIT 20;

-- 查看同步失败的记录
SELECT * FROM sync_queue WHERE status = 'failed' OR retry_count >= 3;
```

### 2. 监控指标

```python
# monitor.py
import pymysql
from datetime import datetime, timedelta

def get_sync_metrics(db_config):
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    
    # 队列积压
    cursor.execute("SELECT COUNT(*) FROM sync_queue WHERE status = 'pending'")
    pending_count = cursor.fetchone()[0]
    
    # 失败率
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as success,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
        FROM sync_queue 
        WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
    """)
    result = cursor.fetchone()
    
    # 平均延迟
    cursor.execute("""
        SELECT AVG(TIMESTAMPDIFF(SECOND, created_at, processed_at)) as avg_delay
        FROM sync_queue 
        WHERE status = 'completed' 
        AND created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
    """)
    avg_delay = cursor.fetchone()[0] or 0
    
    print(f"Pending: {pending_count}")
    print(f"Success Rate: {result[0]/(result[0]+result[1])*100:.2f}%")
    print(f"Average Delay: {avg_delay:.2f}s")
    
    conn.close()
```

### 3. 告警配置

当出现以下情况时应该告警：

- 队列积压超过 1000 条
- 失败率超过 10%
- 平均延迟超过 30 秒

## 性能优化

### 1. 批量同步

修改 `sync_service.py` 支持批量操作：

```python
def batch_sync_to_feishu(self, records: List[Dict]):
    """批量同步到飞书"""
    # 使用飞书批量 API
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        # 批量创建/更新
```

### 2. 索引优化

确保数据库表有适当的索引：

```sql
-- 为经常查询的字段添加索引
ALTER TABLE users ADD INDEX idx_feishu_id (feishu_id);
ALTER TABLE users ADD INDEX idx_updated_at (updated_at);
```

### 3. 缓存优化

使用 Redis 缓存频繁访问的数据：

```python
import redis

class CachedSyncService(RealtimeSyncService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = redis.Redis()
    
    def get_cached_mapping(self, db_id: str) -> Optional[str]:
        """获取缓存的 ID 映射"""
        return self.redis.get(f"id_mapping:{db_id}")
```

## 故障处理

### 1. 同步循环

如果发现数据在两个系统间反复同步：

```sql
-- 查看循环同步的记录
SELECT record_id, COUNT(*) as sync_count 
FROM sync_log 
WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY record_id 
HAVING sync_count > 10;

-- 临时禁用某条记录的同步
INSERT INTO sync_blacklist (record_id, reason) 
VALUES ('problematic_id', 'sync loop detected');
```

### 2. 数据不一致

定期运行数据一致性检查：

```python
def check_data_consistency(self):
    """检查数据一致性"""
    for feishu_table, db_table in self.table_mapping.items():
        # 获取两边的记录数
        feishu_count = len(self.feishu.read(...))
        db_count = self.get_db_record_count(db_table)
        
        if abs(feishu_count - db_count) > 10:
            logger.warning(f"Data inconsistency: {feishu_table}")
```

## 注意事项

1. **API 限制**：飞书 API 有调用频率限制，建议轮询间隔不小于 5 秒
2. **数据量**：对于大量数据，建议先做全量同步，再启动增量同步
3. **字段映射**：确保飞书表格字段和数据库字段类型兼容
4. **时区问题**：注意处理时间字段的时区转换
5. **权限管理**：定期检查飞书应用权限是否正常

## 总结

这个方案虽然不是真正的"实时"同步（有 5-10 秒延迟），但已经能满足大部分业务场景的需求。通过合理的架构设计和防循环机制，可以实现稳定可靠的双向数据同步。