# 飞书表格与云端数据库双向实时同步方案

## 概述

本文档详细介绍如何实现飞书多维表格与云端数据库的双向实时同步系统。该系统能够确保飞书表格和数据库的数据保持一致，支持双向实时更新。

## 系统架构

```mermaid
graph TB
    A[飞书多维表格] <-->|双向同步| B[同步服务器]
    B <-->|双向同步| C[云端数据库]
    
    subgraph 飞书端
        A --> D[Webhook通知]
        E[飞书开放API] --> A
    end
    
    subgraph 同步服务
        B --> F[消息队列]
        B --> G[同步日志]
        B --> H[冲突处理器]
    end
    
    subgraph 数据库端
        C --> I[数据库触发器]
        J[同步队列表] --> C
    end
```

## 核心流程

### 1. 飞书表格 → 数据库同步流程

```mermaid
sequenceDiagram
    participant U as 用户
    participant F as 飞书表格
    participant W as Webhook
    participant S as 同步服务
    participant Q as 消息队列
    participant D as 数据库
    participant L as 同步日志

    U->>F: 修改表格数据
    F->>W: 触发数据变更事件
    W->>S: POST /webhook/feishu<br/>{event_type, record_id, fields}
    S->>S: 验证请求签名
    S->>Q: 推送同步任务
    Q->>S: 消费同步任务
    S->>L: 检查是否需要同步<br/>(防止循环)
    alt 需要同步
        S->>D: UPDATE record<br/>SET fields, _sync_source='feishu'
        D-->>S: 更新成功
        S->>L: 记录同步日志
    else 不需要同步
        S->>L: 记录跳过原因
    end
    S-->>W: 返回 200 OK
```

### 2. 数据库 → 飞书表格同步流程

```mermaid
sequenceDiagram
    participant A as 应用程序
    participant D as 数据库
    participant T as 触发器
    participant Q as 同步队列表
    participant S as 同步服务
    participant F as 飞书API
    participant B as 飞书表格
    participant L as 同步日志

    A->>D: UPDATE users SET age=30
    D->>T: 触发 AFTER UPDATE
    T->>T: 检查 _sync_source
    alt 非飞书来源
        T->>Q: INSERT INTO sync_queue
        S->>Q: 定时轮询队列
        S->>L: 检查是否需要同步
        S->>F: PUT /tables/{table_id}/records/{record_id}
        F->>B: 更新表格数据
        B-->>F: 返回成功
        F-->>S: 返回结果
        S->>Q: 标记任务完成
        S->>L: 记录同步日志
    else 飞书来源
        T->>T: 跳过同步
    end
```

## 关键组件实现

### 1. 同步服务器

```python
# sync_server.py
from flask import Flask, request, jsonify
from datetime import datetime
import hashlib
import json
import redis
from rq import Queue

app = Flask(__name__)
redis_conn = redis.Redis()
task_queue = Queue(connection=redis_conn)

class SyncServer:
    def __init__(self):
        self.feishu_client = FeishuClient()
        self.db = Database()
        self.sync_logger = SyncLogger()
    
    @app.route('/webhook/feishu', methods=['POST'])
    def handle_feishu_webhook(self):
        """接收飞书表格变更通知"""
        # 1. 验证签名
        if not self.verify_signature(request):
            return jsonify({'error': 'Invalid signature'}), 403
        
        # 2. 解析事件
        event = request.json
        event_type = event.get('event_type')
        
        # 3. 推送到队列异步处理
        job = task_queue.enqueue(
            self.process_feishu_event,
            event_type=event_type,
            data=event.get('data')
        )
        
        return jsonify({
            'status': 'queued',
            'job_id': job.id
        }), 200
    
    def process_feishu_event(self, event_type, data):
        """处理飞书事件"""
        # 生成同步哈希，用于去重
        sync_hash = self.generate_sync_hash(data)
        
        # 检查是否需要同步
        if self.sync_logger.is_recently_synced(sync_hash, source='feishu'):
            return {'status': 'skipped', 'reason': 'Recently synced'}
        
        # 执行同步
        try:
            if event_type == 'record.created':
                self.sync_create_to_db(data)
            elif event_type == 'record.updated':
                self.sync_update_to_db(data)
            elif event_type == 'record.deleted':
                self.sync_delete_to_db(data)
            
            # 记录同步日志
            self.sync_logger.log_sync(
                sync_hash=sync_hash,
                source='feishu',
                target='database',
                status='success'
            )
            
        except Exception as e:
            self.sync_logger.log_sync(
                sync_hash=sync_hash,
                source='feishu',
                target='database',
                status='failed',
                error=str(e)
            )
            raise
```

### 2. 防循环同步机制

```python
# sync_logger.py
import time
from datetime import datetime, timedelta

class SyncLogger:
    def __init__(self, db):
        self.db = db
        self.cache = {}  # 内存缓存
        
    def is_recently_synced(self, sync_hash, source, threshold_seconds=5):
        """检查是否最近同步过相同数据"""
        # 先检查内存缓存
        cache_key = f"{sync_hash}:{source}"
        if cache_key in self.cache:
            if time.time() - self.cache[cache_key] < threshold_seconds:
                return True
        
        # 再检查数据库
        recent_sync = self.db.query("""
            SELECT COUNT(*) as count FROM sync_log
            WHERE sync_hash = %s
            AND source = %s
            AND created_at > %s
        """, (
            sync_hash,
            source,
            datetime.now() - timedelta(seconds=threshold_seconds)
        ))
        
        return recent_sync[0]['count'] > 0
    
    def log_sync(self, sync_hash, source, target, status, error=None):
        """记录同步日志"""
        # 更新内存缓存
        cache_key = f"{sync_hash}:{source}"
        self.cache[cache_key] = time.time()
        
        # 写入数据库
        self.db.execute("""
            INSERT INTO sync_log 
            (sync_hash, source, target, status, error, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sync_hash, source, target, status, error, datetime.now()))
```

### 3. 数据库触发器

```sql
-- 创建同步队列表
CREATE TABLE sync_queue (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    table_name VARCHAR(50) NOT NULL,
    action VARCHAR(20) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    old_data JSON,
    new_data JSON,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    INDEX idx_status (status),
    INDEX idx_created (created_at)
);

-- 创建触发器
DELIMITER $$

CREATE TRIGGER users_after_insert
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    -- 只同步非飞书来源的数据
    IF NEW._sync_source IS NULL OR NEW._sync_source != 'feishu' THEN
        INSERT INTO sync_queue (
            table_name, 
            action, 
            record_id, 
            new_data
        ) VALUES (
            'users',
            'insert',
            NEW.id,
            JSON_OBJECT(
                'id', NEW.id,
                'name', NEW.name,
                'age', NEW.age,
                'email', NEW.email
            )
        );
    END IF;
END$$

CREATE TRIGGER users_after_update
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    IF NEW._sync_source IS NULL OR NEW._sync_source != 'feishu' THEN
        INSERT INTO sync_queue (
            table_name, 
            action, 
            record_id,
            old_data,
            new_data
        ) VALUES (
            'users',
            'update',
            NEW.id,
            JSON_OBJECT(
                'id', OLD.id,
                'name', OLD.name,
                'age', OLD.age,
                'email', OLD.email
            ),
            JSON_OBJECT(
                'id', NEW.id,
                'name', NEW.name,
                'age', NEW.age,
                'email', NEW.email
            )
        );
    END IF;
END$$

DELIMITER ;
```

### 4. 数据库同步到飞书

```python
# db_to_feishu_sync.py
import time
from datetime import datetime

class DatabaseToFeishuSync:
    def __init__(self, db, feishu_client, sync_logger):
        self.db = db
        self.feishu = feishu_client
        self.logger = sync_logger
        
    def start_sync_worker(self):
        """启动同步工作线程"""
        while True:
            try:
                self.process_sync_queue()
                time.sleep(1)  # 每秒检查一次
            except Exception as e:
                print(f"Sync error: {e}")
                time.sleep(5)
    
    def process_sync_queue(self):
        """处理同步队列"""
        # 获取待处理任务
        tasks = self.db.query("""
            SELECT * FROM sync_queue 
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 10
        """)
        
        for task in tasks:
            try:
                # 处理单个任务
                self.process_task(task)
                
                # 标记完成
                self.db.execute("""
                    UPDATE sync_queue 
                    SET status = 'completed', processed_at = %s
                    WHERE id = %s
                """, (datetime.now(), task['id']))
                
            except Exception as e:
                # 标记失败
                self.db.execute("""
                    UPDATE sync_queue 
                    SET status = 'failed', error = %s
                    WHERE id = %s
                """, (str(e), task['id']))
    
    def process_task(self, task):
        """处理单个同步任务"""
        # 生成同步哈希
        sync_hash = self.generate_sync_hash(task['new_data'])
        
        # 检查是否需要同步
        if self.logger.is_recently_synced(sync_hash, source='database'):
            return
        
        # 根据操作类型同步
        if task['action'] == 'insert':
            self.sync_insert_to_feishu(task)
        elif task['action'] == 'update':
            self.sync_update_to_feishu(task)
        elif task['action'] == 'delete':
            self.sync_delete_to_feishu(task)
        
        # 记录同步日志
        self.logger.log_sync(
            sync_hash=sync_hash,
            source='database',
            target='feishu',
            status='success'
        )
```

## 冲突处理策略

```python
# conflict_resolver.py
class ConflictResolver:
    def __init__(self, strategy='last_write_wins'):
        self.strategy = strategy
    
    def resolve(self, feishu_data, db_data):
        """解决数据冲突"""
        if self.strategy == 'last_write_wins':
            # 最后写入优先
            return self._last_write_wins(feishu_data, db_data)
        elif self.strategy == 'feishu_priority':
            # 飞书优先
            return feishu_data
        elif self.strategy == 'db_priority':
            # 数据库优先
            return db_data
        elif self.strategy == 'manual':
            # 人工处理
            return self._create_conflict_record(feishu_data, db_data)
    
    def _last_write_wins(self, feishu_data, db_data):
        """时间戳比较"""
        if feishu_data['updated_at'] > db_data['updated_at']:
            return feishu_data
        else:
            return db_data
    
    def _create_conflict_record(self, feishu_data, db_data):
        """创建冲突记录，等待人工处理"""
        conflict = {
            'record_id': feishu_data['id'],
            'feishu_data': feishu_data,
            'db_data': db_data,
            'created_at': datetime.now(),
            'status': 'pending'
        }
        # 保存到冲突表
        self.db.save_conflict(conflict)
        raise ConflictException("Manual resolution required")
```

## 部署架构

```mermaid
graph TB
    subgraph 飞书云
        A[飞书多维表格]
    end
    
    subgraph 云服务器
        B[Nginx<br/>反向代理]
        C[同步服务<br/>Flask App]
        D[消息队列<br/>Redis + RQ]
        E[定时任务<br/>Celery]
    end
    
    subgraph 数据库服务器
        F[MySQL/PostgreSQL]
        G[触发器]
        H[同步队列表]
    end
    
    A -->|Webhook| B
    B --> C
    C <--> D
    C <--> F
    E --> C
    F --> G
    G --> H
    H --> E
```

## 监控和运维

### 1. 监控指标

```python
# monitoring.py
class SyncMonitor:
    def __init__(self):
        self.metrics = {
            'sync_success_count': 0,
            'sync_failure_count': 0,
            'sync_skip_count': 0,
            'avg_sync_time': 0,
            'queue_size': 0
        }
    
    def record_sync(self, source, target, status, duration):
        """记录同步指标"""
        if status == 'success':
            self.metrics['sync_success_count'] += 1
        elif status == 'failed':
            self.metrics['sync_failure_count'] += 1
        elif status == 'skipped':
            self.metrics['sync_skip_count'] += 1
        
        # 更新平均同步时间
        self.update_avg_sync_time(duration)
    
    def get_queue_size(self):
        """获取队列大小"""
        return self.db.query(
            "SELECT COUNT(*) FROM sync_queue WHERE status = 'pending'"
        )[0]['count']
    
    def alert_if_needed(self):
        """检查是否需要告警"""
        # 队列积压告警
        if self.get_queue_size() > 1000:
            self.send_alert("Queue size exceeds 1000")
        
        # 失败率告警
        total = self.metrics['sync_success_count'] + self.metrics['sync_failure_count']
        if total > 0:
            failure_rate = self.metrics['sync_failure_count'] / total
            if failure_rate > 0.1:  # 10%失败率
                self.send_alert(f"High failure rate: {failure_rate:.2%}")
```

### 2. 日志查询

```sql
-- 查询最近的同步失败
SELECT * FROM sync_log 
WHERE status = 'failed' 
AND created_at > NOW() - INTERVAL 1 HOUR
ORDER BY created_at DESC;

-- 查询同步延迟
SELECT 
    AVG(TIMESTAMPDIFF(SECOND, created_at, processed_at)) as avg_delay,
    MAX(TIMESTAMPDIFF(SECOND, created_at, processed_at)) as max_delay
FROM sync_queue
WHERE status = 'completed'
AND created_at > NOW() - INTERVAL 1 HOUR;

-- 查询冲突记录
SELECT * FROM sync_conflicts
WHERE status = 'pending'
ORDER BY created_at DESC;
```

## 注意事项

1. **API 限流**：飞书 API 有调用频率限制，需要实现限流机制
2. **数据安全**：敏感数据需要加密传输和存储
3. **高可用**：建议部署多个同步服务实例，使用负载均衡
4. **数据备份**：定期备份同步日志和队列数据
5. **版本兼容**：飞书 API 升级时需要注意兼容性

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量
export FEISHU_APP_ID="your_app_id"
export FEISHU_APP_SECRET="your_app_secret"
export DATABASE_URL="mysql://user:pass@localhost/db"

# 3. 初始化数据库
python init_db.py

# 4. 启动同步服务
python sync_server.py

# 5. 启动队列处理器
python queue_worker.py
```

## 总结

这个双向同步方案通过 Webhook、消息队列、数据库触发器等技术，实现了飞书表格与数据库的实时同步。关键在于防止循环同步和冲突处理，通过同步日志和标记机制可以有效解决这些问题。