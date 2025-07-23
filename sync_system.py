"""
飞书多维表格与数据库双向实时同步系统
"""
import asyncio
import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import threading
import queue

from feishu_bitable_db.db.db import DBImpl
from loguru import logger
import redis
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker


class SyncDirection(Enum):
    FEISHU_TO_DB = "feishu_to_db"
    DB_TO_FEISHU = "db_to_feishu"


@dataclass
class SyncRecord:
    """同步记录"""
    record_id: str
    table_name: str
    data: Dict[str, Any]
    source: str
    timestamp: datetime
    hash: str
    
    @staticmethod
    def calculate_hash(data: Dict[str, Any]) -> str:
        """计算数据哈希值"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()


class SyncLock:
    """同步锁，防止循环同步"""
    def __init__(self, redis_client):
        self.redis = redis_client
        self.lock_ttl = 10  # 锁过期时间（秒）
    
    def acquire(self, key: str) -> bool:
        """获取锁"""
        return self.redis.set(
            f"sync_lock:{key}", 
            "1", 
            nx=True, 
            ex=self.lock_ttl
        )
    
    def release(self, key: str):
        """释放锁"""
        self.redis.delete(f"sync_lock:{key}")
    
    def is_locked(self, key: str) -> bool:
        """检查是否被锁定"""
        return self.redis.exists(f"sync_lock:{key}")


class FeishuChangeDetector:
    """飞书表格变更检测器"""
    def __init__(self, feishu_db: DBImpl, redis_client):
        self.feishu_db = feishu_db
        self.redis = redis_client
        self.snapshot_prefix = "feishu_snapshot:"
        
    async def detect_changes(self, database: str, table: str) -> List[SyncRecord]:
        """检测表格变更"""
        changes = []
        
        # 获取当前所有记录
        current_records = self.feishu_db.read(database, table, [])
        
        # 获取上次快照
        snapshot_key = f"{self.snapshot_prefix}{database}:{table}"
        last_snapshot = self.redis.get(snapshot_key)
        last_records = json.loads(last_snapshot) if last_snapshot else {}
        
        # 检测变更
        current_ids = set()
        for record in current_records:
            record_id = record['id']
            current_ids.add(record_id)
            
            # 计算数据哈希
            current_hash = SyncRecord.calculate_hash(record)
            last_hash = last_records.get(record_id, {}).get('hash')
            
            if current_hash != last_hash:
                # 检测到变更
                changes.append(SyncRecord(
                    record_id=record_id,
                    table_name=table,
                    data=record,
                    source='feishu',
                    timestamp=datetime.now(),
                    hash=current_hash
                ))
        
        # 检测删除的记录
        for record_id in set(last_records.keys()) - current_ids:
            changes.append(SyncRecord(
                record_id=record_id,
                table_name=table,
                data={'_deleted': True},
                source='feishu',
                timestamp=datetime.now(),
                hash=''
            ))
        
        # 更新快照
        new_snapshot = {
            record['id']: {
                'data': record,
                'hash': SyncRecord.calculate_hash(record)
            }
            for record in current_records
        }
        self.redis.set(snapshot_key, json.dumps(new_snapshot))
        
        return changes


class DatabaseChangeCapture:
    """数据库变更捕获器"""
    def __init__(self, db_url: str, change_queue: queue.Queue):
        self.engine = create_engine(db_url)
        self.change_queue = change_queue
        self._setup_listeners()
    
    def _setup_listeners(self):
        """设置数据库事件监听器"""
        # 使用 SQLAlchemy 事件系统
        @event.listens_for(self.engine, "after_cursor_execute")
        def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            # 解析 SQL 语句，提取变更信息
            if statement.upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                self._process_change(statement, parameters)
    
    def _process_change(self, statement: str, parameters):
        """处理数据库变更"""
        # 简化示例，实际需要更复杂的 SQL 解析
        change_record = SyncRecord(
            record_id=str(parameters.get('id', '')),
            table_name=self._extract_table_name(statement),
            data=dict(parameters),
            source='database',
            timestamp=datetime.now(),
            hash=SyncRecord.calculate_hash(dict(parameters))
        )
        self.change_queue.put(change_record)
    
    def _extract_table_name(self, statement: str) -> str:
        """从 SQL 语句提取表名"""
        # 简化实现，实际需要 SQL 解析器
        parts = statement.split()
        for i, part in enumerate(parts):
            if part.upper() in ('FROM', 'INTO', 'UPDATE'):
                if i + 1 < len(parts):
                    return parts[i + 1].strip('`"')
        return 'unknown'


class ConflictResolver:
    """冲突解决器"""
    def __init__(self, strategy: str = "last_write_wins"):
        self.strategy = strategy
    
    def resolve(self, record1: SyncRecord, record2: SyncRecord) -> SyncRecord:
        """解决冲突"""
        if self.strategy == "last_write_wins":
            return record1 if record1.timestamp > record2.timestamp else record2
        elif self.strategy == "feishu_priority":
            return record1 if record1.source == "feishu" else record2
        elif self.strategy == "database_priority":
            return record1 if record1.source == "database" else record2
        else:
            raise ValueError(f"Unknown conflict resolution strategy: {self.strategy}")


class BiDirectionalSyncService:
    """双向同步服务主类"""
    def __init__(self, feishu_config: Dict, db_config: Dict):
        # 初始化飞书客户端
        self.feishu_db = DBImpl(
            feishu_config['app_id'],
            feishu_config['app_secret']
        )
        
        # 初始化 Redis（用于分布式锁和快照）
        self.redis = redis.Redis(
            host=feishu_config.get('redis_host', 'localhost'),
            port=feishu_config.get('redis_port', 6379),
            decode_responses=True
        )
        
        # 初始化组件
        self.sync_lock = SyncLock(self.redis)
        self.feishu_detector = FeishuChangeDetector(self.feishu_db, self.redis)
        self.conflict_resolver = ConflictResolver()
        
        # 数据库变更队列
        self.db_change_queue = queue.Queue()
        self.db_capture = DatabaseChangeCapture(db_config['url'], self.db_change_queue)
        
        # 同步配置
        self.sync_interval = feishu_config.get('sync_interval', 10)  # 轮询间隔（秒）
        self.tables_mapping = feishu_config.get('tables_mapping', {})
        
    async def start(self):
        """启动双向同步服务"""
        logger.info("Starting bi-directional sync service...")
        
        # 启动飞书轮询任务
        feishu_task = asyncio.create_task(self._feishu_sync_loop())
        
        # 启动数据库同步任务
        db_task = asyncio.create_task(self._database_sync_loop())
        
        # 等待任务完成
        await asyncio.gather(feishu_task, db_task)
    
    async def _feishu_sync_loop(self):
        """飞书同步循环"""
        while True:
            try:
                for feishu_db, feishu_table in self.tables_mapping.items():
                    # 检测飞书表格变更
                    changes = await self.feishu_detector.detect_changes(
                        feishu_db.split(':')[0],
                        feishu_db.split(':')[1]
                    )
                    
                    for change in changes:
                        await self._sync_to_database(change)
                
            except Exception as e:
                logger.error(f"Feishu sync error: {e}")
            
            await asyncio.sleep(self.sync_interval)
    
    async def _database_sync_loop(self):
        """数据库同步循环"""
        while True:
            try:
                # 从队列获取变更（非阻塞）
                try:
                    change = self.db_change_queue.get_nowait()
                    await self._sync_to_feishu(change)
                except queue.Empty:
                    pass
                
            except Exception as e:
                logger.error(f"Database sync error: {e}")
            
            await asyncio.sleep(0.1)  # 短暂休眠避免 CPU 占用过高
    
    async def _sync_to_database(self, change: SyncRecord):
        """同步到数据库"""
        # 生成锁键
        lock_key = f"{change.table_name}:{change.record_id}:{change.hash}"
        
        # 尝试获取锁
        if not self.sync_lock.acquire(lock_key):
            logger.debug(f"Skip sync to DB, record locked: {change.record_id}")
            return
        
        try:
            # 执行数据库更新
            logger.info(f"Syncing to database: {change.record_id}")
            # TODO: 实际的数据库更新逻辑
            
        finally:
            self.sync_lock.release(lock_key)
    
    async def _sync_to_feishu(self, change: SyncRecord):
        """同步到飞书"""
        # 生成锁键
        lock_key = f"{change.table_name}:{change.record_id}:{change.hash}"
        
        # 尝试获取锁
        if not self.sync_lock.acquire(lock_key):
            logger.debug(f"Skip sync to Feishu, record locked: {change.record_id}")
            return
        
        try:
            # 找到对应的飞书表格
            for feishu_key, db_table in self.tables_mapping.items():
                if db_table == change.table_name:
                    feishu_db, feishu_table = feishu_key.split(':')
                    
                    # 执行飞书更新
                    logger.info(f"Syncing to Feishu: {change.record_id}")
                    
                    if change.data.get('_deleted'):
                        self.feishu_db.delete(feishu_db, feishu_table, change.record_id)
                    else:
                        self.feishu_db.update(feishu_db, feishu_table, change.record_id, change.data)
                    
                    break
        
        finally:
            self.sync_lock.release(lock_key)


# 使用示例
if __name__ == "__main__":
    config = {
        'feishu_config': {
            'app_id': 'your_app_id',
            'app_secret': 'your_app_secret',
            'redis_host': 'localhost',
            'redis_port': 6379,
            'sync_interval': 10,  # 10秒轮询一次
            'tables_mapping': {
                'MyDB:users': 'users',  # 飞书表格:数据库表
                'MyDB:orders': 'orders'
            }
        },
        'db_config': {
            'url': 'mysql+pymysql://user:pass@localhost/mydb'
        }
    }
    
    service = BiDirectionalSyncService(
        feishu_config=config['feishu_config'],
        db_config=config['db_config']
    )
    
    # 运行服务
    asyncio.run(service.start())