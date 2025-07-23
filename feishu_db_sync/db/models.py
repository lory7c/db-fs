"""
数据库模型定义
"""
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
import json


class SyncStatus(Enum):
    """同步状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class SyncDirection(Enum):
    """同步方向枚举"""
    FEISHU_TO_DB = "feishu_to_db"
    DB_TO_FEISHU = "db_to_feishu"


class SyncAction(Enum):
    """同步动作枚举"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class SyncQueue:
    """同步队列模型"""
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.table_name = kwargs.get('table_name')
        self.record_id = kwargs.get('record_id')
        self.action = kwargs.get('action')
        self.old_data = kwargs.get('old_data')
        self.new_data = kwargs.get('new_data')
        self.sync_hash = kwargs.get('sync_hash')
        self.sync_source = kwargs.get('sync_source', 'database')
        self.created_at = kwargs.get('created_at', datetime.now())
        self.processed_at = kwargs.get('processed_at')
        self.status = kwargs.get('status', SyncStatus.PENDING.value)
        self.retry_count = kwargs.get('retry_count', 0)
        self.error_message = kwargs.get('error_message')
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'table_name': self.table_name,
            'record_id': self.record_id,
            'action': self.action,
            'old_data': self.old_data,
            'new_data': self.new_data,
            'sync_hash': self.sync_hash,
            'sync_source': self.sync_source,
            'status': self.status,
            'retry_count': self.retry_count,
            'error_message': self.error_message
        }
    
    @staticmethod
    def from_db_record(record: Dict[str, Any]) -> 'SyncQueue':
        """从数据库记录创建对象"""
        # 解析 JSON 字段
        if isinstance(record.get('old_data'), str):
            record['old_data'] = json.loads(record['old_data']) if record['old_data'] else None
        if isinstance(record.get('new_data'), str):
            record['new_data'] = json.loads(record['new_data']) if record['new_data'] else None
        
        return SyncQueue(**record)


class SyncLog:
    """同步日志模型"""
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.sync_id = kwargs.get('sync_id')
        self.table_name = kwargs.get('table_name')
        self.record_id = kwargs.get('record_id')
        self.direction = kwargs.get('direction')
        self.sync_hash = kwargs.get('sync_hash')
        self.status = kwargs.get('status')
        self.error_message = kwargs.get('error_message')
        self.created_at = kwargs.get('created_at', datetime.now())
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'sync_id': self.sync_id,
            'table_name': self.table_name,
            'record_id': self.record_id,
            'direction': self.direction,
            'sync_hash': self.sync_hash,
            'status': self.status,
            'error_message': self.error_message
        }
    
    @staticmethod
    def generate_sync_id(table_name: str, record_id: str, sync_hash: str) -> str:
        """生成同步ID"""
        return f"{table_name}_{record_id}_{sync_hash}"


class IdMapping:
    """ID映射模型"""
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.table_name = kwargs.get('table_name')
        self.db_id = kwargs.get('db_id')
        self.feishu_id = kwargs.get('feishu_id')
        self.created_at = kwargs.get('created_at')
        self.updated_at = kwargs.get('updated_at')
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'table_name': self.table_name,
            'db_id': self.db_id,
            'feishu_id': self.feishu_id
        }