"""
飞书表格变更检测器
"""
import json
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime
import redis
from loguru import logger

from .client import FeishuClient


class ChangeRecord:
    """变更记录"""
    
    def __init__(self, record_id: str, action: str, 
                 old_data: Optional[Dict] = None, 
                 new_data: Optional[Dict] = None):
        self.record_id = record_id
        self.action = action  # 'insert', 'update', 'delete'
        self.old_data = old_data
        self.new_data = new_data
        self.timestamp = datetime.now()
        self.hash = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'record_id': self.record_id,
            'action': self.action,
            'old_data': self.old_data,
            'new_data': self.new_data,
            'timestamp': self.timestamp.isoformat(),
            'hash': self.hash
        }


class ChangeDetector:
    """飞书表格变更检测器"""
    
    def __init__(self, feishu_client: FeishuClient, 
                 redis_client: Optional[redis.Redis] = None):
        self.feishu = feishu_client
        self.redis = redis_client
        self.use_redis = redis_client is not None
        
        # 内存快照缓存（当Redis不可用时使用）
        self.memory_snapshots: Dict[str, Dict[str, Any]] = {}
    
    def _get_snapshot_key(self, database: str, table: str) -> str:
        """获取快照键名"""
        return f"feishu_snapshot:{database}:{table}"
    
    def _get_snapshot(self, database: str, table: str) -> Dict[str, Dict[str, Any]]:
        """获取表快照"""
        key = self._get_snapshot_key(database, table)
        
        if self.use_redis:
            snapshot_data = self.redis.get(key)
            if snapshot_data:
                return json.loads(snapshot_data)
            return {}
        else:
            return self.memory_snapshots.get(key, {})
    
    def _save_snapshot(self, database: str, table: str, 
                      snapshot: Dict[str, Dict[str, Any]]) -> None:
        """保存表快照"""
        key = self._get_snapshot_key(database, table)
        
        if self.use_redis:
            self.redis.set(key, json.dumps(snapshot), ex=86400)  # 24小时过期
        else:
            self.memory_snapshots[key] = snapshot
    
    def detect_changes(self, database: str, table: str) -> List[ChangeRecord]:
        """检测表格变更"""
        logger.debug(f"Detecting changes for {database}.{table}")
        changes = []
        
        try:
            # 获取当前所有记录
            current_records = self.feishu.read_all_records(database, table)
            
            # 获取上次快照
            last_snapshot = self._get_snapshot(database, table)
            
            # 构建当前快照
            current_snapshot = {}
            current_ids = set()
            
            # 检测新增和修改的记录
            for record in current_records:
                record_id = record.get('id')
                if not record_id:
                    continue
                
                current_ids.add(record_id)
                
                # 计算记录哈希
                record_hash = self.feishu.calculate_record_hash(record)
                current_snapshot[record_id] = {
                    'data': record,
                    'hash': record_hash
                }
                
                # 检查是否有变更
                if record_id not in last_snapshot:
                    # 新增记录
                    change = ChangeRecord(
                        record_id=record_id,
                        action='insert',
                        new_data=record
                    )
                    change.hash = record_hash
                    changes.append(change)
                    logger.debug(f"Detected new record: {record_id}")
                    
                elif last_snapshot[record_id]['hash'] != record_hash:
                    # 修改记录
                    change = ChangeRecord(
                        record_id=record_id,
                        action='update',
                        old_data=last_snapshot[record_id]['data'],
                        new_data=record
                    )
                    change.hash = record_hash
                    changes.append(change)
                    logger.debug(f"Detected updated record: {record_id}")
            
            # 检测删除的记录
            last_ids = set(last_snapshot.keys())
            deleted_ids = last_ids - current_ids
            
            for record_id in deleted_ids:
                change = ChangeRecord(
                    record_id=record_id,
                    action='delete',
                    old_data=last_snapshot[record_id]['data']
                )
                changes.append(change)
                logger.debug(f"Detected deleted record: {record_id}")
            
            # 保存新快照
            self._save_snapshot(database, table, current_snapshot)
            
            logger.info(f"Detected {len(changes)} changes in {database}.{table}")
            return changes
            
        except Exception as e:
            logger.error(f"Error detecting changes: {e}")
            raise
    
    def reset_snapshot(self, database: str, table: str) -> None:
        """重置表快照"""
        key = self._get_snapshot_key(database, table)
        
        if self.use_redis:
            self.redis.delete(key)
        else:
            self.memory_snapshots.pop(key, None)
        
        logger.info(f"Reset snapshot for {database}.{table}")
    
    def get_snapshot_info(self, database: str, table: str) -> Dict[str, Any]:
        """获取快照信息"""
        snapshot = self._get_snapshot(database, table)
        
        return {
            'database': database,
            'table': table,
            'record_count': len(snapshot),
            'record_ids': list(snapshot.keys())
        }
    
    def batch_detect_changes(self, table_mapping: Dict[str, str]) -> Dict[str, List[ChangeRecord]]:
        """批量检测多个表的变更"""
        all_changes = {}
        
        for feishu_table, db_table in table_mapping.items():
            try:
                database, table = feishu_table.split(':')
                changes = self.detect_changes(database, table)
                all_changes[feishu_table] = changes
            except Exception as e:
                logger.error(f"Error detecting changes for {feishu_table}: {e}")
                all_changes[feishu_table] = []
        
        return all_changes