"""
同步队列处理器
"""
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from loguru import logger

from .database import Database
from .models import SyncQueue, SyncLog, SyncStatus, SyncDirection


class QueueProcessor:
    """同步队列处理器"""
    
    def __init__(self, database: Database):
        self.db = database
    
    def add_to_queue(self, table_name: str, record_id: str, 
                    action: str, old_data: Optional[Dict] = None,
                    new_data: Optional[Dict] = None, 
                    sync_hash: Optional[str] = None) -> int:
        """添加到同步队列"""
        data = {
            'table_name': table_name,
            'record_id': record_id,
            'action': action,
            'old_data': json.dumps(old_data) if old_data else None,
            'new_data': json.dumps(new_data) if new_data else None,
            'sync_hash': sync_hash,
            'sync_source': 'database'
        }
        
        return self.db.insert('sync_queue', data)
    
    def get_pending_items(self, limit: int = 50) -> List[SyncQueue]:
        """获取待处理的队列项"""
        sql = """
            SELECT * FROM sync_queue
            WHERE status = %s AND retry_count < 3
            ORDER BY created_at ASC
            LIMIT %s
        """
        
        records = self.db.query(sql, (SyncStatus.PENDING.value, limit))
        return [SyncQueue.from_db_record(record) for record in records]
    
    def mark_processing(self, queue_id: int) -> None:
        """标记为处理中"""
        self.db.update(
            'sync_queue',
            {'status': SyncStatus.PROCESSING.value},
            {'id': queue_id}
        )
    
    def mark_completed(self, queue_id: int) -> None:
        """标记为已完成"""
        self.db.update(
            'sync_queue',
            {
                'status': SyncStatus.COMPLETED.value,
                'processed_at': datetime.now()
            },
            {'id': queue_id}
        )
    
    def mark_failed(self, queue_id: int, error_message: str) -> None:
        """标记为失败"""
        # 获取当前重试次数
        item = self.db.query_one("SELECT retry_count FROM sync_queue WHERE id = %s", (queue_id,))
        retry_count = item['retry_count'] + 1 if item else 1
        
        self.db.update(
            'sync_queue',
            {
                'status': SyncStatus.FAILED.value if retry_count >= 3 else SyncStatus.PENDING.value,
                'retry_count': retry_count,
                'error_message': error_message
            },
            {'id': queue_id}
        )
    
    def check_sync_loop(self, sync_hash: str, direction: str, 
                       window_seconds: int = 10) -> bool:
        """检查是否存在同步循环"""
        sql = """
            SELECT COUNT(*) as count FROM sync_log
            WHERE sync_hash = %s
            AND direction != %s
            AND created_at > %s
            AND status = %s
        """
        
        time_threshold = datetime.now() - timedelta(seconds=window_seconds)
        result = self.db.query_one(
            sql, 
            (sync_hash, direction, time_threshold, 'completed')
        )
        
        return result['count'] > 0
    
    def log_sync(self, table_name: str, record_id: str, 
                direction: str, sync_hash: str, 
                status: str, error_message: Optional[str] = None) -> None:
        """记录同步日志"""
        sync_id = SyncLog.generate_sync_id(table_name, record_id, sync_hash)
        
        data = {
            'sync_id': sync_id,
            'table_name': table_name,
            'record_id': record_id,
            'direction': direction,
            'sync_hash': sync_hash,
            'status': status,
            'error_message': error_message
        }
        
        # 使用 UPSERT 避免重复
        self.db.execute("""
            INSERT INTO sync_log (sync_id, table_name, record_id, direction, sync_hash, status, error_message)
            VALUES (%(sync_id)s, %(table_name)s, %(record_id)s, %(direction)s, %(sync_hash)s, %(status)s, %(error_message)s)
            ON DUPLICATE KEY UPDATE status = VALUES(status), error_message = VALUES(error_message)
        """, data)
    
    def cleanup_old_records(self, days: int = 7) -> None:
        """清理旧记录"""
        time_threshold = datetime.now() - timedelta(days=days)
        
        # 清理已完成的队列记录
        deleted_queue = self.db.execute("""
            DELETE FROM sync_queue 
            WHERE status = %s AND processed_at < %s
        """, (SyncStatus.COMPLETED.value, time_threshold))
        
        # 清理同步日志
        deleted_logs = self.db.execute("""
            DELETE FROM sync_log 
            WHERE created_at < %s
        """, (time_threshold,))
        
        logger.info(f"Cleaned up {deleted_queue} queue records and {deleted_logs} log records")
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        sql = """
            SELECT 
                status,
                COUNT(*) as count,
                MIN(created_at) as oldest,
                MAX(created_at) as newest
            FROM sync_queue
            GROUP BY status
        """
        
        results = self.db.query(sql)
        
        stats = {
            'total': 0,
            'by_status': {},
            'oldest_pending': None
        }
        
        for row in results:
            stats['total'] += row['count']
            stats['by_status'][row['status']] = row['count']
            
            if row['status'] == SyncStatus.PENDING.value and row['oldest']:
                stats['oldest_pending'] = row['oldest']
        
        return stats
    
    def save_id_mapping(self, table_name: str, db_id: str, feishu_id: str) -> None:
        """保存ID映射关系"""
        self.db.upsert(
            'id_mapping',
            {
                'table_name': table_name,
                'db_id': db_id,
                'feishu_id': feishu_id
            },
            ['table_name', 'db_id']
        )
    
    def get_feishu_id(self, table_name: str, db_id: str) -> Optional[str]:
        """根据数据库ID获取飞书ID"""
        result = self.db.query_one(
            "SELECT feishu_id FROM id_mapping WHERE table_name = %s AND db_id = %s",
            (table_name, db_id)
        )
        return result['feishu_id'] if result else None
    
    def get_db_id(self, table_name: str, feishu_id: str) -> Optional[str]:
        """根据飞书ID获取数据库ID"""
        result = self.db.query_one(
            "SELECT db_id FROM id_mapping WHERE table_name = %s AND feishu_id = %s",
            (table_name, feishu_id)
        )
        return result['db_id'] if result else None