"""
同步工作器
"""
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from loguru import logger

from ..feishu.client import FeishuClient
from ..feishu.change_detector import ChangeRecord
from ..db.database import Database
from ..db.queue_processor import QueueProcessor
from ..db.models import SyncQueue, SyncDirection, SyncAction
from .field_mapper import FieldMapper


class SyncWorker:
    """同步工作器，处理具体的同步任务"""
    
    def __init__(self, feishu_client: FeishuClient, 
                 database: Database,
                 queue_processor: QueueProcessor,
                 field_mapper: FieldMapper):
        self.feishu = feishu_client
        self.db = database
        self.queue = queue_processor
        self.mapper = field_mapper
    
    def sync_feishu_to_db(self, feishu_table: str, db_table: str,
                         change: ChangeRecord) -> bool:
        """同步飞书变更到数据库"""
        try:
            # 检查是否是循环同步
            if change.hash and self.queue.check_sync_loop(
                change.hash, SyncDirection.FEISHU_TO_DB.value
            ):
                logger.debug(f"Skip circular sync for record {change.record_id}")
                return True
            
            # 根据动作类型处理
            if change.action == 'insert':
                self._insert_to_db(db_table, change.new_data, change.record_id)
            elif change.action == 'update':
                self._update_in_db(db_table, change.new_data, change.record_id)
            elif change.action == 'delete':
                self._delete_from_db(db_table, change.record_id)
            
            # 记录同步日志
            self.queue.log_sync(
                table_name=db_table,
                record_id=change.record_id,
                direction=SyncDirection.FEISHU_TO_DB.value,
                sync_hash=change.hash,
                status='completed'
            )
            
            logger.info(f"Synced {change.action} from Feishu to DB: {change.record_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync Feishu to DB: {e}")
            # 记录失败日志
            self.queue.log_sync(
                table_name=db_table,
                record_id=change.record_id,
                direction=SyncDirection.FEISHU_TO_DB.value,
                sync_hash=change.hash,
                status='failed',
                error_message=str(e)
            )
            return False
    
    def sync_db_to_feishu(self, queue_item: SyncQueue,
                         feishu_db: str, feishu_table: str) -> bool:
        """同步数据库变更到飞书"""
        try:
            # 检查是否是循环同步
            if queue_item.sync_hash and self.queue.check_sync_loop(
                queue_item.sync_hash, SyncDirection.DB_TO_FEISHU.value
            ):
                logger.debug(f"Skip circular sync for record {queue_item.record_id}")
                self.queue.mark_completed(queue_item.id)
                return True
            
            # 根据动作类型处理
            if queue_item.action == SyncAction.INSERT.value:
                self._insert_to_feishu(feishu_db, feishu_table, 
                                     queue_item.new_data, queue_item.record_id)
            elif queue_item.action == SyncAction.UPDATE.value:
                self._update_in_feishu(feishu_db, feishu_table,
                                     queue_item.new_data, queue_item.record_id)
            elif queue_item.action == SyncAction.DELETE.value:
                self._delete_from_feishu(feishu_db, feishu_table,
                                       queue_item.record_id)
            
            # 标记队列项为完成
            self.queue.mark_completed(queue_item.id)
            
            # 记录同步日志
            self.queue.log_sync(
                table_name=queue_item.table_name,
                record_id=queue_item.record_id,
                direction=SyncDirection.DB_TO_FEISHU.value,
                sync_hash=queue_item.sync_hash,
                status='completed'
            )
            
            logger.info(f"Synced {queue_item.action} from DB to Feishu: {queue_item.record_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync DB to Feishu: {e}")
            # 标记失败
            self.queue.mark_failed(queue_item.id, str(e))
            # 记录失败日志
            self.queue.log_sync(
                table_name=queue_item.table_name,
                record_id=queue_item.record_id,
                direction=SyncDirection.DB_TO_FEISHU.value,
                sync_hash=queue_item.sync_hash,
                status='failed',
                error_message=str(e)
            )
            return False
    
    def _insert_to_db(self, table: str, feishu_data: Dict[str, Any], 
                     feishu_id: str) -> None:
        """插入记录到数据库"""
        # 转换字段
        db_data = self.mapper.feishu_to_db(table, feishu_data)
        
        # 添加飞书ID
        db_data['feishu_id'] = feishu_id
        
        # 插入数据
        db_id = self.db.insert(table, db_data)
        
        # 保存ID映射
        self.queue.save_id_mapping(table, str(db_id), feishu_id)
    
    def _update_in_db(self, table: str, feishu_data: Dict[str, Any],
                     feishu_id: str) -> None:
        """更新数据库记录"""
        # 获取数据库ID
        db_id = self.queue.get_db_id(table, feishu_id)
        
        if not db_id:
            # 如果找不到映射，尝试用feishu_id查找
            existing = self.db.query_one(
                f"SELECT id FROM {table} WHERE feishu_id = %s",
                (feishu_id,)
            )
            if existing:
                db_id = str(existing['id'])
                # 保存映射
                self.queue.save_id_mapping(table, db_id, feishu_id)
            else:
                # 记录不存在，改为插入
                self._insert_to_db(table, feishu_data, feishu_id)
                return
        
        # 转换字段
        db_data = self.mapper.feishu_to_db(table, feishu_data)
        
        # 更新数据
        self.db.update(table, db_data, {'id': db_id})
    
    def _delete_from_db(self, table: str, feishu_id: str) -> None:
        """从数据库删除记录"""
        # 获取数据库ID
        db_id = self.queue.get_db_id(table, feishu_id)
        
        if db_id:
            self.db.delete(table, {'id': db_id})
        else:
            # 尝试用feishu_id删除
            self.db.delete(table, {'feishu_id': feishu_id})
    
    def _insert_to_feishu(self, database: str, table: str,
                         db_data: Dict[str, Any], db_id: str) -> None:
        """插入记录到飞书"""
        # 转换字段
        feishu_data = self.mapper.db_to_feishu(table, db_data)
        
        # 添加数据库ID引用（如果需要）
        if 'db_id' not in feishu_data:
            feishu_data['db_id'] = db_id
        
        # 创建记录
        feishu_id = self.feishu.create_record(database, table, feishu_data)
        
        # 保存ID映射
        self.queue.save_id_mapping(table, db_id, feishu_id)
    
    def _update_in_feishu(self, database: str, table: str,
                         db_data: Dict[str, Any], db_id: str) -> None:
        """更新飞书记录"""
        # 获取飞书ID
        feishu_id = self.queue.get_feishu_id(table, db_id)
        
        if not feishu_id:
            # 如果找不到映射，尝试搜索
            records = self.feishu.search_records(
                database, table, 'db_id', '=', db_id
            )
            if records:
                feishu_id = records[0]['id']
                # 保存映射
                self.queue.save_id_mapping(table, db_id, feishu_id)
            else:
                # 记录不存在，改为插入
                self._insert_to_feishu(database, table, db_data, db_id)
                return
        
        # 转换字段
        feishu_data = self.mapper.db_to_feishu(table, db_data)
        
        # 更新记录
        self.feishu.update_record(database, table, feishu_id, feishu_data)
    
    def _delete_from_feishu(self, database: str, table: str, db_id: str) -> None:
        """从飞书删除记录"""
        # 获取飞书ID
        feishu_id = self.queue.get_feishu_id(table, db_id)
        
        if feishu_id:
            self.feishu.delete_record(database, table, feishu_id)
        else:
            # 尝试搜索并删除
            records = self.feishu.search_records(
                database, table, 'db_id', '=', db_id
            )
            for record in records:
                self.feishu.delete_record(database, table, record['id'])