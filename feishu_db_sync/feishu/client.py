"""
飞书客户端封装
"""
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import hashlib
import json

from feishu_bitable_db import DBImpl
from feishu_bitable_db.db.types import Table, Field, FieldType, SearchCmd
from loguru import logger


class FeishuClient:
    """飞书客户端封装"""
    
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.db_client = DBImpl(app_id, app_secret)
        self._token_cache = {}
        self._token_expire_time = None
    
    def create_database(self, name: str) -> str:
        """创建数据库"""
        try:
            db_id = self.db_client.save_database(name)
            logger.info(f"Created database: {name} -> {db_id}")
            return db_id
        except Exception as e:
            logger.error(f"Failed to create database {name}: {e}")
            raise
    
    def create_table(self, database: str, table: Table) -> str:
        """创建表"""
        try:
            table_id = self.db_client.save_table(database, table)
            logger.info(f"Created table: {table.name} -> {table_id}")
            return table_id
        except Exception as e:
            logger.error(f"Failed to create table {table.name}: {e}")
            raise
    
    def list_tables(self, database: str) -> List[str]:
        """获取所有表"""
        try:
            return self.db_client.list_tables(database)
        except Exception as e:
            logger.error(f"Failed to list tables in {database}: {e}")
            raise
    
    def create_record(self, database: str, table: str, record: Dict[str, Any]) -> str:
        """创建记录"""
        try:
            record_id = self.db_client.create(database, table, record)
            logger.debug(f"Created record in {database}.{table}: {record_id}")
            return record_id
        except Exception as e:
            logger.error(f"Failed to create record: {e}")
            raise
    
    def batch_create_records(self, database: str, table: str, records: List[Dict[str, Any]]) -> List[str]:
        """批量创建记录"""
        record_ids = []
        for record in records:
            try:
                record_id = self.create_record(database, table, record)
                record_ids.append(record_id)
            except Exception as e:
                logger.error(f"Failed to create record: {e}")
                record_ids.append(None)
        return record_ids
    
    def read_records(self, database: str, table: str, 
                    search_cmds: Optional[List[SearchCmd]] = None) -> List[Dict[str, Any]]:
        """读取记录"""
        try:
            search_cmds = search_cmds or []
            records = self.db_client.read(database, table, search_cmds)
            logger.debug(f"Read {len(records)} records from {database}.{table}")
            return records
        except Exception as e:
            logger.error(f"Failed to read records: {e}")
            raise
    
    def read_all_records(self, database: str, table: str) -> List[Dict[str, Any]]:
        """读取所有记录（分页处理）"""
        all_records = []
        page_size = 500  # 飞书API单次最大返回数
        
        try:
            # 第一次查询
            records = self.read_records(database, table)
            all_records.extend(records)
            
            # 如果记录数等于页大小，可能还有更多记录
            while len(records) == page_size:
                # TODO: 实现基于游标的分页查询
                # 目前飞书 API 可能不支持游标分页，需要根据实际 API 调整
                break
            
            return all_records
        except Exception as e:
            logger.error(f"Failed to read all records: {e}")
            raise
    
    def update_record(self, database: str, table: str, 
                     record_id: str, record: Dict[str, Any]) -> None:
        """更新记录"""
        try:
            self.db_client.update(database, table, record_id, record)
            logger.debug(f"Updated record {record_id} in {database}.{table}")
        except Exception as e:
            logger.error(f"Failed to update record {record_id}: {e}")
            raise
    
    def batch_update_records(self, database: str, table: str, 
                           updates: List[Dict[str, Any]]) -> None:
        """批量更新记录"""
        for update in updates:
            try:
                self.update_record(database, table, update['id'], update['fields'])
            except Exception as e:
                logger.error(f"Failed to update record {update['id']}: {e}")
    
    def delete_record(self, database: str, table: str, record_id: str) -> None:
        """删除记录"""
        try:
            self.db_client.delete(database, table, record_id)
            logger.debug(f"Deleted record {record_id} from {database}.{table}")
        except Exception as e:
            logger.error(f"Failed to delete record {record_id}: {e}")
            raise
    
    def batch_delete_records(self, database: str, table: str, 
                           record_ids: List[str]) -> None:
        """批量删除记录"""
        for record_id in record_ids:
            try:
                self.delete_record(database, table, record_id)
            except Exception as e:
                logger.error(f"Failed to delete record {record_id}: {e}")
    
    def get_table_fields(self, database: str, table: str) -> List[Field]:
        """获取表字段信息"""
        # TODO: 实现获取表结构的方法
        # 需要根据飞书实际 API 实现
        pass
    
    def search_records(self, database: str, table: str, 
                      field: str, operator: str, value: Any) -> List[Dict[str, Any]]:
        """搜索记录"""
        search_cmd = SearchCmd(key=field, operator=operator, val=value)
        return self.read_records(database, table, [search_cmd])
    
    def calculate_record_hash(self, record: Dict[str, Any], 
                            exclude_fields: Optional[List[str]] = None) -> str:
        """计算记录哈希值"""
        exclude_fields = exclude_fields or ['id', 'created_at', 'updated_at', '_sync_source']
        
        # 创建记录副本并移除排除字段
        clean_record = {
            k: v for k, v in record.items() 
            if k not in exclude_fields
        }
        
        # 对字典进行排序并序列化
        record_str = json.dumps(clean_record, sort_keys=True, ensure_ascii=False)
        
        # 计算哈希
        return hashlib.md5(record_str.encode('utf-8')).hexdigest()
    
    def test_connection(self) -> bool:
        """测试连接"""
        try:
            # 尝试列出数据库来测试连接
            self.db_client.list_tables("test")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False