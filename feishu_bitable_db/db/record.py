"""记录管理模块"""

from typing import List, Dict, Any, Optional
import logging

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

from .types import SearchCmd, ID
from .field import FieldManagerImpl


logger = logging.getLogger(__name__)


class RecordManager:
    """记录管理器"""
    
    def __init__(self, client: lark.Client, field_manager: FieldManagerImpl):
        self.client = client
        self.field_manager = field_manager
    
    def create(self, database: str, table: str, record: Dict[str, Any]) -> str:
        """
        创建记录
        
        Args:
            database: 数据库（应用）token
            table: 表 ID
            record: 记录数据
            
        Returns:
            创建的记录 ID
        """
        # 获取字段列表
        fields = self.field_manager.list_fields(database, table)
        field_map = {field["field_name"]: field["field_id"] for field in fields}
        
        # 如果存在 ID 字段，先设置为空
        update_id_after = False
        if ID in field_map:
            record[ID] = ""
            update_id_after = True
        
        # 创建记录请求
        request_body = AppTableRecord.builder() \
            .fields(record) \
            .build()
        
        request = CreateAppTableRecordRequest.builder() \
            .app_token(database) \
            .table_id(table) \
            .request_body(request_body) \
            .build()
        
        # 发起请求
        logger.info(f"创建记录请求: {request}")
        response = self.client.bitable.v1.app_table_record.create(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"创建记录失败: database={database}, table={table}, error={response.msg}")
            raise Exception(f"创建记录失败: {response.msg}")
        
        logger.debug(f"创建记录成功: {response}")
        record_id = response.data.record.record_id
        
        # 如果需要更新 ID 字段
        if update_id_after:
            try:
                self.update(database, table, record_id, {})
            except Exception as e:
                logger.warning(f"更新 ID 字段失败: {e}")
        
        return record_id
    
    def read(self, database: str, table: str, search_cmds: List[SearchCmd]) -> List[Dict[str, Any]]:
        """
        查询记录
        
        Args:
            database: 数据库（应用）token
            table: 表 ID
            search_cmds: 搜索条件列表
            
        Returns:
            记录列表
        """
        # 构建过滤条件
        filters = []
        for cmd in search_cmds:
            key = cmd.key
            operator = cmd.operator
            val = cmd.val
            
            if isinstance(val, str):
                filters.append(f'CurrentValue.[{key}]{operator}"{val}"')
            elif isinstance(val, (int, float)):
                filters.append(f'CurrentValue.[{key}]{operator}{val}')
            else:
                filters.append(f'CurrentValue.[{key}]{operator}{val}')
        
        # 组合过滤条件
        if len(filters) == 0:
            filter_str = ""
        elif len(filters) == 1:
            filter_str = f"AND({filters[0]})"
        else:
            filter_str = f"AND({','.join(filters)})"
        
        # 创建请求
        request = ListAppTableRecordRequest.builder() \
            .app_token(database) \
            .table_id(table) \
            .filter(filter_str) \
            .page_size(1000) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_record.list(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"查询记录失败: database={database}, table={table}, "
                        f"filter={filter_str}, error={response.msg}")
            return []
        
        logger.debug(f"查询记录成功: {response}")
        
        # 处理结果
        results = []
        if response.data and response.data.items:
            for item in response.data.items:
                record = dict(item.fields) if item.fields else {}
                record[ID] = item.record_id
                results.append(record)
        
        return results
    
    def update(self, database: str, table: str, record_id: str, record: Dict[str, Any]) -> None:
        """
        更新记录
        
        Args:
            database: 数据库（应用）token
            table: 表 ID
            record_id: 记录 ID
            record: 更新的数据
        """
        # 获取字段列表
        fields = self.field_manager.list_fields(database, table)
        field_map = {field["field_name"]: field["field_id"] for field in fields}
        
        # 如果存在 ID 字段，设置为记录 ID
        if ID in field_map:
            record[ID] = record_id
        
        # 创建请求
        request_body = AppTableRecord.builder() \
            .fields(record) \
            .build()
        
        request = UpdateAppTableRecordRequest.builder() \
            .app_token(database) \
            .table_id(table) \
            .record_id(record_id) \
            .request_body(request_body) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_record.update(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"更新记录失败: database={database}, table={table}, "
                        f"record_id={record_id}, error={response.msg}")
            raise Exception(f"更新记录失败: {response.msg}")
        
        logger.debug(f"更新记录成功: {response}")
    
    def delete(self, database: str, table: str, record_id: str) -> None:
        """
        删除记录
        
        Args:
            database: 数据库（应用）token
            table: 表 ID
            record_id: 记录 ID
        """
        # 创建请求
        request = DeleteAppTableRecordRequest.builder() \
            .app_token(database) \
            .table_id(table) \
            .record_id(record_id) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_record.delete(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"删除记录失败: database={database}, table={table}, "
                        f"record_id={record_id}, error={response.msg}")
            raise Exception(f"删除记录失败: {response.msg}")
        
        logger.debug(f"删除记录成功: {response}")