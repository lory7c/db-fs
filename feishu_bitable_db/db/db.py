"""数据库主模块"""

from typing import List, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod
import logging

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.drive.v1 import *

from .types import Database, Table, Field, SearchCmd, FieldType, ID, NAME
from .field import FieldManagerImpl
from .record import RecordManager
from ..client import Bitable, BitableImpl


logger = logging.getLogger(__name__)


class DB(ABC):
    """数据库接口"""
    
    @abstractmethod
    def save_database(self, name: str) -> str:
        """创建数据库（如果不存在）"""
        pass
    
    @abstractmethod
    def save_table(self, database: str, table: Table) -> str:
        """创建或更新表"""
        pass
    
    @abstractmethod
    def list_tables(self, database: str) -> List[str]:
        """列出数据库中的所有表"""
        pass
    
    @abstractmethod
    def drop_table(self, database: str, table: str) -> None:
        """删除表"""
        pass
    
    @abstractmethod
    def create(self, database: str, table: str, record: Dict[str, Any]) -> str:
        """创建记录"""
        pass
    
    @abstractmethod
    def read(self, database: str, table: str, search_cmds: List[SearchCmd]) -> List[Dict[str, Any]]:
        """查询记录"""
        pass
    
    @abstractmethod
    def update(self, database: str, table: str, record_id: str, record: Dict[str, Any]) -> None:
        """更新记录"""
        pass
    
    @abstractmethod
    def delete(self, database: str, table: str, record_id: str) -> None:
        """删除记录"""
        pass


class DBImpl(DB):
    """数据库实现"""
    
    def __init__(self, app_id: str, app_secret: str):
        """
        初始化数据库
        
        Args:
            app_id: 飞书应用 ID
            app_secret: 飞书应用密钥
        """
        logger.debug(f"初始化飞书客户端: app_id={app_id}")
        
        # 创建客户端
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .domain(lark.FEISHU_DOMAIN) \
            .log_level(lark.LogLevel.ERROR) \
            .build()
        
        # 初始化各个管理器
        self.bitable = BitableImpl(self.client)
        self.field_manager = FieldManagerImpl(self.client)
        self.record_manager = RecordManager(self.client, self.field_manager)
        
        # 缓存
        self._cache: Dict[str, str] = {}
        
        # 获取根文件夹 token 和用户 ID
        self._init_root_meta()
    
    def _init_root_meta(self):
        """初始化根文件夹信息"""
        request = GetRootFolderMetaRequest.builder().build()
        response = self.client.drive.v1.file.get_root_folder_meta(request)
        
        if not response.success():
            raise Exception(f"获取根文件夹信息失败: {response.msg}")
        
        self.root_token = response.data.token
        self.user_id = response.data.user_id
    
    def save_database(self, name: str) -> str:
        """创建数据库（如果不存在）"""
        # 尝试从缓存获取
        did = self._get_did(name)
        if did:
            return did
        
        # 创建新数据库
        did = self.bitable.create_app(name, self.root_token)
        self._cache[f"db-{name}"] = did
        return did
    
    def save_table(self, database: str, table: Table) -> str:
        """创建或更新表"""
        # 确保数据库存在
        did = self.save_database(database)
        
        # 获取现有表
        tables = self._list_tables_with_id(database)
        table_id = tables.get(table.name)
        
        # 如果表不存在，创建表
        if not table_id:
            table_id = self._create_table(did, table.name)
        
        # 获取现有字段
        fields = self.field_manager.list_fields(did, table_id)
        old_field_map = {field["field_name"]: field for field in fields}
        
        # 确保第一个字段是 ID 字段
        if fields and fields[0]["field_name"] != ID:
            self.field_manager.update_field(did, table_id, {
                "field_id": fields[0]["field_id"],
                "field_name": ID,
                "type": int(FieldType.STRING),
            })
            del old_field_map[fields[0]["field_name"]]
        
        # 移除 ID 字段，因为已经处理过
        old_field_map.pop(ID, None)
        
        # 更新或创建字段
        for field in table.fields:
            old_field = old_field_map.get(field.name)
            
            if old_field:
                # 字段存在，检查是否需要更新
                if field.name == old_field["field_name"] and int(field.type) == old_field["type"]:
                    # 字段相同，无需更新
                    old_field_map.pop(field.name)
                    continue
                
                # 更新字段
                self.field_manager.update_field(did, table_id, {
                    "field_id": old_field["field_id"],
                    "field_name": field.name,
                    "type": int(field.type),
                })
                old_field_map.pop(field.name)
            else:
                # 创建新字段
                self.field_manager.create_field(did, table_id, {
                    "field_name": field.name,
                    "type": int(field.type),
                })
        
        # 删除不再需要的字段
        for field in old_field_map.values():
            self.field_manager.delete_field(did, table_id, field["field_id"])
        
        return table_id
    
    def list_tables(self, database: str) -> List[str]:
        """列出数据库中的所有表"""
        tables = self._list_tables_with_id(database)
        return list(tables.keys())
    
    def drop_table(self, database: str, table: str) -> None:
        """删除表"""
        # 获取数据库 ID
        did = self._get_did(database)
        if not did:
            raise ValueError(f"数据库 [{database}] 不存在")
        
        # 获取表 ID
        tid = self._get_tid(database, table)
        if not tid:
            raise ValueError(f"表 [{database}.{table}] 不存在")
        
        # 删除表
        request = DeleteAppTableRequest.builder() \
            .app_token(did) \
            .table_id(tid) \
            .build()
        
        response = self.client.bitable.v1.app_table.delete(request)
        
        if not response.success():
            logger.error(f"删除表失败: database={did}, table={tid}, error={response.msg}")
            raise Exception(f"删除表失败: {response.msg}")
        
        logger.debug(f"删除表成功: {response}")
    
    def create(self, database: str, table: str, record: Dict[str, Any]) -> str:
        """创建记录"""
        return self.record_manager.create(database, table, record)
    
    def read(self, database: str, table: str, search_cmds: List[SearchCmd]) -> List[Dict[str, Any]]:
        """查询记录"""
        return self.record_manager.read(database, table, search_cmds)
    
    def update(self, database: str, table: str, record_id: str, record: Dict[str, Any]) -> None:
        """更新记录"""
        self.record_manager.update(database, table, record_id, record)
    
    def delete(self, database: str, table: str, record_id: str) -> None:
        """删除记录"""
        self.record_manager.delete(database, table, record_id)
    
    def _list_tables_with_id(self, database: str) -> Dict[str, str]:
        """获取表名到表 ID 的映射"""
        # 确保数据库存在
        did = self.save_database(database)
        
        # 创建请求
        request = ListAppTableRequest.builder() \
            .app_token(did) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table.list(request)
        
        if not response.success():
            logger.error(f"列出表失败: database={database}, app_token={did}, error={response.msg}")
            return {}
        
        logger.debug(f"列出表成功: {response}")
        
        # 返回表名到 ID 的映射
        result = {}
        if response.data and response.data.items:
            for item in response.data.items:
                result[item.name] = item.table_id
        
        return result
    
    def _create_table(self, app_token: str, name: str) -> str:
        """创建表"""
        # 创建请求
        request_body = CreateAppTableRequestBody.builder() \
            .table(ReqTable.builder().name(name).build()) \
            .build()
        
        request = CreateAppTableRequest.builder() \
            .app_token(app_token) \
            .request_body(request_body) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table.create(request)
        
        if not response.success():
            logger.error(f"创建表失败: app_token={app_token}, error={response.msg}")
            raise Exception(f"创建表失败: {response.msg}")
        
        logger.debug(f"创建表成功: {response}")
        return response.data.table_id
    
    def _get_did(self, database: str) -> Optional[str]:
        """获取数据库 ID"""
        # 如果已经是 token（以 bascn 开头）
        if database.startswith("bascn"):
            return database
        
        # 从缓存获取
        cache_key = f"db-{database}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # 从飞书查询
        did, found = self.bitable.query_by_name(database, self.root_token)
        if found:
            self._cache[cache_key] = did
            return did
        
        return None
    
    def _get_tid(self, database: str, table: str) -> Optional[str]:
        """获取表 ID"""
        # 从缓存获取
        cache_key = f"table-{database}-{table}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # 获取所有表
        tables = self._list_tables_with_id(database)
        
        # 更新缓存
        for name, tid in tables.items():
            self._cache[f"table-{database}-{name}"] = tid
        
        return tables.get(table)