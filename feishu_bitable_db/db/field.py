"""字段管理模块"""

from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import logging

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

from .types import FieldType


logger = logging.getLogger(__name__)


class FieldManager(ABC):
    """字段管理接口"""
    
    @abstractmethod
    def list_fields(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        """列出表的所有字段"""
        pass
    
    @abstractmethod
    def create_field(self, app_token: str, table_id: str, field: Dict[str, Any]) -> str:
        """创建字段"""
        pass
    
    @abstractmethod
    def update_field(self, app_token: str, table_id: str, field: Dict[str, Any]) -> None:
        """更新字段"""
        pass
    
    @abstractmethod
    def delete_field(self, app_token: str, table_id: str, field_id: str) -> None:
        """删除字段"""
        pass


class FieldManagerImpl(FieldManager):
    """字段管理实现"""
    
    def __init__(self, client: lark.Client):
        self.client = client
    
    def list_fields(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        """列出表的所有字段"""
        # 创建请求
        request = ListAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_field.list(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"列出字段失败: app_token={app_token}, table_id={table_id}, error={response.msg}")
            raise Exception(f"列出字段失败: {response.msg}")
        
        logger.debug(f"列出字段成功: {response}")
        
        # 返回字段列表
        fields = []
        if response.data and response.data.items:
            for field in response.data.items:
                fields.append(self._field_to_dict(field))
        
        return fields
    
    def create_field(self, app_token: str, table_id: str, field: Dict[str, Any]) -> str:
        """创建字段"""
        # 构建字段对象
        field_obj = self._dict_to_field(field)
        
        # 创建请求
        request = CreateAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(field_obj) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_field.create(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"创建字段失败: app_token={app_token}, error={response.msg}")
            raise Exception(f"创建字段失败: {response.msg}")
        
        logger.debug(f"创建字段成功: {response}")
        
        return response.data.field.field_id
    
    def update_field(self, app_token: str, table_id: str, field: Dict[str, Any]) -> None:
        """更新字段"""
        field_id = field.get("field_id")
        if not field_id:
            raise ValueError("字段 ID 不能为空")
        
        # 构建字段对象
        field_obj = self._dict_to_field(field)
        
        # 创建请求
        request = UpdateAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .field_id(field_id) \
            .request_body(field_obj) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_field.update(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"更新字段失败: app_token={app_token}, table_id={table_id}, "
                        f"field_id={field_id}, error={response.msg}")
            raise Exception(f"更新字段失败: {response.msg}")
        
        logger.debug(f"更新字段成功: {response}")
    
    def delete_field(self, app_token: str, table_id: str, field_id: str) -> None:
        """删除字段"""
        # 创建请求
        request = DeleteAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .field_id(field_id) \
            .build()
        
        # 发起请求
        response = self.client.bitable.v1.app_table_field.delete(request)
        
        # 处理响应
        if not response.success():
            logger.error(f"删除字段失败: app_token={app_token}, field_id={field_id}, error={response.msg}")
            raise Exception(f"删除字段失败: {response.msg}")
        
        logger.debug(f"删除字段成功: {response}")
    
    def _field_to_dict(self, field: AppTableField) -> Dict[str, Any]:
        """将字段对象转换为字典"""
        return {
            "field_id": field.field_id,
            "field_name": field.field_name,
            "type": field.type,
            "property": field.property,
            "description": field.description if hasattr(field, 'description') else None,
            "is_primary": field.is_primary if hasattr(field, 'is_primary') else False,
        }
    
    def _dict_to_field(self, field_dict: Dict[str, Any]) -> AppTableField:
        """将字典转换为字段对象"""
        field = AppTableField.builder() \
            .field_name(field_dict["field_name"]) \
            .type(field_dict["type"])
        
        if "property" in field_dict:
            field.property(field_dict["property"])
        
        if "description" in field_dict:
            field.description(field_dict["description"])
        
        if "is_primary" in field_dict:
            field.is_primary(field_dict["is_primary"])
        
        return field.build()