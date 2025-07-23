"""飞书多维表格客户端实现"""

from typing import Optional, Tuple
from abc import ABC, abstractmethod

import lark_oapi as lark
from lark_oapi.api.drive.v1 import *

from .drive import DriveExt


TYPE = "bitable"


class Bitable(ABC):
    """飞书多维表格接口"""
    
    @abstractmethod
    def create_app(self, name: str, folder_token: str) -> str:
        """
        创建多维表格应用
        
        Args:
            name: 应用名称
            folder_token: 文件夹 token
            
        Returns:
            创建的多维表格 token
            
        Raises:
            Exception: 创建失败时抛出
        """
        pass
    
    @abstractmethod
    def query_by_name(self, name: str, folder_token: str) -> Tuple[str, bool]:
        """
        根据名称查询多维表格
        
        Args:
            name: 应用名称
            folder_token: 文件夹 token
            
        Returns:
            (token, found) - 多维表格 token 和是否找到
        """
        pass


class BitableImpl(Bitable):
    """飞书多维表格实现"""
    
    def __init__(self, client: lark.Client):
        """
        初始化
        
        Args:
            client: 飞书客户端
        """
        self.client = client
        self.drives = DriveExt(client)
    
    def create_app(self, name: str, folder_token: str) -> str:
        """创建多维表格应用"""
        # 创建请求
        request = CreateFileRequest.builder() \
            .folder_token(folder_token) \
            .request_body(CreateFileRequestBody.builder()
                .title(name)
                .type(TYPE)
                .build()) \
            .build()
        
        # 发起请求
        response = self.client.drive.v1.file.create(request)
        
        # 处理响应
        if not response.success():
            raise Exception(f"创建多维表格失败: {response.msg}")
        
        return response.data.token
    
    def query_by_name(self, name: str, folder_token: str) -> Tuple[str, bool]:
        """根据名称查询多维表格"""
        try:
            # 获取文件夹中的文件列表
            files = self.drives.get_drive_files(folder_token)
            
            # 查找匹配的多维表格
            for file in files:
                if file.get("name") == name and file.get("type") == TYPE:
                    return file.get("token", ""), True
            
            return "", False
        except Exception:
            return "", False