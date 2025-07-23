"""飞书云文档扩展功能"""

from typing import List, Dict, Any
from abc import ABC, abstractmethod

import lark_oapi as lark
from lark_oapi.api.drive.v1 import *


class DriveFile:
    """云文档文件信息"""
    
    def __init__(self, data: Dict[str, Any]):
        self.name = data.get("name", "")
        self.parent_token = data.get("parent_token", "")
        self.token = data.get("token", "")
        self.type = data.get("type", "")
        self.url = data.get("url", "")


class DriveFiles:
    """云文档文件列表"""
    
    def __init__(self, data: Dict[str, Any]):
        self.has_more = data.get("has_more", False)
        self.page_token = data.get("page_token", "")
        self.total = data.get("total", 0)
        self.files = [DriveFile(file) for file in data.get("files", [])]


class DriveExt:
    """飞书云文档扩展接口"""
    
    def __init__(self, client: lark.Client):
        """
        初始化
        
        Args:
            client: 飞书客户端
        """
        self.client = client
    
    def get_drive_files(self, folder_token: str) -> List[Dict[str, Any]]:
        """
        获取文件夹中的文件列表
        
        Args:
            folder_token: 文件夹 token
            
        Returns:
            文件列表
            
        Raises:
            Exception: 获取失败时抛出
        """
        # 创建请求
        request = ListFileRequest.builder() \
            .folder_token(folder_token) \
            .build()
        
        # 发起请求
        response = self.client.drive.v1.file.list(request)
        
        # 处理响应
        if not response.success():
            raise Exception(f"获取文件列表失败: {response.msg}")
        
        # 返回文件列表
        files = []
        if response.data and response.data.files:
            for file in response.data.files:
                files.append({
                    "name": file.name,
                    "parent_token": file.parent_token,
                    "token": file.token,
                    "type": file.type,
                    "url": file.url if hasattr(file, 'url') else ""
                })
        
        return files