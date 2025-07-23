"""类型转换辅助函数"""

from typing import Dict, Any
from .types import ID


def get_str(record: Dict[str, Any], key: str) -> str:
    """
    从字典中获取字符串值
    
    Args:
        record: 记录字典
        key: 键名
        
    Returns:
        字符串值，不存在或类型错误时返回空字符串
    """
    value = record.get(key)
    if isinstance(value, str):
        return value
    return ""


def get_int(record: Dict[str, Any], key: str) -> int:
    """
    从字典中获取整数值
    
    Args:
        record: 记录字典
        key: 键名
        
    Returns:
        整数值，不存在或类型错误时返回 0
    """
    value = record.get(key)
    if isinstance(value, int):
        return value
    return 0


def get_id(record: Dict[str, Any]) -> str:
    """
    获取记录的 ID
    
    Args:
        record: 记录字典
        
    Returns:
        ID 字符串
    """
    return get_str(record, ID)