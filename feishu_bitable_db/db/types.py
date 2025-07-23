"""数据库类型定义"""

from enum import IntEnum
from typing import List, Any, Optional
from dataclasses import dataclass


class FieldType(IntEnum):
    """字段类型枚举"""
    STRING = 1
    INT = 2
    RADIO = 3
    MULTI_SELECT = 4
    DATE = 5
    PEOPLE = 11


# 常量定义
ID = "id"
NAME = "Databases"


@dataclass
class Field:
    """字段定义"""
    name: str
    type: FieldType


@dataclass
class Table:
    """表定义"""
    name: str
    fields: List[Field]


@dataclass
class Database:
    """数据库定义"""
    name: str
    tables: List[Table]


@dataclass
class SearchCmd:
    """搜索条件"""
    key: str
    operator: str
    val: Any