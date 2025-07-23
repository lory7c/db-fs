"""
飞书多维表格数据库 Python SDK

将飞书多维表格作为数据库使用的 Python 库
"""

__version__ = "0.1.0"

from .db.db import DB
from .db.types import Database, Table, Field, SearchCmd, FieldType

__all__ = [
    "DB",
    "Database",
    "Table", 
    "Field",
    "SearchCmd",
    "FieldType",
]