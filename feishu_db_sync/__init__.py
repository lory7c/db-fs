"""
飞书多维表格与数据库双向同步系统
"""

__version__ = "1.0.0"
__author__ = "lory7c"

from .core.sync_service import SyncService
from .config.config import Config

__all__ = ["SyncService", "Config"]