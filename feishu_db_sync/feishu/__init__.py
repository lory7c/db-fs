"""飞书客户端模块"""

from .client import FeishuClient
from .change_detector import ChangeDetector

__all__ = ["FeishuClient", "ChangeDetector"]