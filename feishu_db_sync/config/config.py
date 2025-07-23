"""
配置管理模块
"""
import json
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    pool_size: int = 5
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'charset': self.charset
        }


@dataclass
class FeishuConfig:
    """飞书配置"""
    app_id: str
    app_secret: str
    base_url: str = "https://open.feishu.cn/open-apis"
    timeout: int = 30


@dataclass
class SyncConfig:
    """同步配置"""
    poll_interval: int = 5  # 轮询间隔（秒）
    batch_size: int = 100  # 批量处理大小
    retry_times: int = 3  # 重试次数
    retry_interval: int = 5  # 重试间隔（秒）
    sync_timeout: int = 300  # 同步超时（秒）
    enable_cache: bool = True  # 是否启用缓存
    cache_ttl: int = 3600  # 缓存过期时间（秒）
    
    # 表映射配置: {"飞书数据库:飞书表": "数据库表"}
    table_mapping: Dict[str, str] = field(default_factory=dict)
    
    # 字段映射配置: {"数据库表": {"飞书字段": "数据库字段"}}
    field_mapping: Dict[str, Dict[str, str]] = field(default_factory=dict)
    
    # 同步过滤条件
    sync_filters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MonitorConfig:
    """监控配置"""
    enable_metrics: bool = True
    metrics_port: int = 8080
    alert_webhook: Optional[str] = None
    log_level: str = "INFO"
    log_file: str = "sync.log"
    log_max_size: str = "100MB"
    log_backup_count: int = 10


class Config:
    """配置管理器"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self._data: Dict[str, Any] = {}
        
        # 配置对象
        self.database: Optional[DatabaseConfig] = None
        self.feishu: Optional[FeishuConfig] = None
        self.sync: Optional[SyncConfig] = None
        self.monitor: Optional[MonitorConfig] = None
        
        # 加载配置
        self.load()
    
    def _find_config_file(self) -> str:
        """查找配置文件"""
        search_paths = [
            Path.cwd() / "config.json",
            Path.cwd() / "config" / "config.json",
            Path.home() / ".feishu_sync" / "config.json",
            Path("/etc/feishu_sync/config.json")
        ]
        
        for path in search_paths:
            if path.exists():
                return str(path)
        
        # 默认配置文件路径
        return str(Path.cwd() / "config.json")
    
    def load(self) -> None:
        """加载配置文件"""
        if not os.path.exists(self.config_path):
            self._create_default_config()
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self._data = json.load(f)
        
        self._parse_config()
    
    def _parse_config(self) -> None:
        """解析配置"""
        # 数据库配置
        db_config = self._data.get('database', {})
        self.database = DatabaseConfig(**db_config)
        
        # 飞书配置
        feishu_config = self._data.get('feishu', {})
        self.feishu = FeishuConfig(**feishu_config)
        
        # 同步配置
        sync_config = self._data.get('sync', {})
        self.sync = SyncConfig(**sync_config)
        
        # 监控配置
        monitor_config = self._data.get('monitor', {})
        self.monitor = MonitorConfig(**monitor_config)
    
    def _create_default_config(self) -> None:
        """创建默认配置文件"""
        default_config = {
            "database": {
                "host": "localhost",
                "port": 3306,
                "user": "root",
                "password": "",
                "database": "feishu_sync"
            },
            "feishu": {
                "app_id": "your_app_id",
                "app_secret": "your_app_secret"
            },
            "sync": {
                "poll_interval": 5,
                "batch_size": 100,
                "retry_times": 3,
                "table_mapping": {
                    "MyDB:users": "users",
                    "MyDB:orders": "orders"
                },
                "field_mapping": {
                    "users": {
                        "姓名": "name",
                        "年龄": "age",
                        "邮箱": "email"
                    }
                }
            },
            "monitor": {
                "enable_metrics": True,
                "log_level": "INFO",
                "log_file": "sync.log"
            }
        }
        
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)
        
        self._data = default_config
    
    def save(self) -> None:
        """保存配置"""
        config_dict = {
            "database": self.database.__dict__ if self.database else {},
            "feishu": self.feishu.__dict__ if self.feishu else {},
            "sync": self.sync.__dict__ if self.sync else {},
            "monitor": self.monitor.__dict__ if self.monitor else {}
        }
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=4, ensure_ascii=False)
    
    def validate(self) -> bool:
        """验证配置是否有效"""
        # 验证必要字段
        if not self.feishu or not self.feishu.app_id or not self.feishu.app_secret:
            raise ValueError("飞书配置缺少 app_id 或 app_secret")
        
        if not self.database or not self.database.host or not self.database.database:
            raise ValueError("数据库配置缺少必要信息")
        
        if not self.sync or not self.sync.table_mapping:
            raise ValueError("同步配置缺少表映射关系")
        
        return True
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value = self._data
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        keys = key.split('.')
        data = self._data
        
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        
        data[keys[-1]] = value
        self._parse_config()
    
    def reload(self) -> None:
        """重新加载配置"""
        self.load()