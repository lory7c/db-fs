"""
同步服务主类
"""
import time
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime
from loguru import logger
import redis

from ..config.config import Config
from ..feishu.client import FeishuClient
from ..feishu.change_detector import ChangeDetector
from ..db.database import Database
from ..db.queue_processor import QueueProcessor
from ..monitor.metrics import MetricsCollector
from .field_mapper import FieldMapper
from .sync_worker import SyncWorker


class SyncService:
    """双向同步服务"""
    
    def __init__(self, config: Config):
        self.config = config
        self.running = False
        self._threads = []
        
        # 初始化组件
        self._init_components()
        
        # 同步统计
        self.stats = {
            'feishu_to_db_success': 0,
            'feishu_to_db_failed': 0,
            'db_to_feishu_success': 0,
            'db_to_feishu_failed': 0,
            'start_time': None
        }
    
    def _init_components(self) -> None:
        """初始化组件"""
        # 验证配置
        self.config.validate()
        
        # 初始化飞书客户端
        self.feishu_client = FeishuClient(
            self.config.feishu.app_id,
            self.config.feishu.app_secret
        )
        
        # 初始化数据库
        self.database = Database(self.config.database)
        
        # 创建同步表
        self.database.create_sync_tables()
        
        # 初始化Redis（可选）
        self.redis_client = None
        if self.config.sync.enable_cache:
            try:
                self.redis_client = redis.Redis(
                    host=self.config.get('redis.host', 'localhost'),
                    port=self.config.get('redis.port', 6379),
                    decode_responses=True
                )
                self.redis_client.ping()
                logger.info("Redis connected successfully")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}, using memory cache")
                self.redis_client = None
        
        # 初始化其他组件
        self.change_detector = ChangeDetector(self.feishu_client, self.redis_client)
        self.queue_processor = QueueProcessor(self.database)
        self.field_mapper = FieldMapper(self.config.sync.field_mapping)
        self.sync_worker = SyncWorker(
            self.feishu_client,
            self.database,
            self.queue_processor,
            self.field_mapper
        )
        
        # 初始化监控
        if self.config.monitor.enable_metrics:
            self.metrics = MetricsCollector(self.config.monitor)
        else:
            self.metrics = None
        
        logger.info("All components initialized successfully")
    
    def start(self) -> None:
        """启动同步服务"""
        if self.running:
            logger.warning("Sync service is already running")
            return
        
        logger.info("Starting sync service...")
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        # 测试连接
        if not self._test_connections():
            self.running = False
            raise Exception("Connection test failed")
        
        # 启动同步线程
        feishu_thread = threading.Thread(
            target=self._feishu_sync_loop,
            name="FeishuSyncThread"
        )
        feishu_thread.daemon = True
        feishu_thread.start()
        self._threads.append(feishu_thread)
        
        db_thread = threading.Thread(
            target=self._db_sync_loop,
            name="DatabaseSyncThread"
        )
        db_thread.daemon = True
        db_thread.start()
        self._threads.append(db_thread)
        
        # 启动清理线程
        cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            name="CleanupThread"
        )
        cleanup_thread.daemon = True
        cleanup_thread.start()
        self._threads.append(cleanup_thread)
        
        # 启动监控线程
        if self.metrics:
            metrics_thread = threading.Thread(
                target=self._metrics_loop,
                name="MetricsThread"
            )
            metrics_thread.daemon = True
            metrics_thread.start()
            self._threads.append(metrics_thread)
        
        logger.info("Sync service started successfully")
    
    def stop(self) -> None:
        """停止同步服务"""
        logger.info("Stopping sync service...")
        self.running = False
        
        # 等待线程结束
        for thread in self._threads:
            thread.join(timeout=5)
        
        logger.info("Sync service stopped")
    
    def _test_connections(self) -> bool:
        """测试连接"""
        # 测试飞书连接
        if not self.feishu_client.test_connection():
            logger.error("Feishu connection test failed")
            return False
        
        # 测试数据库连接
        if not self.database.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("All connections tested successfully")
        return True
    
    def _feishu_sync_loop(self) -> None:
        """飞书同步循环"""
        logger.info("Feishu sync loop started")
        
        while self.running:
            try:
                # 检测所有表的变更
                all_changes = self.change_detector.batch_detect_changes(
                    self.config.sync.table_mapping
                )
                
                # 处理每个表的变更
                for feishu_table, changes in all_changes.items():
                    if not changes:
                        continue
                    
                    db_table = self.config.sync.table_mapping[feishu_table]
                    logger.info(f"Processing {len(changes)} changes for {feishu_table}")
                    
                    for change in changes:
                        success = self.sync_worker.sync_feishu_to_db(
                            feishu_table, db_table, change
                        )
                        
                        # 更新统计
                        if success:
                            self.stats['feishu_to_db_success'] += 1
                        else:
                            self.stats['feishu_to_db_failed'] += 1
                        
                        # 更新监控指标
                        if self.metrics:
                            self.metrics.record_sync(
                                'feishu_to_db',
                                'success' if success else 'failed'
                            )
                
            except Exception as e:
                logger.error(f"Error in Feishu sync loop: {e}")
                if self.metrics:
                    self.metrics.record_error('feishu_sync_loop', str(e))
            
            # 等待下次轮询
            time.sleep(self.config.sync.poll_interval)
    
    def _db_sync_loop(self) -> None:
        """数据库同步循环"""
        logger.info("Database sync loop started")
        
        while self.running:
            try:
                # 获取待处理的队列项
                queue_items = self.queue_processor.get_pending_items(
                    limit=self.config.sync.batch_size
                )
                
                if not queue_items:
                    time.sleep(1)  # 没有任务时短暂休眠
                    continue
                
                logger.info(f"Processing {len(queue_items)} queue items")
                
                for item in queue_items:
                    # 标记为处理中
                    self.queue_processor.mark_processing(item.id)
                    
                    # 找到对应的飞书表
                    feishu_table = None
                    for ft, dt in self.config.sync.table_mapping.items():
                        if dt == item.table_name:
                            feishu_table = ft
                            break
                    
                    if not feishu_table:
                        logger.error(f"No Feishu table mapping for {item.table_name}")
                        self.queue_processor.mark_failed(
                            item.id, 
                            f"No mapping for table {item.table_name}"
                        )
                        continue
                    
                    # 解析飞书数据库和表名
                    feishu_db, feishu_table_name = feishu_table.split(':')
                    
                    # 执行同步
                    success = self.sync_worker.sync_db_to_feishu(
                        item, feishu_db, feishu_table_name
                    )
                    
                    # 更新统计
                    if success:
                        self.stats['db_to_feishu_success'] += 1
                    else:
                        self.stats['db_to_feishu_failed'] += 1
                    
                    # 更新监控指标
                    if self.metrics:
                        self.metrics.record_sync(
                            'db_to_feishu',
                            'success' if success else 'failed'
                        )
                
            except Exception as e:
                logger.error(f"Error in database sync loop: {e}")
                if self.metrics:
                    self.metrics.record_error('db_sync_loop', str(e))
            
            time.sleep(0.1)  # 短暂休眠避免CPU占用过高
    
    def _cleanup_loop(self) -> None:
        """清理循环"""
        logger.info("Cleanup loop started")
        
        while self.running:
            try:
                # 每小时执行一次清理
                time.sleep(3600)
                
                # 清理旧记录
                self.queue_processor.cleanup_old_records(days=7)
                
                logger.info("Cleanup completed")
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    def _metrics_loop(self) -> None:
        """监控指标循环"""
        logger.info("Metrics loop started")
        
        while self.running:
            try:
                # 获取队列统计
                queue_stats = self.queue_processor.get_queue_stats()
                
                # 更新监控指标
                self.metrics.update_queue_stats(queue_stats)
                
                # 记录同步统计
                self.metrics.update_sync_stats(self.stats)
                
                # 每30秒更新一次
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error in metrics loop: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        uptime = None
        if self.stats['start_time']:
            uptime = (datetime.now() - self.stats['start_time']).total_seconds()
        
        # 获取队列统计
        queue_stats = self.queue_processor.get_queue_stats()
        
        return {
            'running': self.running,
            'uptime_seconds': uptime,
            'sync_stats': self.stats,
            'queue_stats': queue_stats,
            'threads': [
                {
                    'name': t.name,
                    'alive': t.is_alive()
                }
                for t in self._threads
            ]
        }
    
    def reload_config(self) -> None:
        """重新加载配置"""
        logger.info("Reloading configuration...")
        
        # 重新加载配置文件
        self.config.reload()
        
        # 更新组件配置
        self.field_mapper = FieldMapper(self.config.sync.field_mapping)
        
        logger.info("Configuration reloaded")
    
    def reset_snapshot(self, feishu_table: str) -> None:
        """重置指定表的快照"""
        database, table = feishu_table.split(':')
        self.change_detector.reset_snapshot(database, table)
        logger.info(f"Reset snapshot for {feishu_table}")