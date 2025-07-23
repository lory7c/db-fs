#!/usr/bin/env python3
"""
飞书多维表格与数据库双向同步服务
主程序入口
"""
import os
import sys
import signal
import time
import argparse
from pathlib import Path
from loguru import logger

# 添加项目路径到系统路径
sys.path.insert(0, str(Path(__file__).parent))

from feishu_db_sync.config.config import Config
from feishu_db_sync.core.sync_service import SyncService
from feishu_db_sync.monitor.logger import setup_logger


class SyncApplication:
    """同步应用主类"""
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path
        self.sync_service = None
        self.running = False
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def initialize(self):
        """初始化应用"""
        try:
            # 加载配置
            config = Config(self.config_path)
            
            # 设置日志
            setup_logger(config.monitor)
            
            logger.info("=" * 60)
            logger.info("Feishu Database Sync Service")
            logger.info("=" * 60)
            logger.info(f"Config file: {config.config_path}")
            logger.info(f"Log level: {config.monitor.log_level}")
            logger.info(f"Poll interval: {config.sync.poll_interval}s")
            
            # 创建同步服务
            self.sync_service = SyncService(config)
            
            logger.info("Application initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            raise
    
    def start(self):
        """启动应用"""
        if not self.sync_service:
            raise RuntimeError("Application not initialized")
        
        try:
            self.running = True
            
            # 启动同步服务
            self.sync_service.start()
            
            logger.info("Application started, press Ctrl+C to stop")
            
            # 主循环
            while self.running:
                try:
                    # 定期输出状态
                    time.sleep(60)
                    if self.running:
                        self._print_status()
                except KeyboardInterrupt:
                    break
                
        except Exception as e:
            logger.error(f"Application error: {e}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """停止应用"""
        if not self.running:
            return
        
        self.running = False
        
        if self.sync_service:
            self.sync_service.stop()
        
        logger.info("Application stopped")
    
    def _print_status(self):
        """打印状态信息"""
        if not self.sync_service:
            return
        
        status = self.sync_service.get_status()
        
        logger.info("-" * 50)
        logger.info("Sync Service Status")
        logger.info("-" * 50)
        logger.info(f"Running: {status['running']}")
        logger.info(f"Uptime: {status['uptime_seconds']:.0f} seconds")
        
        sync_stats = status['sync_stats']
        logger.info(f"Feishu→DB: {sync_stats['feishu_to_db_success']} success, "
                   f"{sync_stats['feishu_to_db_failed']} failed")
        logger.info(f"DB→Feishu: {sync_stats['db_to_feishu_success']} success, "
                   f"{sync_stats['db_to_feishu_failed']} failed")
        
        queue_stats = status['queue_stats']
        logger.info(f"Queue: {queue_stats.get('total', 0)} total, "
                   f"{queue_stats.get('by_status', {}).get('pending', 0)} pending")
        logger.info("-" * 50)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Feishu Database Bidirectional Sync Service'
    )
    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file',
        default=None
    )
    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize configuration file'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test connections and exit'
    )
    parser.add_argument(
        '--reset-snapshot',
        help='Reset snapshot for specified table (format: DB:table)'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show service status (requires running service)'
    )
    
    args = parser.parse_args()
    
    # 初始化配置文件
    if args.init:
        config = Config(args.config)
        config.save()
        print(f"Configuration file created: {config.config_path}")
        print("Please edit the configuration file and run the service again")
        return
    
    # 创建应用实例
    app = SyncApplication(args.config)
    app.initialize()
    
    # 测试连接
    if args.test:
        logger.info("Testing connections...")
        # 测试已在初始化时完成
        logger.info("Connection test completed")
        return
    
    # 重置快照
    if args.reset_snapshot:
        app.sync_service.reset_snapshot(args.reset_snapshot)
        logger.info(f"Snapshot reset for {args.reset_snapshot}")
        return
    
    # 显示状态
    if args.status:
        # TODO: 实现通过 API 或共享内存获取运行中服务的状态
        logger.warning("Status command requires running service")
        return
    
    # 启动服务
    try:
        app.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()