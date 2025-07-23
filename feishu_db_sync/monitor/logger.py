"""
日志配置
"""
import sys
from pathlib import Path
from loguru import logger

from ..config.config import MonitorConfig


def setup_logger(config: MonitorConfig) -> None:
    """配置日志"""
    # 移除默认的日志处理器
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sys.stdout,
        level=config.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # 添加文件输出
    if config.log_file:
        logger.add(
            config.log_file,
            level=config.log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation=config.log_max_size,
            retention=config.log_backup_count,
            compression="zip",
            encoding="utf-8"
        )
    
    # 添加错误日志文件
    error_log_file = Path(config.log_file).with_suffix('.error.log')
    logger.add(
        str(error_log_file),
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation=config.log_max_size,
        retention=config.log_backup_count * 2,  # 错误日志保留更长时间
        compression="zip",
        encoding="utf-8"
    )
    
    logger.info(f"Logger initialized with level: {config.log_level}")