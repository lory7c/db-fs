"""
数据库操作封装
"""
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from contextlib import contextmanager
import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB
from loguru import logger

from ..config.config import DatabaseConfig


class Database:
    """数据库操作类"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._pool = None
        self._init_pool()
    
    def _init_pool(self) -> None:
        """初始化连接池"""
        try:
            self._pool = PooledDB(
                creator=pymysql,
                maxconnections=self.config.pool_size,
                mincached=2,
                maxcached=5,
                blocking=True,
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset=self.config.charset,
                cursorclass=DictCursor
            )
            logger.info(f"Database connection pool initialized: {self.config.host}:{self.config.port}")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self) -> Connection:
        """获取数据库连接（上下文管理器）"""
        conn = None
        try:
            conn = self._pool.connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute(self, sql: str, params: Optional[Tuple] = None) -> int:
        """执行SQL语句"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                result = cursor.execute(sql, params)
                conn.commit()
                return result
            finally:
                cursor.close()
    
    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        """批量执行SQL语句"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                result = cursor.executemany(sql, params_list)
                conn.commit()
                return result
            finally:
                cursor.close()
    
    def query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """查询数据"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                return cursor.fetchall()
            finally:
                cursor.close()
    
    def query_one(self, sql: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """查询单条数据"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                return cursor.fetchone()
            finally:
                cursor.close()
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """插入数据"""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['%s'] * len(columns))
        
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, values)
                conn.commit()
                return cursor.lastrowid
            finally:
                cursor.close()
    
    def batch_insert(self, table: str, data_list: List[Dict[str, Any]]) -> int:
        """批量插入数据"""
        if not data_list:
            return 0
        
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        values_list = [tuple(data[col] for col in columns) for data in data_list]
        
        return self.execute_many(sql, values_list)
    
    def update(self, table: str, data: Dict[str, Any], 
               where: Dict[str, Any]) -> int:
        """更新数据"""
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(where.values())
        
        return self.execute(sql, params)
    
    def upsert(self, table: str, data: Dict[str, Any], 
               unique_keys: List[str]) -> int:
        """插入或更新数据"""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['%s'] * len(columns))
        
        update_clause = ', '.join([
            f"{col} = VALUES({col})" 
            for col in columns 
            if col not in unique_keys
        ])
        
        sql = f"""
            INSERT INTO {table} ({', '.join(columns)}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        return self.execute(sql, values)
    
    def delete(self, table: str, where: Dict[str, Any]) -> int:
        """删除数据"""
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        sql = f"DELETE FROM {table} WHERE {where_clause}"
        
        return self.execute(sql, list(where.values()))
    
    def table_exists(self, table: str) -> bool:
        """检查表是否存在"""
        sql = """
            SELECT COUNT(*) as count
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        """
        result = self.query_one(sql, (self.config.database, table))
        return result['count'] > 0
    
    def get_table_columns(self, table: str) -> List[Dict[str, Any]]:
        """获取表字段信息"""
        sql = """
            SELECT 
                COLUMN_NAME as name,
                DATA_TYPE as type,
                IS_NULLABLE as nullable,
                COLUMN_KEY as key,
                COLUMN_DEFAULT as default_value,
                EXTRA as extra
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ORDINAL_POSITION
        """
        return self.query(sql, (self.config.database, table))
    
    def create_sync_tables(self) -> None:
        """创建同步相关的表"""
        # 同步队列表
        self.execute("""
            CREATE TABLE IF NOT EXISTS sync_queue (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(100) NOT NULL,
                record_id VARCHAR(100) NOT NULL,
                action VARCHAR(20) NOT NULL,
                old_data JSON,
                new_data JSON,
                sync_hash VARCHAR(64),
                sync_source VARCHAR(20) DEFAULT 'database',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL,
                status VARCHAR(20) DEFAULT 'pending',
                retry_count INT DEFAULT 0,
                error_message TEXT,
                INDEX idx_status_created (status, created_at),
                INDEX idx_table_record (table_name, record_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        # 同步日志表
        self.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                sync_id VARCHAR(100) UNIQUE,
                table_name VARCHAR(100),
                record_id VARCHAR(100),
                direction VARCHAR(50),
                sync_hash VARCHAR(64),
                status VARCHAR(20),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_sync_hash (sync_hash),
                INDEX idx_created_at (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        # ID映射表
        self.execute("""
            CREATE TABLE IF NOT EXISTS id_mapping (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(100) NOT NULL,
                db_id VARCHAR(100) NOT NULL,
                feishu_id VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_table_db_id (table_name, db_id),
                UNIQUE KEY uk_table_feishu_id (table_name, feishu_id),
                INDEX idx_updated_at (updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        logger.info("Sync tables created/verified")
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            self.query_one("SELECT 1 as test")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False