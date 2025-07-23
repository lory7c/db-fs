#!/usr/bin/env python3
"""
简化版的双向实时同步服务
使用轮询 + 数据库触发器实现准实时同步
"""
import time
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import threading
import pymysql
from loguru import logger

from feishu_bitable_db.db.db import DBImpl
from feishu_bitable_db.db.types import SearchCmd


class RealtimeSyncService:
    """实时同步服务"""
    
    def __init__(self, app_id: str, app_secret: str, db_config: Dict[str, Any]):
        # 初始化飞书客户端
        self.feishu = DBImpl(app_id, app_secret)
        
        # 数据库配置
        self.db_config = db_config
        
        # 同步映射配置
        # 格式: {"飞书数据库:飞书表": "MySQL表"}
        self.table_mapping = {
            "MyDB:users": "users",
            "MyDB:orders": "orders"
        }
        
        # 轮询间隔（秒）
        self.poll_interval = 5
        
        # 内存缓存，存储表格快照
        self.snapshots = {}
        
        # 运行标志
        self.running = False
    
    def get_db_connection(self):
        """获取数据库连接"""
        return pymysql.connect(
            host=self.db_config['host'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """计算记录的哈希值"""
        # 移除系统字段
        data = {k: v for k, v in record.items() 
                if k not in ['id', 'created_at', 'updated_at', '_sync_source']}
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    def sync_feishu_to_db(self):
        """从飞书同步到数据库"""
        logger.info("Starting Feishu to DB sync...")
        
        for feishu_table, db_table in self.table_mapping.items():
            try:
                feishu_db, feishu_table_name = feishu_table.split(':')
                
                # 获取飞书表格所有记录
                records = self.feishu.read(feishu_db, feishu_table_name, [])
                
                # 获取当前快照
                snapshot_key = f"{feishu_db}:{feishu_table_name}"
                old_snapshot = self.snapshots.get(snapshot_key, {})
                new_snapshot = {}
                
                # 处理每条记录
                with self.get_db_connection() as conn:
                    cursor = conn.cursor()
                    
                    for record in records:
                        record_id = record['id']
                        record_hash = self.calculate_record_hash(record)
                        new_snapshot[record_id] = record_hash
                        
                        # 检查是否有变更
                        if old_snapshot.get(record_id) != record_hash:
                            # 检查同步日志，避免循环同步
                            cursor.execute("""
                                SELECT COUNT(*) as count FROM sync_log 
                                WHERE sync_hash = %s 
                                AND direction = 'db_to_feishu'
                                AND created_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)
                            """, (record_hash,))
                            
                            if cursor.fetchone()['count'] == 0:
                                # 同步到数据库
                                self._sync_record_to_db(cursor, db_table, record)
                                
                                # 记录同步日志
                                cursor.execute("""
                                    INSERT INTO sync_log 
                                    (sync_id, table_name, record_id, direction, sync_hash, status)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE status = %s
                                """, (
                                    f"{db_table}_{record_id}_{record_hash}",
                                    db_table,
                                    record_id,
                                    'feishu_to_db',
                                    record_hash,
                                    'completed',
                                    'completed'
                                ))
                    
                    # 处理删除的记录
                    for old_id in set(old_snapshot.keys()) - set(new_snapshot.keys()):
                        cursor.execute(f"""
                            DELETE FROM {db_table} WHERE feishu_id = %s
                        """, (old_id,))
                        logger.info(f"Deleted record {old_id} from {db_table}")
                    
                    conn.commit()
                
                # 更新快照
                self.snapshots[snapshot_key] = new_snapshot
                
            except Exception as e:
                logger.error(f"Error syncing {feishu_table} to DB: {e}")
    
    def _sync_record_to_db(self, cursor, table: str, record: Dict[str, Any]):
        """同步单条记录到数据库"""
        # 移除飞书特有字段
        data = {k: v for k, v in record.items() if k != 'id'}
        data['feishu_id'] = record['id']
        
        # 构建 UPSERT 语句
        columns = list(data.keys())
        values = list(data.values())
        
        # ON DUPLICATE KEY UPDATE
        update_pairs = [f"{col} = VALUES({col})" for col in columns if col != 'feishu_id']
        
        sql = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON DUPLICATE KEY UPDATE {', '.join(update_pairs)}
        """
        
        cursor.execute(sql, values)
        logger.info(f"Synced record {record['id']} to {table}")
    
    def sync_db_to_feishu(self):
        """从数据库同步到飞书"""
        logger.info("Starting DB to Feishu sync...")
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 获取待同步的记录
            cursor.execute("""
                SELECT q.*, l.created_at as last_sync
                FROM sync_queue q
                LEFT JOIN sync_log l ON q.sync_hash = l.sync_hash 
                    AND l.direction = 'feishu_to_db'
                WHERE q.status = 'pending' 
                AND q.retry_count < 3
                AND (l.created_at IS NULL OR l.created_at < DATE_SUB(NOW(), INTERVAL 10 SECOND))
                ORDER BY q.created_at ASC
                LIMIT 50
            """)
            
            queue_items = cursor.fetchall()
            
            for item in queue_items:
                try:
                    # 找到对应的飞书表
                    feishu_table = None
                    for ft, dt in self.table_mapping.items():
                        if dt == item['table_name']:
                            feishu_table = ft
                            break
                    
                    if not feishu_table:
                        continue
                    
                    feishu_db, feishu_table_name = feishu_table.split(':')
                    
                    # 执行同步
                    if item['action'] == 'INSERT':
                        self._insert_to_feishu(feishu_db, feishu_table_name, item['new_data'])
                    elif item['action'] == 'UPDATE':
                        self._update_to_feishu(feishu_db, feishu_table_name, 
                                             item['record_id'], item['new_data'])
                    elif item['action'] == 'DELETE':
                        self._delete_from_feishu(feishu_db, feishu_table_name, 
                                               item['record_id'])
                    
                    # 标记为已处理
                    cursor.execute("""
                        UPDATE sync_queue 
                        SET status = 'completed', processed_at = NOW()
                        WHERE id = %s
                    """, (item['id'],))
                    
                    # 记录同步日志
                    cursor.execute("""
                        INSERT INTO sync_log 
                        (sync_id, table_name, record_id, direction, sync_hash, status)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE status = %s
                    """, (
                        f"{item['table_name']}_{item['record_id']}_{item['sync_hash']}",
                        item['table_name'],
                        item['record_id'],
                        'db_to_feishu',
                        item['sync_hash'],
                        'completed',
                        'completed'
                    ))
                    
                except Exception as e:
                    logger.error(f"Error syncing queue item {item['id']}: {e}")
                    # 增加重试计数
                    cursor.execute("""
                        UPDATE sync_queue 
                        SET retry_count = retry_count + 1,
                            error_message = %s
                        WHERE id = %s
                    """, (str(e), item['id']))
            
            conn.commit()
    
    def _insert_to_feishu(self, database: str, table: str, data: Dict[str, Any]):
        """插入记录到飞书"""
        # 解析 JSON 数据
        if isinstance(data, str):
            data = json.loads(data)
        
        # 移除数据库特有字段
        feishu_data = {k: v for k, v in data.items() 
                      if k not in ['id', 'feishu_id', 'created_at', 'updated_at']}
        
        record_id = self.feishu.create(database, table, feishu_data)
        logger.info(f"Created record {record_id} in Feishu {database}:{table}")
    
    def _update_to_feishu(self, database: str, table: str, record_id: str, data: Dict[str, Any]):
        """更新飞书记录"""
        if isinstance(data, str):
            data = json.loads(data)
        
        # 先查找飞书记录 ID
        feishu_id = self._find_feishu_record_id(database, table, record_id)
        if not feishu_id:
            # 如果找不到，创建新记录
            self._insert_to_feishu(database, table, data)
            return
        
        feishu_data = {k: v for k, v in data.items() 
                      if k not in ['id', 'feishu_id', 'created_at', 'updated_at']}
        
        self.feishu.update(database, table, feishu_id, feishu_data)
        logger.info(f"Updated record {feishu_id} in Feishu {database}:{table}")
    
    def _delete_from_feishu(self, database: str, table: str, record_id: str):
        """从飞书删除记录"""
        feishu_id = self._find_feishu_record_id(database, table, record_id)
        if feishu_id:
            self.feishu.delete(database, table, feishu_id)
            logger.info(f"Deleted record {feishu_id} from Feishu {database}:{table}")
    
    def _find_feishu_record_id(self, database: str, table: str, db_record_id: str) -> Optional[str]:
        """根据数据库记录 ID 查找飞书记录 ID"""
        # 这里需要维护一个 ID 映射关系
        # 简化示例：假设有一个字段存储了数据库 ID
        records = self.feishu.read(database, table, [
            SearchCmd(key="db_id", operator="=", val=db_record_id)
        ])
        return records[0]['id'] if records else None
    
    def run_sync_loop(self):
        """运行同步循环"""
        while self.running:
            try:
                # 飞书到数据库
                self.sync_feishu_to_db()
                
                # 数据库到飞书
                self.sync_db_to_feishu()
                
                # 清理旧日志
                self._cleanup_old_logs()
                
            except Exception as e:
                logger.error(f"Sync loop error: {e}")
            
            time.sleep(self.poll_interval)
    
    def _cleanup_old_logs(self):
        """清理旧的同步日志"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM sync_log 
                WHERE created_at < DATE_SUB(NOW(), INTERVAL 1 DAY)
            """)
            cursor.execute("""
                DELETE FROM sync_queue 
                WHERE status = 'completed' 
                AND processed_at < DATE_SUB(NOW(), INTERVAL 1 HOUR)
            """)
            conn.commit()
    
    def start(self):
        """启动同步服务"""
        logger.info("Starting realtime sync service...")
        self.running = True
        
        # 在新线程中运行同步循环
        sync_thread = threading.Thread(target=self.run_sync_loop)
        sync_thread.daemon = True
        sync_thread.start()
        
        logger.info("Sync service started successfully")
    
    def stop(self):
        """停止同步服务"""
        logger.info("Stopping sync service...")
        self.running = False


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logger.add("sync_service.log", rotation="500 MB", level="INFO")
    
    # 创建同步服务
    service = RealtimeSyncService(
        app_id="your_app_id",
        app_secret="your_app_secret",
        db_config={
            'host': 'localhost',
            'user': 'root',
            'password': 'password',
            'database': 'mydb'
        }
    )
    
    # 启动服务
    service.start()
    
    try:
        # 保持运行
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        service.stop()
        logger.info("Service stopped")