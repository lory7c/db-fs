"""
同步功能测试
"""
import unittest
import json
import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from feishu_db_sync.config.config import Config, DatabaseConfig, FeishuConfig, SyncConfig
from feishu_db_sync.feishu.client import FeishuClient
from feishu_db_sync.feishu.change_detector import ChangeDetector, ChangeRecord
from feishu_db_sync.db.database import Database
from feishu_db_sync.db.queue_processor import QueueProcessor
from feishu_db_sync.core.field_mapper import FieldMapper
from feishu_db_sync.core.sync_worker import SyncWorker


class TestFieldMapper(unittest.TestCase):
    """字段映射器测试"""
    
    def setUp(self):
        self.field_mapping = {
            "users": {
                "姓名": "name",
                "年龄": "age",
                "邮箱": "email"
            }
        }
        self.mapper = FieldMapper(self.field_mapping)
    
    def test_feishu_to_db(self):
        """测试飞书到数据库的字段映射"""
        feishu_record = {
            "id": "rec123",
            "姓名": "张三",
            "年龄": 25,
            "邮箱": "zhangsan@example.com"
        }
        
        db_record = self.mapper.feishu_to_db("users", feishu_record)
        
        self.assertEqual(db_record["name"], "张三")
        self.assertEqual(db_record["age"], 25)
        self.assertEqual(db_record["email"], "zhangsan@example.com")
        self.assertEqual(db_record["feishu_id"], "rec123")
    
    def test_db_to_feishu(self):
        """测试数据库到飞书的字段映射"""
        db_record = {
            "id": 1,
            "name": "李四",
            "age": 30,
            "email": "lisi@example.com",
            "feishu_id": "rec456"
        }
        
        feishu_record = self.mapper.db_to_feishu("users", db_record)
        
        self.assertEqual(feishu_record["姓名"], "李四")
        self.assertEqual(feishu_record["年龄"], 30)
        self.assertEqual(feishu_record["邮箱"], "lisi@example.com")
        self.assertNotIn("id", feishu_record)
        self.assertNotIn("feishu_id", feishu_record)


class TestChangeDetector(unittest.TestCase):
    """变更检测器测试"""
    
    def setUp(self):
        self.feishu_client = Mock(spec=FeishuClient)
        self.detector = ChangeDetector(self.feishu_client, None)
    
    def test_detect_new_record(self):
        """测试检测新增记录"""
        # 模拟飞书返回数据
        self.feishu_client.read_all_records.return_value = [
            {"id": "rec1", "name": "Test1", "age": 20},
            {"id": "rec2", "name": "Test2", "age": 25}
        ]
        
        # 第一次检测（所有记录都是新的）
        changes = self.detector.detect_changes("TestDB", "users")
        
        self.assertEqual(len(changes), 2)
        self.assertTrue(all(c.action == 'insert' for c in changes))
    
    def test_detect_updated_record(self):
        """测试检测更新记录"""
        # 第一次检测
        self.feishu_client.read_all_records.return_value = [
            {"id": "rec1", "name": "Test1", "age": 20}
        ]
        self.detector.detect_changes("TestDB", "users")
        
        # 修改数据后第二次检测
        self.feishu_client.read_all_records.return_value = [
            {"id": "rec1", "name": "Test1", "age": 21}  # age 改变了
        ]
        changes = self.detector.detect_changes("TestDB", "users")
        
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].action, 'update')
        self.assertEqual(changes[0].new_data['age'], 21)
    
    def test_detect_deleted_record(self):
        """测试检测删除记录"""
        # 第一次检测
        self.feishu_client.read_all_records.return_value = [
            {"id": "rec1", "name": "Test1", "age": 20},
            {"id": "rec2", "name": "Test2", "age": 25}
        ]
        self.detector.detect_changes("TestDB", "users")
        
        # 删除一条记录后第二次检测
        self.feishu_client.read_all_records.return_value = [
            {"id": "rec1", "name": "Test1", "age": 20}
        ]
        changes = self.detector.detect_changes("TestDB", "users")
        
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].action, 'delete')
        self.assertEqual(changes[0].record_id, 'rec2')


class TestQueueProcessor(unittest.TestCase):
    """队列处理器测试"""
    
    def setUp(self):
        self.db = Mock(spec=Database)
        self.processor = QueueProcessor(self.db)
    
    def test_add_to_queue(self):
        """测试添加到队列"""
        self.db.insert.return_value = 1
        
        queue_id = self.processor.add_to_queue(
            table_name="users",
            record_id="123",
            action="INSERT",
            new_data={"name": "Test"}
        )
        
        self.assertEqual(queue_id, 1)
        self.db.insert.assert_called_once()
    
    def test_check_sync_loop(self):
        """测试循环同步检测"""
        # 模拟存在相同哈希的同步记录
        self.db.query_one.return_value = {'count': 1}
        
        is_loop = self.processor.check_sync_loop(
            sync_hash="test_hash",
            direction="feishu_to_db"
        )
        
        self.assertTrue(is_loop)
    
    def test_get_queue_stats(self):
        """测试获取队列统计"""
        self.db.query.return_value = [
            {'status': 'pending', 'count': 10, 'oldest': datetime.now(), 'newest': datetime.now()},
            {'status': 'completed', 'count': 50, 'oldest': datetime.now(), 'newest': datetime.now()}
        ]
        
        stats = self.processor.get_queue_stats()
        
        self.assertEqual(stats['total'], 60)
        self.assertEqual(stats['by_status']['pending'], 10)
        self.assertEqual(stats['by_status']['completed'], 50)


class TestSyncWorker(unittest.TestCase):
    """同步工作器测试"""
    
    def setUp(self):
        self.feishu_client = Mock(spec=FeishuClient)
        self.database = Mock(spec=Database)
        self.queue_processor = Mock(spec=QueueProcessor)
        self.field_mapper = Mock(spec=FieldMapper)
        
        self.worker = SyncWorker(
            self.feishu_client,
            self.database,
            self.queue_processor,
            self.field_mapper
        )
    
    def test_sync_feishu_to_db_insert(self):
        """测试飞书到数据库的插入同步"""
        change = ChangeRecord(
            record_id="rec123",
            action='insert',
            new_data={"name": "Test", "age": 20}
        )
        change.hash = "test_hash"
        
        # 模拟没有循环同步
        self.queue_processor.check_sync_loop.return_value = False
        
        # 模拟字段映射
        self.field_mapper.feishu_to_db.return_value = {
            "name": "Test",
            "age": 20,
            "feishu_id": "rec123"
        }
        
        # 模拟数据库插入返回ID
        self.database.insert.return_value = 1
        
        result = self.worker.sync_feishu_to_db("TestDB:users", "users", change)
        
        self.assertTrue(result)
        self.database.insert.assert_called_once()
        self.queue_processor.log_sync.assert_called_once()
    
    def test_sync_db_to_feishu_update(self):
        """测试数据库到飞书的更新同步"""
        from feishu_db_sync.db.models import SyncQueue, SyncAction
        
        queue_item = SyncQueue(
            id=1,
            table_name="users",
            record_id="123",
            action=SyncAction.UPDATE.value,
            new_data={"name": "Updated", "age": 25},
            sync_hash="test_hash"
        )
        
        # 模拟没有循环同步
        self.queue_processor.check_sync_loop.return_value = False
        
        # 模拟获取飞书ID
        self.queue_processor.get_feishu_id.return_value = "rec123"
        
        # 模拟字段映射
        self.field_mapper.db_to_feishu.return_value = {
            "姓名": "Updated",
            "年龄": 25
        }
        
        result = self.worker.sync_db_to_feishu(queue_item, "TestDB", "users")
        
        self.assertTrue(result)
        self.feishu_client.update_record.assert_called_once()
        self.queue_processor.mark_completed.assert_called_once_with(1)


class TestIntegration(unittest.TestCase):
    """集成测试"""
    
    @patch('feishu_db_sync.core.sync_service.Database')
    @patch('feishu_db_sync.core.sync_service.FeishuClient')
    def test_sync_service_initialization(self, mock_feishu, mock_db):
        """测试同步服务初始化"""
        # 创建配置
        config = Config()
        config.database = DatabaseConfig(host="localhost", database="test")
        config.feishu = FeishuConfig(app_id="test_id", app_secret="test_secret")
        config.sync = SyncConfig(table_mapping={"TestDB:users": "users"})
        
        # 模拟连接测试成功
        mock_feishu.return_value.test_connection.return_value = True
        mock_db.return_value.test_connection.return_value = True
        
        from feishu_db_sync.core.sync_service import SyncService
        
        # 创建服务
        service = SyncService(config)
        
        # 验证组件初始化
        self.assertIsNotNone(service.feishu_client)
        self.assertIsNotNone(service.database)
        self.assertIsNotNone(service.change_detector)
        self.assertIsNotNone(service.queue_processor)


if __name__ == '__main__':
    unittest.main()