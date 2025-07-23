"""数据库测试"""

import os
import pytest
from typing import Dict, Any

from feishu_bitable_db import DB, Table, Field, FieldType, SearchCmd
from feishu_bitable_db.db.db import DBImpl
from feishu_bitable_db.internal.faker import APP_ID, APP_SECRET


class TestDB:
    """数据库测试类"""
    
    @pytest.fixture
    def db(self) -> DB:
        """创建数据库实例"""
        if not APP_ID or not APP_SECRET:
            pytest.skip("需要设置 app_id 和 app_secret 环境变量")
        
        return DBImpl(APP_ID, APP_SECRET)
    
    @pytest.fixture
    def test_data(self) -> Dict[str, Any]:
        """测试数据"""
        return {
            "db_name": "test-database",
            "table_name": "test-table",
        }
    
    def test_create_database(self, db: DB, test_data: Dict[str, Any]):
        """测试创建数据库"""
        db_name = test_data["db_name"]
        
        # 第一次创建
        did = db.save_database(db_name)
        assert did
        assert did.startswith("bascn")
        
        # 第二次创建（应该返回相同的 ID）
        did2 = db.save_database(db_name)
        assert did2 == did
    
    def test_create_table(self, db: DB, test_data: Dict[str, Any]):
        """测试创建表"""
        db_name = test_data["db_name"]
        table_name = test_data["table_name"]
        
        # 创建表
        table = Table(
            name=table_name,
            fields=[
                Field(name="username", type=FieldType.STRING),
                Field(name="age", type=FieldType.INT),
            ]
        )
        
        tid = db.save_table(db_name, table)
        assert tid
        
        # 更新表（添加字段）
        table2 = Table(
            name=table_name,
            fields=[
                Field(name="username", type=FieldType.STRING),
                Field(name="passport", type=FieldType.STRING),
                Field(name="age", type=FieldType.INT),
            ]
        )
        
        tid2 = db.save_table(db_name, table2)
        assert tid2 == tid
    
    def test_crud_record(self, db: DB, test_data: Dict[str, Any]):
        """测试记录的增删改查"""
        db_name = test_data["db_name"]
        table_name = test_data["table_name"]
        
        # 确保表存在
        table = Table(
            name=table_name,
            fields=[
                Field(name="username", type=FieldType.STRING),
                Field(name="age", type=FieldType.INT),
            ]
        )
        db.save_table(db_name, table)
        
        # 创建记录
        record = {
            "username": "zhangsan",
            "age": 12,
        }
        record_id = db.create(db_name, table_name, record)
        assert record_id
        
        # 更新记录
        update_data = {
            "username": "zhangsan13",
            "age": 13,
        }
        db.update(db_name, table_name, record_id, update_data)
        
        # 删除记录
        db.delete(db_name, table_name, record_id)
    
    def test_read_records(self, db: DB, test_data: Dict[str, Any]):
        """测试查询记录"""
        db_name = test_data["db_name"]
        table_name = test_data["table_name"]
        
        # 查询记录
        search_cmds = [
            SearchCmd(key="age", operator="=", val=12)
        ]
        results = db.read(db_name, table_name, search_cmds)
        
        print(f"查询结果: {results}")
        assert isinstance(results, list)