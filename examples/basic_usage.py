"""基本使用示例"""

import os
from feishu_bitable_db import DB, Table, Field, FieldType, SearchCmd
from feishu_bitable_db.db.db import DBImpl


def main():
    """主函数"""
    # 从环境变量获取凭证
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    
    if not app_id or not app_secret:
        print("请设置环境变量 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        return
    
    # 初始化数据库
    print("初始化数据库连接...")
    db = DBImpl(app_id, app_secret)
    
    # 数据库和表名
    db_name = "示例数据库"
    table_name = "用户表"
    
    # 创建数据库
    print(f"创建数据库: {db_name}")
    db_id = db.save_database(db_name)
    print(f"数据库 ID: {db_id}")
    
    # 定义表结构
    print(f"\n创建表: {table_name}")
    table = Table(
        name=table_name,
        fields=[
            Field(name="用户名", type=FieldType.STRING),
            Field(name="年龄", type=FieldType.INT),
            Field(name="邮箱", type=FieldType.STRING),
            Field(name="注册日期", type=FieldType.DATE),
            Field(name="会员类型", type=FieldType.RADIO),
        ]
    )
    table_id = db.save_table(db_name, table)
    print(f"表 ID: {table_id}")
    
    # 列出所有表
    print(f"\n数据库 {db_name} 中的所有表:")
    tables = db.list_tables(db_name)
    for t in tables:
        print(f"  - {t}")
    
    # 插入记录
    print("\n插入测试数据...")
    users = [
        {"用户名": "张三", "年龄": 25, "邮箱": "zhangsan@example.com", "会员类型": "普通会员"},
        {"用户名": "李四", "年龄": 30, "邮箱": "lisi@example.com", "会员类型": "VIP会员"},
        {"用户名": "王五", "年龄": 28, "邮箱": "wangwu@example.com", "会员类型": "普通会员"},
    ]
    
    record_ids = []
    for user in users:
        record_id = db.create(db_name, table_name, user)
        record_ids.append(record_id)
        print(f"  插入记录: {user['用户名']} (ID: {record_id})")
    
    # 查询所有记录
    print("\n查询所有记录:")
    all_records = db.read(db_name, table_name, [])
    for record in all_records:
        print(f"  {record}")
    
    # 条件查询
    print("\n查询年龄大于等于28的用户:")
    search_cmds = [SearchCmd(key="年龄", operator=">=", val=28)]
    filtered_records = db.read(db_name, table_name, search_cmds)
    for record in filtered_records:
        print(f"  {record.get('用户名')} - 年龄: {record.get('年龄')}")
    
    # 更新记录
    if record_ids:
        print(f"\n更新第一个用户的年龄...")
        db.update(db_name, table_name, record_ids[0], {"年龄": 26})
        
        # 查询更新后的记录
        updated_records = db.read(db_name, table_name, [
            SearchCmd(key="id", operator="=", val=record_ids[0])
        ])
        if updated_records:
            print(f"  更新后: {updated_records[0]}")
    
    # 删除记录
    if len(record_ids) > 2:
        print(f"\n删除最后一个用户...")
        db.delete(db_name, table_name, record_ids[-1])
        print("  删除成功")
    
    # 修改表结构
    print("\n修改表结构，添加新字段...")
    new_table = Table(
        name=table_name,
        fields=[
            Field(name="用户名", type=FieldType.STRING),
            Field(name="年龄", type=FieldType.INT),
            Field(name="邮箱", type=FieldType.STRING),
            Field(name="注册日期", type=FieldType.DATE),
            Field(name="会员类型", type=FieldType.RADIO),
            Field(name="积分", type=FieldType.INT),  # 新增字段
        ]
    )
    db.save_table(db_name, new_table)
    print("  表结构更新成功")
    
    print("\n示例程序执行完成！")


if __name__ == "__main__":
    main()