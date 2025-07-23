"""高级使用示例"""

import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any

from feishu_bitable_db import DB, Table, Field, FieldType, SearchCmd
from feishu_bitable_db.db.db import DBImpl


class UserManager:
    """用户管理器示例"""
    
    def __init__(self, db: DB, db_name: str):
        self.db = db
        self.db_name = db_name
        self.table_name = "高级用户表"
        self._init_table()
    
    def _init_table(self):
        """初始化表结构"""
        table = Table(
            name=self.table_name,
            fields=[
                Field(name="用户ID", type=FieldType.STRING),
                Field(name="用户名", type=FieldType.STRING),
                Field(name="密码哈希", type=FieldType.STRING),
                Field(name="邮箱", type=FieldType.STRING),
                Field(name="手机号", type=FieldType.STRING),
                Field(name="年龄", type=FieldType.INT),
                Field(name="性别", type=FieldType.RADIO),
                Field(name="兴趣爱好", type=FieldType.MULTI_SELECT),
                Field(name="注册时间", type=FieldType.DATE),
                Field(name="最后登录", type=FieldType.DATE),
                Field(name="状态", type=FieldType.RADIO),
                Field(name="备注", type=FieldType.STRING),
            ]
        )
        self.db.save_table(self.db_name, table)
    
    def create_user(self, user_data: Dict[str, Any]) -> str:
        """创建用户"""
        # 生成用户ID
        user_data["用户ID"] = f"USER_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        user_data["注册时间"] = datetime.now().isoformat()
        user_data["状态"] = "活跃"
        
        return self.db.create(self.db_name, self.table_name, user_data)
    
    def get_user_by_username(self, username: str) -> Dict[str, Any]:
        """根据用户名获取用户"""
        users = self.db.read(self.db_name, self.table_name, [
            SearchCmd(key="用户名", operator="=", val=username)
        ])
        return users[0] if users else None
    
    def get_users_by_age_range(self, min_age: int, max_age: int) -> List[Dict[str, Any]]:
        """获取年龄范围内的用户"""
        # 注意：飞书多维表格的过滤条件有限制，这里使用 >= 操作符
        users = self.db.read(self.db_name, self.table_name, [
            SearchCmd(key="年龄", operator=">=", val=min_age)
        ])
        # 在内存中进一步过滤
        return [u for u in users if u.get("年龄", 0) <= max_age]
    
    def update_last_login(self, user_id: str):
        """更新最后登录时间"""
        self.db.update(self.db_name, self.table_name, user_id, {
            "最后登录": datetime.now().isoformat()
        })
    
    def deactivate_user(self, user_id: str):
        """停用用户"""
        self.db.update(self.db_name, self.table_name, user_id, {
            "状态": "停用"
        })
    
    def search_users(self, **kwargs) -> List[Dict[str, Any]]:
        """多条件搜索用户"""
        search_cmds = []
        for key, value in kwargs.items():
            if value is not None:
                search_cmds.append(SearchCmd(key=key, operator="=", val=value))
        
        return self.db.read(self.db_name, self.table_name, search_cmds)


class ProductManager:
    """产品管理器示例"""
    
    def __init__(self, db: DB, db_name: str):
        self.db = db
        self.db_name = db_name
        self.table_name = "产品目录"
        self._init_table()
    
    def _init_table(self):
        """初始化产品表"""
        table = Table(
            name=self.table_name,
            fields=[
                Field(name="产品编号", type=FieldType.STRING),
                Field(name="产品名称", type=FieldType.STRING),
                Field(name="分类", type=FieldType.RADIO),
                Field(name="标签", type=FieldType.MULTI_SELECT),
                Field(name="价格", type=FieldType.INT),
                Field(name="库存", type=FieldType.INT),
                Field(name="描述", type=FieldType.STRING),
                Field(name="上架时间", type=FieldType.DATE),
                Field(name="状态", type=FieldType.RADIO),
            ]
        )
        self.db.save_table(self.db_name, table)
    
    def add_product(self, product: Dict[str, Any]) -> str:
        """添加产品"""
        product["产品编号"] = f"PROD_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        product["上架时间"] = datetime.now().isoformat()
        product["状态"] = "在售"
        
        return self.db.create(self.db_name, self.table_name, product)
    
    def update_stock(self, product_id: str, stock: int):
        """更新库存"""
        self.db.update(self.db_name, self.table_name, product_id, {
            "库存": stock
        })
    
    def get_products_by_category(self, category: str) -> List[Dict[str, Any]]:
        """按分类获取产品"""
        return self.db.read(self.db_name, self.table_name, [
            SearchCmd(key="分类", operator="=", val=category)
        ])
    
    def get_low_stock_products(self, threshold: int = 10) -> List[Dict[str, Any]]:
        """获取低库存产品"""
        # 获取所有产品，然后在内存中过滤
        all_products = self.db.read(self.db_name, self.table_name, [])
        return [p for p in all_products if p.get("库存", 0) < threshold]


def main():
    """主函数"""
    # 初始化
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    
    if not app_id or not app_secret:
        print("请设置环境变量 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
        return
    
    db = DBImpl(app_id, app_secret)
    db_name = "高级示例数据库"
    
    # 创建数据库
    print("创建数据库...")
    db.save_database(db_name)
    
    # 用户管理示例
    print("\n=== 用户管理示例 ===")
    user_manager = UserManager(db, db_name)
    
    # 创建用户
    print("创建用户...")
    user1_id = user_manager.create_user({
        "用户名": "alice",
        "密码哈希": "hash123",
        "邮箱": "alice@example.com",
        "手机号": "13800138000",
        "年龄": 25,
        "性别": "女",
        "兴趣爱好": "阅读,音乐,运动",
    })
    print(f"创建用户 alice, ID: {user1_id}")
    
    user2_id = user_manager.create_user({
        "用户名": "bob",
        "密码哈希": "hash456",
        "邮箱": "bob@example.com",
        "手机号": "13900139000",
        "年龄": 30,
        "性别": "男",
        "兴趣爱好": "游戏,电影",
    })
    print(f"创建用户 bob, ID: {user2_id}")
    
    # 查询用户
    print("\n查询用户 alice...")
    alice = user_manager.get_user_by_username("alice")
    if alice:
        print(f"找到用户: {alice}")
    
    # 更新最后登录时间
    print("\n更新 alice 的最后登录时间...")
    user_manager.update_last_login(user1_id)
    
    # 查询年龄范围
    print("\n查询 20-30 岁的用户...")
    young_users = user_manager.get_users_by_age_range(20, 30)
    for user in young_users:
        print(f"  {user.get('用户名')} - {user.get('年龄')}岁")
    
    # 产品管理示例
    print("\n=== 产品管理示例 ===")
    product_manager = ProductManager(db, db_name)
    
    # 添加产品
    print("添加产品...")
    products = [
        {
            "产品名称": "智能手机",
            "分类": "电子产品",
            "标签": "热销,新品",
            "价格": 2999,
            "库存": 50,
            "描述": "最新款智能手机",
        },
        {
            "产品名称": "笔记本电脑",
            "分类": "电子产品",
            "标签": "办公,游戏",
            "价格": 5999,
            "库存": 5,
            "描述": "高性能笔记本",
        },
        {
            "产品名称": "运动鞋",
            "分类": "服装",
            "标签": "运动,潮流",
            "价格": 399,
            "库存": 100,
            "描述": "舒适运动鞋",
        }
    ]
    
    product_ids = []
    for product in products:
        pid = product_manager.add_product(product)
        product_ids.append(pid)
        print(f"  添加产品: {product['产品名称']} (ID: {pid})")
    
    # 查询分类产品
    print("\n查询电子产品...")
    electronics = product_manager.get_products_by_category("电子产品")
    for product in electronics:
        print(f"  {product.get('产品名称')} - ¥{product.get('价格')}")
    
    # 查询低库存产品
    print("\n查询库存低于10的产品...")
    low_stock = product_manager.get_low_stock_products(10)
    for product in low_stock:
        print(f"  {product.get('产品名称')} - 库存: {product.get('库存')}")
    
    # 更新库存
    if product_ids and len(product_ids) > 1:
        print(f"\n补充笔记本电脑库存...")
        product_manager.update_stock(product_ids[1], 20)
        print("  库存已更新")
    
    print("\n高级示例程序执行完成！")


if __name__ == "__main__":
    main()