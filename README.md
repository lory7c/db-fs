# feishu-bitable-db-py

把飞书多维表格作为底层存储，实现数据库的基本操作（CRUD）。

这是 [feishu-bitable-db](https://github.com/geeklubcn/feishu-bitable-db) 的 Python 版本。

## 功能特性

### DDL（数据定义语言）
- 新建数据库 `save_database(name: str) -> str`
- 新建数据表 `save_table(database: str, table: Table) -> str`
- 查看所有数据表 `list_tables(database: str) -> List[str]`
- 删除数据表 `drop_table(database: str, table: str) -> None`

### DML（数据操作语言）
- 新建记录 `create(database: str, table: str, record: Dict[str, Any]) -> str`
- 查询记录 `read(database: str, table: str, search_cmds: List[SearchCmd]) -> List[Dict[str, Any]]`
- 修改记录 `update(database: str, table: str, record_id: str, record: Dict[str, Any]) -> None`
- 删除记录 `delete(database: str, table: str, record_id: str) -> None`

## 安装

```bash
pip install feishu-bitable-db
```

## 使用方式

需要确认飞书 app 已经申请了相关权限，详情参考 https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/scope-list

- drive:drive
- bitable:app

### 基本使用示例

```python
from feishu_bitable_db import DB, Table, Field, FieldType, SearchCmd
from feishu_bitable_db.db.db import DBImpl

# 初始化数据库
db = DBImpl("${app_id}", "${app_secret}")

# 创建数据库
db.save_database("MyDB")

# 创建表
table = Table(
    name="MyTable",
    fields=[
        Field(name="username", type=FieldType.STRING),
        Field(name="passport", type=FieldType.STRING),
    ]
)
db.save_table("MyDB", table)

# 修改表结构
table = Table(
    name="MyTable",
    fields=[
        Field(name="username", type=FieldType.STRING),
        Field(name="age", type=FieldType.INT),
    ]
)
db.save_table("MyDB", table)

# 创建记录
record_id = db.create("MyDB", "MyTable", {
    "username": "zhangsan",
    "age": 12,
})

# 更新记录
db.update("MyDB", "MyTable", record_id, {
    "age": 13,
})

# 查询记录
results = db.read("MyDB", "MyTable", [
    SearchCmd(key="age", operator="=", val=13)
])

print(results)  # [{'age': 13, 'id': 'rec3gAKlML', 'username': 'zhangsan'}]

# 删除记录
db.delete("MyDB", "MyTable", record_id)
```

## 支持的字段类型

- `FieldType.STRING` - 文本
- `FieldType.INT` - 数字
- `FieldType.RADIO` - 单选
- `FieldType.MULTI_SELECT` - 多选
- `FieldType.DATE` - 日期
- `FieldType.PEOPLE` - 人员

## 开发

### 安装开发依赖

```bash
pip install -e ".[dev]"
```

### 运行测试

```bash
# 设置环境变量
export app_id="your_app_id"
export app_secret="your_app_secret"

# 运行测试
pytest
```

### 代码格式化

```bash
# 格式化代码
black .
isort .

# 检查代码质量
flake8
mypy .
```

## 许可证

MIT License
