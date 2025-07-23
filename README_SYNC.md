# 飞书多维表格与数据库双向同步服务

这是一个完整的飞书多维表格与数据库双向同步解决方案，支持准实时的数据同步。

## 功能特性

- ✅ **双向同步**：支持飞书表格与数据库之间的双向数据同步
- ✅ **准实时同步**：飞书到数据库 5-10 秒延迟，数据库到飞书 <1 秒延迟
- ✅ **防循环同步**：基于哈希值和时间窗口的智能去重机制
- ✅ **字段映射**：支持飞书字段与数据库字段的灵活映射
- ✅ **错误重试**：自动重试失败的同步任务
- ✅ **监控告警**：完善的监控指标和告警机制
- ✅ **易于部署**：支持 Docker 部署和 systemd 服务

## 系统架构

```
飞书多维表格 <---> 同步服务 <---> 数据库
     |               |              |
  轮询检测      防循环机制      触发器
     |               |              |
  变更检测      同步队列       实时捕获
```

## 快速开始

### 1. 安装

```bash
# 克隆代码
git clone https://github.com/lory7c/db-fs.git
cd db-fs

# 运行安装脚本
chmod +x scripts/install.sh
./scripts/install.sh
```

### 2. 配置

编辑 `config.json` 文件：

```json
{
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "your_user",
        "password": "your_password",
        "database": "your_database"
    },
    "feishu": {
        "app_id": "cli_xxxxx",
        "app_secret": "xxxxx"
    },
    "sync": {
        "poll_interval": 5,
        "table_mapping": {
            "MyDB:users": "users",
            "MyDB:orders": "orders"
        },
        "field_mapping": {
            "users": {
                "姓名": "name",
                "年龄": "age",
                "邮箱": "email"
            }
        }
    }
}
```

### 3. 初始化数据库

```bash
# 创建同步所需的表和触发器
mysql -u root -p your_database < database_triggers.sql
```

### 4. 测试连接

```bash
# 激活虚拟环境
source venv/bin/activate

# 测试连接
python main.py --test
```

### 5. 启动服务

```bash
# 前台运行
python main.py

# 或使用 systemd（推荐）
sudo systemctl start feishu-db-sync
sudo systemctl enable feishu-db-sync
```

## Docker 部署

```bash
# 使用 docker-compose
docker-compose up -d

# 查看日志
docker-compose logs -f sync-service
```

## 使用说明

### 表映射配置

在 `config.json` 中配置表映射关系：

```json
"table_mapping": {
    "飞书数据库:飞书表名": "数据库表名"
}
```

### 字段映射配置

配置飞书字段与数据库字段的对应关系：

```json
"field_mapping": {
    "数据库表名": {
        "飞书字段名": "数据库字段名"
    }
}
```

### 监控指标

服务运行时会输出以下监控指标：

- 同步成功/失败次数
- 队列积压情况
- 平均同步延迟
- 错误日志

### 常用命令

```bash
# 查看服务状态
python main.py --status

# 重置表快照（重新全量同步）
python main.py --reset-snapshot MyDB:users

# 查看帮助
python main.py --help
```

## 项目结构

```
feishu_db_sync/
├── config/          # 配置管理
├── core/            # 同步核心逻辑
├── db/              # 数据库操作
├── feishu/          # 飞书客户端
├── monitor/         # 监控和日志
└── tests/           # 测试代码

main.py              # 主程序入口
database_triggers.sql # 数据库触发器脚本
docker-compose.yml   # Docker 编排文件
```

## 注意事项

1. **API 限制**：飞书 API 有调用频率限制，建议轮询间隔不小于 5 秒
2. **数据一致性**：本方案保证最终一致性，不保证强一致性
3. **字段类型**：注意飞书特殊字段（如人员、附件）的处理
4. **性能优化**：大数据量场景建议使用批量同步

## 故障排查

### 同步失败

1. 检查日志文件 `logs/sync.log`
2. 查看同步队列：`SELECT * FROM sync_queue WHERE status = 'failed'`
3. 查看错误日志：`SELECT * FROM sync_log WHERE status = 'failed'`

### 数据不一致

1. 重置表快照：`python main.py --reset-snapshot TABLE_NAME`
2. 手动触发全量同步

### 循环同步

1. 检查同步日志：`SELECT * FROM sync_log ORDER BY created_at DESC`
2. 确认防循环机制是否生效

## 贡献指南

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License