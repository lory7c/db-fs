"""测试辅助工具"""

import os


# 从环境变量读取配置
APP_ID = os.getenv("app_id", "")
APP_SECRET = os.getenv("app_secret", "")