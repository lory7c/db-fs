FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt requirements_sync.txt ./

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt -r requirements_sync.txt

# 复制源代码
COPY feishu_db_sync ./feishu_db_sync
COPY main.py ./

# 创建日志目录
RUN mkdir -p /app/logs

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 运行服务
CMD ["python", "main.py"]