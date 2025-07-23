#!/bin/bash
# 安装脚本

set -e

echo "Installing Feishu Database Sync Service..."

# 检查Python版本
python_version=$(python3 --version 2>&1 | awk '{print $2}')
required_version="3.7"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3,7) else 1)"; then
    echo "Error: Python 3.7 or higher is required"
    exit 1
fi

echo "Python version: $python_version"

# 创建虚拟环境
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# 安装依赖
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements_sync.txt

# 创建必要的目录
echo "Creating directories..."
mkdir -p logs
mkdir -p config

# 初始化配置文件
if [ ! -f config.json ]; then
    echo "Creating default configuration..."
    python main.py --init
fi

# 创建systemd服务文件
if [ -d /etc/systemd/system ]; then
    echo "Creating systemd service..."
    sudo tee /etc/systemd/system/feishu-db-sync.service > /dev/null <<EOF
[Unit]
Description=Feishu Database Sync Service
After=network.target mysql.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin"
ExecStart=$(pwd)/venv/bin/python $(pwd)/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

    echo "Systemd service created. You can start it with:"
    echo "  sudo systemctl start feishu-db-sync"
    echo "  sudo systemctl enable feishu-db-sync"
fi

echo ""
echo "Installation completed!"
echo ""
echo "Next steps:"
echo "1. Edit config.json with your Feishu and database credentials"
echo "2. Run 'source venv/bin/activate' to activate the virtual environment"
echo "3. Run 'python main.py --test' to test connections"
echo "4. Run 'python main.py' to start the sync service"