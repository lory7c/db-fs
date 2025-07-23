# Contributing to feishu-bitable-db-py

感谢您对 feishu-bitable-db-py 项目的关注！我们欢迎各种形式的贡献。

## 如何贡献

### 报告问题

如果您发现了 bug 或有功能建议，请在 [GitHub Issues](https://github.com/geeklubcn/feishu-bitable-db-py/issues) 中创建一个 issue。

在创建 issue 时，请提供：
- 清晰的问题描述
- 复现步骤
- 期望的行为
- 实际的行为
- 您的环境信息（Python 版本、操作系统等）

### 提交代码

1. Fork 本仓库
2. 创建您的特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交您的更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建一个 Pull Request

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/geeklubcn/feishu-bitable-db-py.git
cd feishu-bitable-db-py

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装开发依赖
pip install -e ".[dev]"

# 或使用 make
make dev-install
```

### 代码规范

我们使用以下工具来保持代码质量：

- **Black**: 代码格式化
- **isort**: import 语句排序
- **flake8**: 代码风格检查
- **mypy**: 类型检查

在提交代码前，请运行：

```bash
# 格式化代码
make format

# 运行代码检查
make lint

# 运行测试
make test
```

### 测试

请确保您的代码包含适当的测试：

```python
def test_your_feature():
    """测试您的功能"""
    # 测试代码
    assert expected == actual
```

运行测试：

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_specific.py::test_function

# 运行测试并生成覆盖率报告
pytest --cov=feishu_bitable_db
```

### 文档

如果您的更改影响了公共 API，请更新相应的文档：

- 在代码中添加/更新 docstring
- 更新 README.md（如果需要）
- 在 examples/ 中添加示例（如果适用）

### 提交信息规范

请使用清晰的提交信息，建议遵循以下格式：

```
<type>: <subject>

<body>

<footer>
```

类型（type）：
- feat: 新功能
- fix: 修复 bug
- docs: 文档更新
- style: 代码格式调整（不影响功能）
- refactor: 代码重构
- test: 测试相关
- chore: 构建过程或辅助工具的变动

示例：
```
feat: 添加批量创建记录功能

- 实现 batch_create 方法
- 添加相关测试
- 更新文档

Closes #123
```

### Pull Request 指南

- PR 标题应该清晰地描述更改
- 在 PR 描述中说明更改的原因和内容
- 确保所有测试通过
- 确保代码符合项目规范
- 如果 PR 解决了某个 issue，请在描述中链接它

## 行为准则

参与本项目即表示您同意遵守我们的行为准则：

- 使用友善和包容的语言
- 尊重不同的观点和经验
- 优雅地接受建设性批评
- 专注于对社区最有利的事情
- 对其他社区成员表现出同理心

## 许可

通过贡献代码，您同意您的贡献将按照与项目相同的 [MIT 许可证](LICENSE) 进行许可。

## 问题和帮助

如果您有任何问题，可以：
- 查看项目文档
- 在 Issues 中搜索相关问题
- 创建新的 Issue 寻求帮助

感谢您的贡献！