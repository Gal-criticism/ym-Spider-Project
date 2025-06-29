# Bangumi-月幕游戏匹配系统

一个用于匹配Bangumi游戏与月幕游戏的Python工具，采用面向对象设计，支持多种匹配模式和交互式操作。

## 功能特性

- 🔍 **智能匹配**：支持多种匹配模式（精确匹配、模糊匹配、会社匹配）
- 📊 **数据管理**：自动处理Excel文件，支持多种数据格式
- 🏢 **会社信息**：自动获取和匹配游戏会社信息
- 📝 **日志记录**：完整的API响应日志记录，支持静默模式
- 🎯 **交互式操作**：支持命令行交互选择匹配模式和源文件
- 💾 **结果保存**：自动保存匹配结果到Excel文件

## 项目结构

```
Spider Project/
├── src/                    # 源代码目录
│   ├── api/               # API客户端
│   │   └── api_client.py  # 月幕API客户端
│   ├── core/              # 核心控制器
│   │   └── main_controller.py
│   ├── data/              # 数据处理
│   │   └── data_processor.py
│   ├── matching/          # 匹配引擎
│   │   └── matching_engine.py
│   ├── organization/      # 会社管理
│   │   └── organization_manager.py
│   └── utils/             # 工具类
│       └── logger.py      # 日志管理
├── data/                  # 输入数据目录
├── save/                  # 输出结果目录
├── logs/                  # 日志文件目录
├── main.py               # 主程序入口
├── requirements.txt      # 依赖包列表
└── README.md            # 项目说明
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 准备数据

将需要匹配的Excel文件放入`data/`目录。

### 3. 运行程序

```bash
python main.py
```

程序会提示你选择：
- 匹配模式（精确匹配/模糊匹配/会社匹配）
- 源文件（从data目录中选择）

### 4. 查看结果

匹配结果会保存在`save/`目录中，文件名包含时间戳。

## 日志功能

### 静默模式（默认）

程序默认使用静默模式，控制台输出简洁：
- ✅ **显示**：WARNING、ERROR、IMPORTANT信息
- ❌ **隐藏**：INFO信息（保存到日志文件）

### 日志文件

- **位置**：`logs/`目录
- **格式**：`api_responses_YYYYMMDD_HHMMSS.log`
- **内容**：完整的API请求和响应信息

### 自定义日志模式

```python
from src.utils.logger import Logger

# 静默模式（默认）
logger = Logger(silent_mode=True)

# 详细模式（显示所有信息）
logger = Logger(silent_mode=False)
```

## 匹配模式说明

### 1. 精确匹配
- 使用游戏名称进行精确搜索
- 适合已知完整游戏名称的情况
- 匹配度阈值：0.8

### 2. 模糊匹配
- 使用游戏名称进行模糊搜索
- 适合名称不完整或有变体的情况
- 返回前3个最佳匹配

### 3. 会社匹配
- 基于游戏会社信息进行匹配
- 适合需要按会社分类的情况
- 自动获取会社详细信息

## 配置说明

### 默认路径配置

```python
# 在main_controller.py中
DEFAULT_SOURCE_FILE = "data/主表_updated_processed_aliases_20250621_124012.xlsx"
DEFAULT_OUTPUT_DIR = "save"
```

### API配置

```python
# 在api_client.py中
BASE_URL = "https://www.ymgal.games"
CLIENT_ID = "ymgal"
CLIENT_SECRET = "luna0327"
```

## 使用示例

### 基本使用

```python
from src.core.main_controller import MainController

# 创建控制器
controller = MainController()

# 运行匹配
controller.run_matching(
    source_file="data/your_file.xlsx",
    matching_mode="exact",
    output_dir="save"
)
```

### 自定义匹配

```python
from src.matching.matching_engine import MatchingEngine
from src.api.api_client import YMGalAPIClient

# 初始化组件
api_client = YMGalAPIClient()
matching_engine = MatchingEngine(api_client)

# 执行匹配
results = matching_engine.match_game("游戏名称", mode="fuzzy")
```

## 开发说明

### 添加新的匹配模式

1. 在`MatchingEngine`中添加新的匹配方法
2. 在`MainController`中添加模式选项
3. 更新主程序的交互选项

### 扩展API功能

1. 在`YMGalAPIClient`中添加新的API方法
2. 更新日志记录
3. 添加错误处理

## 故障排除

### 常见问题

1. **Token获取失败**
   - 检查网络连接
   - 验证API配置

2. **文件读取错误**
   - 确认文件路径正确
   - 检查文件格式是否为Excel

3. **匹配结果为空**
   - 尝试不同的匹配模式
   - 检查游戏名称格式

### 日志分析

查看`logs/`目录中的日志文件，可以：
- 分析API请求和响应
- 调试匹配问题
- 监控程序运行状态

## 更新日志

### v2.0.0
- ✅ 重构为面向对象设计
- ✅ 添加多种匹配模式
- ✅ 实现交互式操作
- ✅ 完善日志系统
- ✅ 添加静默模式

### v1.0.0
- ✅ 基础匹配功能
- ✅ API集成
- ✅ 数据导出

## 许可证

本项目仅供学习和研究使用。

## 贡献

欢迎提交Issue和Pull Request来改进项目。 