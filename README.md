# Bangumi-月幕游戏匹配系统



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

### v3.0
- 新增异步批量爬取功能，支持高并发与自适应拥塞控制算法，提升了处理效率。
- 尝试集成自动冷却与重试机制以应对服务器限流（503）问题。
- 由于目标服务器限流机制复杂，503 问题未能完全解决，部分请求仍可能频繁遇到 503。
- 建议后续结合代理池、账号池等手段进一步优化。

## 异步爬虫与缓冲池结构说明

### 异步爬虫（AsyncSpiderEngine）
- 基于 Python 的 asyncio 和 aiohttp 实现高并发异步爬取。
- 支持自适应拥塞控制，动态调整并发窗口和请求间隔，提升爬取效率并降低被限流风险。
- 通过批量调度和任务重试机制，提升大规模数据处理能力。
- 核心类：`AsyncSpiderEngine`，负责主流程调度、API请求、断点续传等。

### 缓冲池（BufferManager）
- 设计用于异步环境下高效批量写入文件，减少频繁IO操作带来的性能瓶颈。
- 支持数据缓冲、定时/批量写盘、自动关闭等功能。
- 与异步爬虫协作时，爬虫任务将结果异步推送到缓冲池，缓冲池自动管理写入节奏。
- 核心类：`BufferManager`，可配置缓冲区大小、写入策略等。

#### 协作示例
1. 爬虫任务通过 `await buffer_manager.put_data(data)` 异步推送结果。
2. BufferManager 在缓冲区满或定时触发时批量写入文件，极大降低磁盘IO压力。
3. 结束时调用 `await buffer_manager.stop()`，确保所有数据写盘。

**优势：**
- 异步爬虫+缓冲池结构可显著提升大规模数据抓取与写入的整体性能。
- 降低单次IO阻塞风险，适合高并发、海量数据场景。

## 许可证

本项目仅供学习和研究使用。

## 贡献

欢迎提交Issue和Pull Request来改进项目。 