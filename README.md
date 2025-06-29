# Bangumi-月幕游戏匹配工具

这是一个用于匹配Bangumi游戏与月幕游戏的Python工具，支持原始匹配和别名匹配两种模式。

## 功能特性

- **原始匹配模式**: 使用日文名和中文名进行匹配（第一个源代码的功能）
- **别名匹配模式**: 使用别名列进行匹配（第二个源代码的功能）
- **交互式选择**: 可以选择匹配模式和源文件
- **断点续跑**: 支持中断后继续处理
- **会社信息查询**: 自动获取和缓存会社详细信息
- **二次匹配**: 支持月幕到Bangumi的额外信息匹配

## 项目结构

```
Spider Project/
├── main.py                 # 主程序入口（交互模式）
├── example_usage.py        # 使用示例
├── test_simple.py          # 简单测试脚本
├── run_simple.py           # 交互功能测试脚本
├── requirements.txt        # 依赖包列表
├── data/                   # 输入数据文件夹
│   ├── bgm_archive_20250525 (1).xlsx
│   └── 主表_updated_processed_aliases_20250621_124012.xlsx
├── save/                   # 输出结果文件夹
└── src/                    # 源代码
    ├── api/                # API相关
    │   └── api_client.py   # 月幕API客户端
    ├── data/               # 数据处理
    │   └── data_processor.py
    ├── matching/           # 匹配引擎
    │   └── matching_engine.py
    ├── core/               # 核心控制
    │   └── main_controller.py
    └── utils/              # 工具模块
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 准备数据

将您的Excel文件放入`data`文件夹中。

### 3. 运行程序

#### 交互模式（推荐）

```bash
python main.py
```

程序会引导您：
1. 选择匹配模式（原始匹配或别名匹配）
2. 选择要处理的源文件

#### 命令行模式

```bash
# 基础匹配模式
python -m src.core.main_controller --mode basic --input data/your_file.xlsx

# 别名匹配模式
python -m src.core.main_controller --mode alias --input data/your_file.xlsx

# 二次匹配模式
python -m src.core.main_controller --mode secondary --ym-file save/ymgames_matched.xlsx --bangumi-file data/processed_games_test5.xlsx
```

## 匹配模式说明

### 原始匹配模式
- 使用Excel文件中的"日文名"和"中文名"列进行匹配
- 适用于标准的Bangumi数据格式
- 对应第一个源代码的功能

### 别名匹配模式
- 使用Excel文件中以"别名"开头的列进行匹配
- 支持多个别名，选择最佳匹配结果
- 会与原始分数比较，选择更优的结果
- 对应第二个源代码的功能

## 输出文件

程序会在`save`文件夹中生成以下文件：

- `ymgames_matched.xlsx`: 匹配结果
- `ymgames_unmatched.xlsx`: 未匹配的游戏
- `organizations_info.xlsx`: 会社信息
- `ym_bangumi_matched.csv`: 二次匹配结果（如果运行二次匹配）

## 使用示例

### 查看使用示例

```bash
python example_usage.py
```

### 测试代码结构

```bash
python test_simple.py
```

### 测试交互功能

```bash
python run_simple.py
```

## 配置说明

### 输入文件格式

#### 原始匹配模式要求
- 必须包含"日文名"和"中文名"列
- 可选包含"id"列作为唯一标识

#### 别名匹配模式要求
- 必须包含以"别名"开头的列（如"别名1"、"别名2"等）
- 可选包含"score"列作为原始分数
- 可选包含"bgm_id"列作为唯一标识

### API配置

程序使用月幕游戏的公开API，无需额外配置。

## 注意事项

1. **网络连接**: 程序需要网络连接来访问月幕API
2. **文件格式**: 输入文件必须是Excel格式（.xlsx）
3. **文件占用**: 确保输出文件没有被其他程序占用
4. **断点续跑**: 程序会自动跳过已处理的记录
5. **API限流**: 程序内置了请求间隔，避免触发API限流

## 故障排除

### 常见问题

1. **导入错误**: 确保已安装所有依赖包
2. **文件不存在**: 检查data文件夹中是否有Excel文件
3. **API错误**: 检查网络连接和API状态
4. **权限错误**: 确保有写入save文件夹的权限

### 调试模式

程序会输出详细的调试信息，包括：
- API响应内容
- 匹配过程详情
- 错误信息

## 开发说明

### 添加新的匹配模式

1. 在`MatchingEngine`中添加新的匹配方法
2. 在`MainController`中添加对应的运行方法
3. 更新交互选择逻辑

### 扩展API功能

1. 在`YMGalAPIClient`中添加新的API方法
2. 更新相关的数据处理逻辑

## 许可证

本项目仅供学习和研究使用 