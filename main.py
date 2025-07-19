#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
匹配工具
主程序入口
"""

import sys
import os

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.core.main_controller import MainController


def main():
    """主程序入口"""
    controller = MainController()
    
    # 默认使用交互模式，让用户选择匹配模式和源文件
    controller.run_interactive()


if __name__ == "__main__":
    main()
