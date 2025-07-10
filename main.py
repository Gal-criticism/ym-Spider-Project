#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bangumi-月幕游戏匹配工具
主程序入口
"""

import sys
import os
import asyncio

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.core.main_controller import MainController
from src.async_spider import AsyncSpiderEngine, SpiderConfig, BufferConfig


async def run_async_spider(input_file: str, output_file: str, unmatched_file: str, use_aliases: bool = False):
    """
    运行异步爬虫
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
        unmatched_file: 未匹配文件路径
        use_aliases: 是否使用别名匹配
    """
    from src.api.api_client import YMGalAPIClient
    
    # 创建API客户端
    api_client = YMGalAPIClient()
    
    # 配置爬虫参数
    spider_config = SpiderConfig(
        max_retries=3,
        timeout=30,
        delay_between_requests=0.05,  # 50ms间隔
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    
    # 配置缓冲区参数
    buffer_config = BufferConfig(
        max_size=1000,
        flush_interval=3.0,  # 3秒刷新一次
        batch_size=30,  # 30条记录一批
        backup_interval=100
    )
    
    # 创建异步爬虫引擎
    spider_engine = AsyncSpiderEngine(
        api_client=api_client,
        spider_config=spider_config,
        buffer_config=buffer_config
    )
    
    print("=== 异步爬虫模式 ===")
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"未匹配文件: {unmatched_file}")
    print(f"使用别名: {'是' if use_aliases else '否'}")
    print("开始异步爬取...")
    
    try:
        if use_aliases:
            await spider_engine.crawl_with_aliases_async(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file
            )
        else:
            await spider_engine.crawl_games_async(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file
            )
        
        # 显示最终统计信息
        stats = spider_engine.get_stats()
        print(f"\n=== 爬取完成 ===")
        print(f"已处理项目数: {stats['processed_count']}")
        print(f"最终并发窗口: {stats['congestion_stats']['current_cwnd']}")
        print(f"成功率: {stats['congestion_stats']['success_rate']:.2f}")
        
    except Exception as e:
        print(f"异步爬取过程中出错: {e}")
        raise


def main():
    """主程序入口"""
    controller = MainController()
    
    print("=== Bangumi-月幕游戏匹配工具 ===")
    print("请选择运行模式:")
    print("1. 同步模式 (原始功能)")
    print("2. 异步模式 (高性能爬取)")
    print("3. 分阶段自适应模式 (推荐)")
    
    while True:
        try:
            mode_choice = input("请输入选择 (1-3): ").strip()
            
            if mode_choice == "1":
                # 同步模式
                controller.run_interactive()
                break
            elif mode_choice == "2":
                # 异步模式
                print("\n=== 异步爬取模式 ===")
                
                # 选择匹配模式
                print("请选择匹配模式:")
                print("1. 原始匹配 (使用日文名和中文名)")
                print("2. 别名匹配 (使用别名列)")
                
                alias_choice = input("请输入选择 (1-2): ").strip()
                use_aliases = alias_choice == "2"
                
                # 选择输入文件
                input_file = controller.select_input_file()
                if not input_file:
                    return
                
                # 设置输出文件
                output_file = "save/ymgames_matched_async.xlsx"
                unmatched_file = "save/ymgames_unmatched_async.xlsx"
                
                # 确保输出目录存在
                os.makedirs("save", exist_ok=True)
                
                # 运行异步爬虫
                asyncio.run(run_async_spider(
                    input_file=input_file,
                    output_file=output_file,
                    unmatched_file=unmatched_file,
                    use_aliases=use_aliases
                ))
                break
            elif mode_choice == "3":
                # 分阶段自适应模式
                print("\n=== 分阶段自适应爬取模式 ===")
                
                # 选择匹配模式
                print("请选择匹配模式:")
                print("1. 原始匹配 (使用日文名和中文名)")
                print("2. 别名匹配 (使用别名列)")
                
                alias_choice = input("请输入选择 (1-2): ").strip()
                use_aliases = alias_choice == "2"
                
                # 选择输入文件
                input_file = controller.select_input_file()
                if not input_file:
                    return
                
                # 运行分阶段爬虫
                from src.core.staged_spider_controller import StagedSpiderController
                staged_controller = StagedSpiderController()
                asyncio.run(staged_controller.run_staged_spider(
                    input_file=input_file,
                    use_aliases=use_aliases
                ))
                break
            else:
                print("无效选择，请输入1、2或3")
        except KeyboardInterrupt:
            print("\n用户取消")
            return
        except Exception as e:
            print(f"运行出错: {e}")
            return


if __name__ == "__main__":
    main()