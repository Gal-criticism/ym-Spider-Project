import argparse
import os
import asyncio
from typing import Optional, List

from ..api.api_client import YMGalAPIClient
from ..data.data_processor import DataProcessor
from ..matching.matching_engine import MatchingEngine
from ..async_spider import AsyncMatchingEngine


class MainController:
    """主控制器，协调各个组件的工作"""
    
    def __init__(self):
        self.api_client = YMGalAPIClient()
        self.data_processor = DataProcessor()
        self.matching_engine = MatchingEngine(self.api_client, self.data_processor)
    
    def list_data_files(self) -> List[str]:
        """列出data文件夹中的Excel文件"""
        data_dir = "data"
        if not os.path.exists(data_dir):
            return []
        
        excel_files = []
        for file in os.listdir(data_dir):
            if file.endswith('.xlsx'):
                excel_files.append(file)
        
        return excel_files
    
    def select_input_file(self) -> str:
        """让用户选择输入文件"""
        data_files = self.list_data_files()
        
        if not data_files:
            print("data文件夹中没有找到Excel文件")
            return ""
        
        print("\n请选择要处理的文件:")
        for i, file in enumerate(data_files, 1):
            print(f"{i}. {file}")
        
        while True:
            try:
                choice = input("请输入文件编号: ").strip()
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(data_files):
                    selected_file = data_files[choice_num - 1]
                    print(f"已选择: {selected_file}")
                    return f"data/{selected_file}"
                else:
                    print("无效的选择，请重新输入")
            except ValueError:
                print("请输入有效的数字")
            except KeyboardInterrupt:
                print("\n用户取消")
                return ""
    
    def select_matching_mode(self) -> str:
        """让用户选择匹配模式"""
        print("\n请选择匹配模式:")
        print("1. 同步原始匹配 (使用日文名和中文名)")
        print("2. 同步别名匹配 (使用别名列)")
        print("3. 异步原始匹配 (高并发，推荐)")
        print("4. 异步别名匹配 (高并发，推荐)")
        print("5. 性能对比测试 (同步 vs 异步)")
        
        while True:
            try:
                choice = input("请输入选择 (1-5): ").strip()
                
                if choice == "1":
                    return "basic"
                elif choice == "2":
                    return "alias"
                elif choice == "3":
                    return "async_basic"
                elif choice == "4":
                    return "async_alias"
                elif choice == "5":
                    return "performance_test"
                else:
                    print("无效选择，请输入1-5")
            except KeyboardInterrupt:
                print("\n用户取消")
                return ""
    
    def select_batch_size(self) -> int:
        print("\n请输入批次大小（建议10~200，默认50）：")
        while True:
            try:
                value = input("批次大小: ").strip()
                if not value:
                    return 50  # 默认值
                batch_size = int(value)
                if 1 <= batch_size <= 500:
                    return batch_size
                else:
                    print("请输入1~500之间的数字")
            except ValueError:
                print("请输入有效数字")
            except KeyboardInterrupt:
                print("\n用户取消")
                return 50
    
    def run_basic_matching(self, input_file: str, output_file: str, 
                          unmatched_file: str, org_output_file: str) -> None:
        """运行基础匹配流程（第一个源代码的功能）"""
        print("开始基础匹配流程...")
        self.matching_engine.match_bgm_games_and_save(
            input_file=input_file,
            output_file=output_file,
            unmatched_file=unmatched_file,
            org_output_file=org_output_file
        )
    
    def run_alias_matching(self, input_file: str, output_file: str,
                          unmatched_file: str, org_output_file: str) -> None:
        """运行别名匹配流程（第二个源代码的功能）"""
        print("开始别名匹配流程...")
        self.matching_engine.match_bgm_games_with_aliases_and_save(
            input_file=input_file,
            output_file=output_file,
            unmatched_file=unmatched_file,
            org_output_file=org_output_file
        )
    
    async def run_async_basic_matching(self, input_file: str, output_file: str,
                                     unmatched_file: str, org_output_file: str, batch_size: int = 50) -> None:
        """运行异步基础匹配流程"""
        print("开始异步基础匹配流程...")
        
        # 创建异步匹配引擎（高性能配置）
        engine = AsyncMatchingEngine(
            max_concurrent=8,       # 提高并发数
            buffer_size=500,        # 增大缓冲区
            write_interval=1.0,     # 减少写入间隔
            batch_size=batch_size   # 批次大小
        )
        
        try:
            # 启动引擎
            await engine.start()
            
            # 执行异步匹配
            await engine.match_bgm_games_async(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file,
                org_output_file=org_output_file
            )
            
        except Exception as e:
            print(f"异步匹配过程中发生错误: {e}")
        finally:
            # 停止引擎
            await engine.stop()
    
    async def run_async_alias_matching(self, input_file: str, output_file: str,
                                     unmatched_file: str, org_output_file: str, batch_size: int = 50) -> None:
        """运行异步别名匹配流程"""
        print("开始异步别名匹配流程...")
        
        # 创建异步匹配引擎（高性能配置）
        engine = AsyncMatchingEngine(
            max_concurrent=8,       # 提高并发数
            buffer_size=600,        # 增大缓冲区
            write_interval=1.0,     # 减少写入间隔
            batch_size=batch_size   # 批次大小
        )
        
        try:
            # 启动引擎
            await engine.start()
            
            # 执行异步别名匹配
            await engine.match_bgm_games_with_aliases_async(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file,
                org_output_file=org_output_file
            )
            
        except Exception as e:
            print(f"异步别名匹配过程中发生错误: {e}")
        finally:
            # 停止引擎
            await engine.stop()
    
    def run_secondary_matching(self, ym_file: str, bangumi_file: str, 
                              output_file: str) -> None:
        """运行二次匹配流程"""
        print("开始二次匹配流程...")
        self.matching_engine.match_ym_with_bangumi(
            ym_file=ym_file,
            bangumi_file=bangumi_file,
            output_file=output_file
        )
    
    def run_interactive(self) -> None:
        """交互式运行主程序"""
        print("=== Bangumi-月幕游戏匹配工具 ===")
        
        # 选择匹配模式
        mode = self.select_matching_mode()
        if not mode:
            return
        
        # 选择输入文件
        input_file = self.select_input_file()
        if not input_file:
            return
        
        # 设置输出文件
        if mode.startswith("async_"):
            # 异步模式使用不同的输出文件名
            suffix = "_async"
        else:
            # 同步模式使用原有文件名
            suffix = ""
        output_file = f"save/ymgames_matched{suffix}.xlsx"
        unmatched_file = f"save/ymgames_unmatched{suffix}.xlsx"
        org_output_file = f"save/organizations_info{suffix}.xlsx"
        
        # 确保输出目录存在
        os.makedirs("save", exist_ok=True)
        
        # 运行选择的模式
        if mode == "basic":
            self.run_basic_matching(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file,
                org_output_file=org_output_file
            )
        elif mode == "alias":
            self.run_alias_matching(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file,
                org_output_file=org_output_file
            )
        elif mode == "async_basic":
            batch_size = self.select_batch_size()
            asyncio.run(self.run_async_basic_matching(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file,
                org_output_file=org_output_file,
                batch_size=batch_size
            ))
        elif mode == "async_alias":
            batch_size = self.select_batch_size()
            asyncio.run(self.run_async_alias_matching(
                input_file=input_file,
                output_file=output_file,
                unmatched_file=unmatched_file,
                org_output_file=org_output_file,
                batch_size=batch_size
            ))
        elif mode == "performance_test":
            self.run_performance_test(input_file)
    
    def run(self, mode: str = "basic", **kwargs) -> None:
        """运行主程序"""
        if mode == "basic":
            self.run_basic_matching(
                input_file=kwargs.get("input_file", "bgm_archive_20250525 (1).xlsx"),
                output_file=kwargs.get("output_file", "ymgames_matched.xlsx"),
                unmatched_file=kwargs.get("unmatched_file", "ymgames_unmatched.xlsx"),
                org_output_file=kwargs.get("org_output_file", "organizations_info.xlsx")
            )
        elif mode == "alias":
            self.run_alias_matching(
                input_file=kwargs.get("input_file", "主表_updated_processed_aliases_20250621_124012.xlsx"),
                output_file=kwargs.get("output_file", "ymgames_matched.xlsx"),
                unmatched_file=kwargs.get("unmatched_file", "ymgames_unmatched.xlsx"),
                org_output_file=kwargs.get("org_output_file", "organizations_info.xlsx")
            )
        elif mode == "async_basic":
            asyncio.run(self.run_async_basic_matching(
                input_file=kwargs.get("input_file", "bgm_archive_20250525 (1).xlsx"),
                output_file=kwargs.get("output_file", "ymgames_matched_async.xlsx"),
                unmatched_file=kwargs.get("unmatched_file", "ymgames_unmatched_async.xlsx"),
                org_output_file=kwargs.get("org_output_file", "organizations_info_async.xlsx")
            ))
        elif mode == "async_alias":
            asyncio.run(self.run_async_alias_matching(
                input_file=kwargs.get("input_file", "主表_updated_processed_aliases_20250621_124012.xlsx"),
                output_file=kwargs.get("output_file", "ymgames_matched_aliases_async.xlsx"),
                unmatched_file=kwargs.get("unmatched_file", "ymgames_unmatched_aliases_async.xlsx"),
                org_output_file=kwargs.get("org_output_file", "organizations_info_aliases_async.xlsx")
            ))
        elif mode == "secondary":
            self.run_secondary_matching(
                ym_file=kwargs.get("ym_file", "ymgames_matched.xlsx"),
                bangumi_file=kwargs.get("bangumi_file", "processed_games_test5.xlsx"),
                output_file=kwargs.get("output_file", "ym_bangumi_matched.csv")
            )
        else:
            print(f"未知的模式: {mode}")
    
    def run_performance_test(self, input_file: str) -> None:
        """运行性能对比测试"""
        import time
        
        print("\n=== 性能对比测试 ===")
        print(f"测试文件: {input_file}")
        
        # 确保输出目录存在
        os.makedirs("save", exist_ok=True)
        
        # 测试同步版本
        print("\n1. 测试同步版本...")
        sync_start = time.time()
        
        try:
            self.run_basic_matching(
                input_file=input_file,
                output_file="save/sync_test.xlsx",
                unmatched_file="save/sync_unmatched.xlsx",
                org_output_file="save/sync_org.xlsx"
            )
            sync_time = time.time() - sync_start
            print(f"同步版本完成，耗时: {sync_time:.2f}秒")
        except Exception as e:
            print(f"同步版本测试失败: {e}")
            sync_time = float('inf')
        
        # 测试异步版本
        print("\n2. 测试异步版本...")
        async_start = time.time()
        
        try:
            asyncio.run(self.run_async_basic_matching(
                input_file=input_file,
                output_file="save/async_test.xlsx",
                unmatched_file="save/async_unmatched.xlsx",
                org_output_file="save/async_org.xlsx"
            ))
            async_time = time.time() - async_start
            print(f"异步版本完成，耗时: {async_time:.2f}秒")
        except Exception as e:
            print(f"异步版本测试失败: {e}")
            async_time = float('inf')
        
        # 显示对比结果
        print("\n=== 性能对比结果 ===")
        if sync_time != float('inf') and async_time != float('inf'):
            speedup = sync_time / async_time
            print(f"同步版本耗时: {sync_time:.2f}秒")
            print(f"异步版本耗时: {async_time:.2f}秒")
            print(f"速度提升: {speedup:.2f}倍")
            
            if speedup > 1.5:
                print("✅ 异步版本显著更快！")
            elif speedup > 1.1:
                print("✅ 异步版本略快")
            elif speedup < 0.9:
                print("❌ 异步版本较慢，可能需要调整配置")
            else:
                print("⚖️ 两种方式速度相近")
        else:
            print("❌ 测试失败，无法比较性能")
        
        print("\n性能优化建议:")
        print("- 如果异步版本较慢，可能是网络延迟或API限流导致")
        print("- 可以尝试调整并发数: 网络好时用8-12，网络差时用3-5")
        print("- 503错误多时，建议降低并发数并增加请求间隔")


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="Bangumi-月幕游戏匹配工具")
    parser.add_argument("--mode", choices=["basic", "alias", "async_basic", "async_alias", "secondary", "interactive"], 
                       default="interactive", help="运行模式")
    
    # 基础匹配参数
    parser.add_argument("--input", help="输入文件路径")
    parser.add_argument("--output", help="输出文件路径")
    parser.add_argument("--unmatched", help="未匹配文件路径")
    parser.add_argument("--org-output", help="会社信息输出文件路径")
    
    # 二次匹配参数
    parser.add_argument("--ym-file", help="月幕文件路径")
    parser.add_argument("--bangumi-file", help="Bangumi文件路径")
    
    args = parser.parse_args()
    
    controller = MainController()
    
    if args.mode == "interactive":
        controller.run_interactive()
    elif args.mode == "basic":
        controller.run_basic_matching(
            input_file=args.input or "bgm_archive_20250525 (1).xlsx",
            output_file=args.output or "ymgames_matched.xlsx",
            unmatched_file=args.unmatched or "ymgames_unmatched.xlsx",
            org_output_file=args.org_output or "organizations_info.xlsx"
        )
    elif args.mode == "alias":
        controller.run_alias_matching(
            input_file=args.input or "主表_updated_processed_aliases_20250621_124012.xlsx",
            output_file=args.output or "ymgames_matched.xlsx",
            unmatched_file=args.unmatched or "ymgames_unmatched.xlsx",
            org_output_file=args.org_output or "organizations_info.xlsx"
        )
    elif args.mode == "async_basic":
        asyncio.run(controller.run_async_basic_matching(
            input_file=args.input or "bgm_archive_20250525 (1).xlsx",
            output_file=args.output or "ymgames_matched_async.xlsx",
            unmatched_file=args.unmatched or "ymgames_unmatched_async.xlsx",
            org_output_file=args.org_output or "organizations_info_async.xlsx"
        ))
    elif args.mode == "async_alias":
        asyncio.run(controller.run_async_alias_matching(
            input_file=args.input or "主表_updated_processed_aliases_20250621_124012.xlsx",
            output_file=args.output or "ymgames_matched_aliases_async.xlsx",
            unmatched_file=args.unmatched or "ymgames_unmatched_aliases_async.xlsx",
            org_output_file=args.org_output or "organizations_info_aliases_async.xlsx"
        ))
    elif args.mode == "secondary":
        controller.run_secondary_matching(
            ym_file=args.ym_file or "ymgames_matched.xlsx",
            bangumi_file=args.bangumi_file or "processed_games_test5.xlsx",
            output_file=args.output or "ym_bangumi_matched.csv"
        )


if __name__ == "__main__":
    main()