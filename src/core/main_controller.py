import argparse
import os
from typing import Optional, List

from ..api.api_client import YMGalAPIClient
from ..data.data_processor import DataProcessor
from ..matching.matching_engine import MatchingEngine


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
        print("1. 原始匹配 (使用日文名和中文名)")
        print("2. 别名匹配 (使用别名列)")
        
        while True:
            try:
                choice = input("请输入选择 (1-2): ").strip()
                
                if choice == "1":
                    return "basic"
                elif choice == "2":
                    return "alias"
                else:
                    print("无效选择，请输入1或2")
            except KeyboardInterrupt:
                print("\n用户取消")
                return ""
    
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
        output_file = "save/ymgames_matched.xlsx"
        unmatched_file = "save/ymgames_unmatched.xlsx"
        org_output_file = "save/organizations_info.xlsx"
        
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
        elif mode == "secondary":
            self.run_secondary_matching(
                ym_file=kwargs.get("ym_file", "ymgames_matched.xlsx"),
                bangumi_file=kwargs.get("bangumi_file", "processed_games_test5.xlsx"),
                output_file=kwargs.get("output_file", "ym_bangumi_matched.csv")
            )
        else:
            print(f"未知的模式: {mode}")


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="Bangumi-月幕游戏匹配工具")
    parser.add_argument("--mode", choices=["basic", "alias", "secondary", "interactive"], 
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
    elif args.mode == "secondary":
        controller.run_secondary_matching(
            ym_file=args.ym_file or "ymgames_matched.xlsx",
            bangumi_file=args.bangumi_file or "processed_games_test5.xlsx",
            output_file=args.output or "ym_bangumi_matched.csv"
        )


if __name__ == "__main__":
    main()