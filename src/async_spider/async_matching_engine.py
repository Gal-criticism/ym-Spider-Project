import asyncio
import pandas as pd
import os
from typing import List, Dict, Any, Optional
from tqdm import tqdm

from .async_spider_engine import AsyncSpiderEngine
from .buffer_manager import BufferManager, BufferConfig, WriteStrategy
from ..data.data_processor import DataProcessor
from ..utils.logger import Logger


class AsyncMatchingEngine:
    """异步匹配引擎，整合异步爬虫和缓冲池功能"""
    
    def __init__(self, 
                 max_concurrent: int = 5,  # 降低默认并发数，避免503错误
                 buffer_size: int = 500,
                 write_interval: float = 3.0,
                 batch_size: int = 50):  # 添加批次大小参数
        """
        初始化异步匹配引擎
        
        Args:
            max_concurrent: 最大并发数
            buffer_size: 缓冲区大小
            write_interval: 写入间隔（秒）
            batch_size: 批次大小（每批处理的任务数量）
        """
        # 初始化异步爬虫引擎
        self.spider = AsyncSpiderEngine(
            max_concurrent=max_concurrent,
            request_delay=0.1,  # 减少请求间隔，提高速度
            max_retries=3,      # 减少重试次数，避免过多延迟
            timeout=20          # 减少超时时间
        )
        
        # 初始化缓冲池管理器
        buffer_config = BufferConfig(
            buffer_size=buffer_size,
            write_interval=write_interval,
            strategy=WriteStrategy.HYBRID,
            auto_flush=True,
            backup_on_error=True
        )
        self.buffer_manager = BufferManager(buffer_config)
        
        # 数据处理器
        self.data_processor = DataProcessor()
        
        # 批次大小
        self.batch_size = batch_size
        
        # 日志记录器
        self.logger = Logger(silent_mode=True)
        
        # 统计信息
        self.processed_count = 0
        self.matched_count = 0
        self.unmatched_count = 0
        

        
    async def start(self):
        """启动异步匹配引擎"""
        print("正在启动异步匹配引擎...")
        
        # 启动缓冲池管理器
        await self.buffer_manager.start()
        
        # 初始化API token
        if not await self.spider.initialize_token():
            raise Exception("无法获取API访问令牌")
        
        print("异步匹配引擎启动完成")
    
    async def stop(self):
        """停止异步匹配引擎"""
        print("正在停止异步匹配引擎...")
        
        # 停止缓冲池管理器
        await self.buffer_manager.stop()
        
        # 显示统计信息
        self._show_statistics()
        
        print("异步匹配引擎已停止")
    
    def _show_statistics(self):
        """显示统计信息"""
        spider_stats = self.spider.get_statistics()
        buffer_stats = self.buffer_manager.get_buffer_status()
        
        print("\n=== 异步处理统计信息 ===")
        print(f"处理总数: {self.processed_count}")
        print(f"匹配成功: {self.matched_count}")
        print(f"匹配失败: {self.unmatched_count}")
        print(f"成功率: {self.matched_count / max(self.processed_count, 1) * 100:.2f}%")
        
        print(f"\nAPI请求统计:")
        print(f"  总请求数: {spider_stats['total_requests']}")
        print(f"  成功请求: {spider_stats['successful_requests']}")
        print(f"  失败请求: {spider_stats['failed_requests']}")
        print(f"  成功率: {spider_stats['success_rate'] * 100:.2f}%")
        print(f"  503错误数: {spider_stats['total_503_errors']}")
        print(f"  连续503错误: {spider_stats['consecutive_503_errors']}")
        
        print(f"\n缓冲池统计:")
        print(f"  总写入次数: {buffer_stats['total_writes']}")
        print(f"  总数据项: {buffer_stats['total_items']}")
    
    async def match_bgm_games_async(
        self,
        input_file: str,
        output_file: str = "save/ymgames_matched_async.xlsx",
        unmatched_file: str = "save/ymgames_unmatched_async.xlsx",
        org_output_file: str = "save/organizations_info_async.xlsx"
    ) -> None:
        """
        异步匹配Bangumi游戏数据
        
        Args:
            input_file: 输入文件路径
            output_file: 匹配结果输出文件
            unmatched_file: 未匹配结果输出文件
            org_output_file: 会社信息输出文件
        """
        # 确保输出目录存在
        os.makedirs("save", exist_ok=True)
        
        # 注册输出文件到缓冲池
        matched_columns = [
            "bgm_id", "bgm游戏", "日文名 (原始)", "中文名 (原始)",
            "name", "chineseName", "ym_id", "score",
            "orgId", "orgName", "orgWebsite", "orgDescription",
            "匹配来源"
        ]
        
        org_columns = [
            "org_id", "name", "chineseName", "website", "description", "birthday"
        ]
        
        self.buffer_manager.register_file("matched", output_file, matched_columns)
        self.buffer_manager.register_file("unmatched", unmatched_file, ["原始的未匹配bgm游戏名称"])
        self.buffer_manager.register_file("org", org_output_file, org_columns)
        
        # 读取输入数据
        df_bgm = self.data_processor.read_bgm_data(input_file)
        
        # 获取已处理的ID（断点续传）
        processed_ids = self.data_processor.get_processed_ids(output_file)
        
        # 获取已处理的会社信息
        processed_orgs = self.data_processor.get_processed_orgs(org_output_file)
        
        # 创建任务列表
        tasks = []
        for idx, row in df_bgm.iterrows():
            bgm_id = str(row['id']) if 'id' in row and pd.notna(row['id']) else f"ROW_{idx}"
            
            if bgm_id in processed_ids:
                continue
            
            jp_name = str(row["日文名"]).strip() if pd.notna(row["日文名"]) else ""
            cn_name = str(row["中文名"]).strip() if pd.notna(row["中文名"]) else ""
            
            if not jp_name and not cn_name:
                continue
            
            tasks.append({
                "id": bgm_id,
                "jp_name": jp_name,
                "cn_name": cn_name,
                "row_index": idx
            })
        
        # 批量处理任务
        await self._process_tasks_batch(tasks, processed_orgs)
    
    async def _process_tasks_batch(self, tasks: List[Dict], processed_orgs: Dict):
        """批量处理任务"""
        print(f"开始处理 {len(tasks)} 个任务，批次大小: {self.batch_size}")
        
        # 创建进度条
        pbar = tqdm(total=len(tasks), desc="异步处理游戏", unit="个")
        
        try:
            for i in range(0, len(tasks), self.batch_size):
                batch = tasks[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (len(tasks) + self.batch_size - 1) // self.batch_size
                
                # 创建协程任务
                coroutines = [self._process_single_task(task, processed_orgs) for task in batch]
                
                # 并发执行
                results = await asyncio.gather(*coroutines, return_exceptions=True)
                
                # 处理结果
                batch_processed = 0
                for result in results:
                    if not isinstance(result, Exception):
                        self.processed_count += 1
                        batch_processed += 1
                        if result.get("matched"):
                            self.matched_count += 1
                        else:
                            self.unmatched_count += 1
                        # 更新进度条
                        pbar.update(1)
                
                # 显示批次信息
                pbar.set_postfix({
                    '批次': f'{batch_num}/{total_batches}',
                    '已处理': self.processed_count,
                    '成功': self.matched_count,
                    '失败': self.unmatched_count
                })
        finally:
            pbar.close()
    
    async def _process_single_task(self, task: Dict, processed_orgs: Dict) -> Dict:
        """处理单个任务"""
        bgm_id = task["id"]
        jp_name = task["jp_name"]
        cn_name = task["cn_name"]
        
        best_match = None
        best_score = -1.0
        match_source = ""
        
        # 尝试匹配日文名
        if jp_name:
            jp_matches = await self.spider.search_game_async(jp_name)
            if jp_matches and jp_matches[0]["score"] > best_score:
                best_match = jp_matches[0]
                best_score = best_match["score"]
                match_source = "日文名"
        
        # 尝试匹配中文名
        if cn_name:
            cn_matches = await self.spider.search_game_async(cn_name)
            if cn_matches and cn_matches[0]["score"] > best_score:
                best_match = cn_matches[0]
                best_score = best_match["score"]
                match_source = "中文名"
        
        if best_match:
            # 处理会社信息
            org_id = str(best_match.get("orgId", ""))
            org_info = None
            
            if org_id:
                if org_id in processed_orgs:
                    org_info = processed_orgs[org_id]["info"]
                else:
                    org_info = await self.spider.get_organization_details_async(org_id)
                    if org_info:
                        processed_orgs[org_id] = {"info": org_info, "retry_count": 0}
                        await self.buffer_manager.put_data("org", org_info)
            
            # 准备匹配结果数据
            matched_data = {
                "bgm_id": bgm_id,
                "bgm游戏": jp_name if jp_name else cn_name,
                "日文名 (原始)": jp_name,
                "中文名 (原始)": cn_name,
                "name": best_match["name"],
                "chineseName": best_match["chineseName"],
                "ym_id": best_match["ym_id"],
                "score": best_match["score"],
                "orgId": org_id,
                "orgName": (org_info or {}).get("name", best_match.get("orgName", "")),
                "orgWebsite": (org_info or {}).get("website", best_match.get("orgWebsite", "")),
                "orgDescription": (org_info or {}).get("description", best_match.get("orgDescription", "")),
                "匹配来源": match_source
            }
            
            await self.buffer_manager.put_data("matched", matched_data)
            
            return {"matched": True, "data": matched_data}
        else:
            # 记录未匹配
            unmatched_data = {"原始的未匹配bgm游戏名称": f"ID_{bgm_id}_未匹配"}
            await self.buffer_manager.put_data("unmatched", unmatched_data)
            
            return {"matched": False, "data": unmatched_data}
    
    async def match_bgm_games_with_aliases_async(
        self,
        input_file: str,
        output_file: str = "save/ymgames_matched_aliases_async.xlsx",
        unmatched_file: str = "save/ymgames_unmatched_aliases_async.xlsx",
        org_output_file: str = "save/organizations_info_aliases_async.xlsx"
    ) -> None:
        """
        异步匹配包含别名的Bangumi游戏数据
        """
        # 确保输出目录存在
        os.makedirs("save", exist_ok=True)
        
        # 注册输出文件到缓冲池
        matched_columns = [
            "bgm_id", "bgm游戏", "name", "chineseName", "ym_id", "score",
            "orgId", "orgName", "orgWebsite", "orgDescription", "匹配来源"
        ]
        
        org_columns = [
            "org_id", "name", "chineseName", "website", "description", "birthday"
        ]
        
        self.buffer_manager.register_file("matched", output_file, matched_columns)
        self.buffer_manager.register_file("unmatched", unmatched_file, ["原始的未匹配bgm游戏名称"])
        self.buffer_manager.register_file("org", org_output_file, org_columns)
        
        # 读取输入数据
        df_bgm = self.data_processor.read_bgm_data_with_aliases(input_file)
        
        # 获取已处理的ID（断点续传）
        processed_ids = self.data_processor.get_processed_ids(output_file)
        
        # 获取已处理的会社信息
        processed_orgs = self.data_processor.get_processed_orgs(org_output_file)
        
        # 创建任务列表
        tasks = []
        for idx, row in df_bgm.iterrows():
            bgm_id = str(row['bgm_id']) if 'bgm_id' in row and pd.notna(row['bgm_id']) else f"ROW_{idx}"
            
            if bgm_id in processed_ids:
                continue
            
            # 获取别名列表
            alias_cols = [col for col in row.index if col.startswith("别名")]
            aliases = [str(row[col]).strip() for col in alias_cols if pd.notna(row[col]) and str(row[col]).strip()]
            
            if not aliases:
                continue
            
            # 获取原始分数
            original_score = 0.0
            if 'score' in row and pd.notna(row['score']):
                try:
                    original_score = float(row['score'])
                except (ValueError, TypeError):
                    pass
            
            tasks.append({
                "id": bgm_id,
                "aliases": aliases,
                "original_score": original_score,
                "original_data": row.to_dict(),
                "row_index": idx
            })
        
        # 批量处理任务
        await self._process_alias_tasks_batch(tasks, processed_orgs)
    
    async def _process_alias_tasks_batch(self, tasks: List[Dict], processed_orgs: Dict):
        """批量处理别名任务"""
        print(f"开始处理 {len(tasks)} 个任务（别名匹配），批次大小: {self.batch_size}")
        
        # 创建进度条
        pbar = tqdm(total=len(tasks), desc="异步处理游戏（别名匹配）", unit="个")
        
        try:
            for i in range(0, len(tasks), self.batch_size):
                batch = tasks[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (len(tasks) + self.batch_size - 1) // self.batch_size
                
                coroutines = [self._process_single_alias_task(task, processed_orgs) for task in batch]
                results = await asyncio.gather(*coroutines, return_exceptions=True)
                
                for result in results:
                    if not isinstance(result, Exception):
                        self.processed_count += 1
                        if result.get("matched"):
                            self.matched_count += 1
                        else:
                            self.unmatched_count += 1
                        # 更新进度条
                        pbar.update(1)
                
                # 显示批次信息
                pbar.set_postfix({
                    '批次': f'{batch_num}/{total_batches}',
                    '已处理': self.processed_count,
                    '成功': self.matched_count,
                    '失败': self.unmatched_count
                })
        finally:
            pbar.close()
    
    async def _process_single_alias_task(self, task: Dict, processed_orgs: Dict) -> Dict:
        """处理单个别名任务"""
        bgm_id = task["id"]
        aliases = task["aliases"]
        original_score = task["original_score"]
        original_data = task["original_data"]
        
        best_match = None
        best_score = -1.0
        match_source = ""
        
        # 查找所有别名中的最佳匹配
        for i, alias in enumerate(aliases):
            matches = await self.spider.search_game_async(alias)
            if matches and matches[0]["score"] > best_score:
                best_match = matches[0]
                best_score = matches[0]["score"]
                match_source = f"别名{i+1}"
        
        # 比较分数，决定是否使用新数据
        if best_match and best_score > original_score:
            # 处理会社信息
            org_id = str(best_match.get("orgId", ""))
            org_info = None
            
            if org_id:
                if org_id in processed_orgs:
                    org_info = processed_orgs[org_id]["info"]
                else:
                    org_info = await self.spider.get_organization_details_async(org_id)
                    if org_info:
                        processed_orgs[org_id] = {"info": org_info, "retry_count": 0}
                        await self.buffer_manager.put_data("org", org_info)
            
            # 准备新匹配结果数据
            matched_data = {
                "bgm_id": bgm_id,
                "bgm游戏": original_data.get('bgm游戏') or original_data.get('原始bgm游戏名称'),
                "name": best_match["name"],
                "chineseName": best_match["chineseName"],
                "ym_id": best_match["ym_id"],
                "score": best_score,
                "orgId": org_id,
                "orgName": (org_info or {}).get("name", best_match.get("orgName", "")),
                "orgWebsite": (org_info or {}).get("website", best_match.get("orgWebsite", "")),
                "orgDescription": (org_info or {}).get("description", best_match.get("orgDescription", "")),
                "匹配来源": match_source
            }
            
            await self.buffer_manager.put_data("matched", matched_data)
            return {"matched": True, "data": matched_data}
        else:
            # 使用原始数据
            if original_data.get("name"):  # 如果有原始匹配结果
                matched_data = {
                    "bgm_id": bgm_id,
                    "bgm游戏": original_data.get('bgm游戏') or original_data.get('原始bgm游戏名称'),
                    "name": original_data.get('name'),
                    "chineseName": original_data.get('chineseName'),
                    "ym_id": original_data.get('ym_id'),
                    "score": original_score,
                    "orgId": original_data.get('orgId'),
                    "orgName": original_data.get('orgName'),
                    "orgWebsite": original_data.get('orgWebsite'),
                    "orgDescription": original_data.get('orgDescription'),
                    "匹配来源": "原始数据"
                }
                await self.buffer_manager.put_data("matched", matched_data)
                return {"matched": True, "data": matched_data}
            else:
                # 记录未匹配
                unmatched_data = {"原始的未匹配bgm游戏名称": f"ID_{bgm_id}_未匹配"}
                await self.buffer_manager.put_data("unmatched", unmatched_data)
                return {"matched": False, "data": unmatched_data} 