"""
异步爬虫引擎
整合拥塞控制和缓冲区管理，实现高效的异步爬取功能
"""

import asyncio
import aiohttp
import time
import os
import json
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass

from .congestion_control import CongestionController
from .buffer_manager import BufferManager, BufferConfig
from ..utils.logger import Logger


@dataclass
class SpiderConfig:
    """爬虫配置"""
    max_retries: int = 3
    timeout: int = 30
    delay_between_requests: float = 0.1
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    headers: Optional[Dict[str, str]] = None
    session_timeout: int = 300


class AsyncSpiderEngine:
    """异步爬虫引擎"""
    
    def __init__(self, 
                 api_client,
                 spider_config: Optional[SpiderConfig] = None,
                 buffer_config: Optional[BufferConfig] = None,
                 max_rows: int = 50):
        """
        初始化异步爬虫引擎
        
        Args:
            api_client: API客户端实例
            spider_config: 爬虫配置
            buffer_config: 缓冲区配置
            max_rows: 每次处理的最大行数
        """
        self.api_client = api_client
        self.config = spider_config or SpiderConfig(delay_between_requests=1.5)
        self.buffer_config = buffer_config or BufferConfig()
        self.max_rows = max_rows
        
        # 初始化组件
        self.congestion_controller = CongestionController(max_cwnd=1)
        self.logger = Logger(silent_mode=True)
        
        # 会话管理
        self.session = None
        self.is_running = False
        
        # 断点续传支持
        self.processed_ids = set()
        self.checkpoint_file = "spider_checkpoint.json"
        self._load_checkpoint()
    
    def _load_checkpoint(self) -> None:
        """加载断点续传信息"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    self.processed_ids = set(checkpoint_data.get('processed_ids', []))
                    print(f"已加载断点信息，跳过 {len(self.processed_ids)} 个已处理项目")
        except Exception as e:
            print(f"加载断点信息失败: {e}")
    
    def _save_checkpoint(self) -> None:
        """保存断点续传信息"""
        try:
            checkpoint_data = {
                'processed_ids': list(self.processed_ids),
                'timestamp': time.time()
            }
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存断点信息失败: {e}")
    
    async def _create_session(self) -> aiohttp.ClientSession:
        """创建aiohttp会话"""
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        headers = self.config.headers or {
            'User-Agent': self.config.user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        return aiohttp.ClientSession(
            timeout=timeout,
            headers=headers,
            connector=aiohttp.TCPConnector(
                limit=100,  # 连接池大小
                limit_per_host=30,  # 每个主机的连接数限制
                ttl_dns_cache=300,  # DNS缓存时间
                use_dns_cache=True
            )
        )
    
    async def _make_request(self, 
                          session: aiohttp.ClientSession,
                          url: str,
                          params: Optional[Dict] = None,
                          headers: Optional[Dict] = None) -> Optional[Dict]:
        """
        发送异步请求
        
        Args:
            session: aiohttp会话
            url: 请求URL
            params: 查询参数
            headers: 请求头
            
        Returns:
            响应数据或None
        """
        for attempt in range(self.config.max_retries):
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        await self.congestion_controller.record_success()
                        return data
                    elif response.status == 401:
                        # Token失效，尝试刷新
                        print("Token失效，尝试刷新...")
                        if hasattr(self.api_client, 'get_access_token'):
                            new_token = self.api_client.get_access_token()
                            if new_token:
                                self.api_client.token_ref["value"] = new_token
                                # 更新请求头中的token
                                if headers:
                                    headers['Authorization'] = f"Bearer {new_token}"
                                continue
                    else:
                        print(f"请求失败: {response.status}")
                        await self.congestion_controller.record_failure()
                        
            except asyncio.TimeoutError:
                print(f"请求超时 (尝试 {attempt + 1}/{self.config.max_retries})")
                await self.congestion_controller.record_failure()
            except Exception as e:
                print(f"请求异常: {e} (尝试 {attempt + 1}/{self.config.max_retries})")
                await self.congestion_controller.record_failure()
            
            # 重试前等待
            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # 指数退避
        
        return None
    
    async def _search_game_async(self, 
                               session: aiohttp.ClientSession,
                               keyword: str,
                               token: str) -> List[Dict[str, Any]]:
        """
        异步搜索游戏
        
        Args:
            session: aiohttp会话
            keyword: 搜索关键词
            token: 访问令牌
            
        Returns:
            搜索结果列表
        """
        url = f"{self.api_client.base_url}/open/archive/search-game"
        params = {
            "mode": "list",
            "keyword": keyword,
            "pageNum": 1,
            "pageSize": 20,
            "includeOrg": "true"
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "version": "1"
        }
        
        data = await self._make_request(session, url, params, headers)
        if data:
            return self.api_client.parse_search_response(data)
        return []
    
    async def _get_org_details_async(self, 
                                   session: aiohttp.ClientSession,
                                   org_id: str,
                                   token: str) -> Optional[Dict[str, Any]]:
        """
        异步获取会社详细信息
        
        Args:
            session: aiohttp会话
            org_id: 会社ID
            token: 访问令牌
            
        Returns:
            会社详细信息或None
        """
        url = f"{self.api_client.base_url}/open/archive"
        params = {"orgId": org_id}
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "version": "1"
        }
        
        data = await self._make_request(session, url, params, headers)
        if not data:
            return None
        
        try:
            org_data = data.get("data", {}).get("org", {})
            if not org_data:
                return None
            
            # 按优先级提取官网地址
            website = ""
            if isinstance(org_data.get("website"), list):
                priority = ["homepage", "官网", "官方网站", "official website"]
                for title in priority:
                    for site in org_data["website"]:
                        if site.get("title", "").lower() == title.lower():
                            website = site.get("link", "")
                            break
                    if website:
                        break
                if not website and org_data["website"]:
                    website = org_data["website"][0].get("link", "")
            
            # 组装结果
            result = {
                "id": org_id,
                "name": org_data.get("name", ""),
                "chineseName": org_data.get("chineseName", ""),
                "website": website,
                "description": org_data.get("introduction", ""),
                "birthday": org_data.get("birthday", "")
            }
            return result
            
        except Exception as e:
            print(f"解析会社信息失败: {e}")
            return None
    
    async def _process_game_item(self,
                               session: aiohttp.ClientSession,
                               game_data: Dict[str, Any],
                               buffer_manager: BufferManager,
                               return_status: bool = False) -> str:
        """
        处理单个游戏项目
        
        Args:
            session: aiohttp会话
            game_data: 游戏数据
            buffer_manager: 缓冲区管理器
            return_status: 是否返回处理状态
            
        Returns:
            处理状态
        """
        try:
            bgm_id = str(game_data.get('id', ''))
            jp_name = str(game_data.get('日文名', '')).strip()
            cn_name = str(game_data.get('中文名', '')).strip()
            if bgm_id in self.processed_ids:
                return 'skip' if return_status else None
            token = self.api_client.token_ref.get("value")
            if not token:
                return 'fail' if return_status else None
            best_match = None
            best_score = -1.0
            match_source = ""
            # 尝试匹配日文名
            if jp_name:
                jp_matches = await self._search_game_async(session, jp_name, token)
                if isinstance(jp_matches, int) and jp_matches == 503:
                    return 503 if return_status else None
                if jp_matches and jp_matches[0]["score"] > best_score:
                    best_match = jp_matches[0]
                    best_score = best_match["score"]
                    match_source = "日文名"
            # 尝试匹配中文名
            if cn_name:
                cn_matches = await self._search_game_async(session, cn_name, token)
                if isinstance(cn_matches, int) and cn_matches == 503:
                    return 503 if return_status else None
                if cn_matches and cn_matches[0]["score"] > best_score:
                    best_match = cn_matches[0]
                    best_score = best_match["score"]
                    match_source = "中文名"
            if best_match:
                org_id = str(best_match.get("orgId", ""))
                org_info = None
                if org_id:
                    org_details = await self._get_org_details_async(session, org_id, token)
                    if org_details:
                        org_info = org_details
                    else:
                        org_info = {
                            "id": org_id,
                            "name": best_match.get("orgName", ""),
                            "website": "",
                            "description": ""
                        }
                # 强制所有字段都写入，严格对齐DataProcessor.EXCEL_COLUMNS_MATCHED
                result_data = {
                    "bgm_id": bgm_id,
                    "bgm游戏": game_data.get('游戏名', ''),
                    "日文名 (原始)": jp_name,
                    "中文名 (原始)": cn_name,
                    "name": best_match.get("name", ""),
                    "chineseName": best_match.get("chineseName", ""),
                    "ym_id": best_match.get("ym_id", ""),
                    "score": best_match.get("score", ""),
                    "orgId": org_id,
                    "orgName": (org_info or {}).get("name", ""),
                    "orgWebsite": (org_info or {}).get("website", ""),
                    "orgDescription": (org_info or {}).get("description", ""),
                    "匹配来源": match_source
                }
                await buffer_manager.put_data(result_data)
                self.processed_ids.add(bgm_id)
                await asyncio.sleep(self.config.delay_between_requests)
                return 'success' if return_status else None
            else:
                unmatched_data = {
                    "bgm_id": bgm_id,
                    "bgm游戏": game_data.get('游戏名', ''),
                    "日文名 (原始)": jp_name,
                    "中文名 (原始)": cn_name,
                    "name": "",
                    "chineseName": "",
                    "ym_id": "",
                    "score": 0.0,
                    "orgId": "",
                    "orgName": "",
                    "orgWebsite": "",
                    "orgDescription": "",
                    "匹配来源": "未匹配"
                }
                await buffer_manager.put_data(unmatched_data)
                self.processed_ids.add(bgm_id)
                await asyncio.sleep(self.config.delay_between_requests)
                return 'success' if return_status else None
        except Exception as e:
            if return_status:
                return 503
            return None
    
    async def crawl_games_async(self,
                              input_file: str,
                              output_file: str,
                              unmatched_file: str) -> None:
        try:
            if not self.api_client.initialize_token():
                return
            import pandas as pd
            df = pd.read_excel(input_file)
            df = df[~df['id'].astype(str).isin(self.processed_ids)]
            all_rows = df.to_dict(orient='records')
            batch_size = 10
            delay = 2.0
            min_delay = 1.0
            max_delay = 10.0
            max_retry = 5
            buffer_manager = BufferManager(
                output_file=output_file,
                buffer_config=self.buffer_config
            )
            await buffer_manager.start()
            self.session = await self._create_session()
            total = len(all_rows)
            processed = 0
            for batch_start in range(0, total, batch_size):
                batch = all_rows[batch_start:batch_start+batch_size]
                fail_items = []
                success, fail_503 = 0, 0
                for item in batch:
                    for retry in range(max_retry):
                        try:
                            resp = await self._process_game_item(self.session, item, buffer_manager, return_status=True)
                            if resp == 503:
                                fail_503 += 1
                                await asyncio.sleep(delay * (2 ** retry))
                            elif resp == 'success':
                                success += 1
                                break
                            else:
                                break
                        except Exception:
                            fail_503 += 1
                            await asyncio.sleep(delay * (2 ** retry))
                    else:
                        fail_items.append(item)
                    await asyncio.sleep(delay)
                processed += len(batch)
                print(f"进度: {min(processed, total)}/{total} ({min(processed, total)/total*100:.1f}%)")
                # 动态调整速率
                total_req = success + fail_503
                rate_503 = fail_503 / total_req if total_req else 0
                if rate_503 > 0.2:
                    delay = min(delay + 1, max_delay)
                elif rate_503 < 0.05:
                    delay = max(delay - 0.2, min_delay)
                await asyncio.sleep(10)
                if fail_items:
                    all_rows.extend(fail_items)
            await asyncio.sleep(2)
        except Exception as e:
            print(f"主循环异常: {e}")
        finally:
            if self.session:
                await self.session.close()
            if buffer_manager:
                await buffer_manager.stop()

    async def crawl_with_aliases_async(self,
                                     input_file: str,
                                     output_file: str,
                                     unmatched_file: str) -> None:
        try:
            if not self.api_client.initialize_token():
                return
            import pandas as pd
            df = pd.read_excel(input_file)
            df = df.head(self.max_rows)
            buffer_manager = BufferManager(
                output_file=output_file,
                buffer_config=self.buffer_config
            )
            await buffer_manager.start()
            self.session = await self._create_session()
            cwnd = await self.congestion_controller.get_current_window()
            batch_size = cwnd
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                tasks = []
                for _, row in batch.iterrows():
                    game_data = row.to_dict()
                    task = self._process_game_with_aliases(self.session, game_data, buffer_manager)
                    tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)
                cwnd = await self.congestion_controller.get_current_window()
                batch_size = cwnd
                self._save_checkpoint()
                progress = min(i + batch_size, len(df))
                print(f"进度: {progress}/{len(df)} ({progress/len(df)*100:.1f}%)")
        except Exception as e:
            pass
        finally:
            if self.session:
                await self.session.close()
            if buffer_manager:
                await buffer_manager.stop()
    
    async def _process_game_with_aliases(self,
                                       session: aiohttp.ClientSession,
                                       game_data: Dict[str, Any],
                                       buffer_manager: BufferManager) -> None:
        """
        使用别名处理单个游戏项目
        
        Args:
            session: aiohttp会话
            game_data: 游戏数据
            buffer_manager: 缓冲区管理器
        """
        try:
            bgm_id = str(game_data.get('bgm_id', ''))
            if bgm_id in self.processed_ids:
                return
            alias_cols = [col for col in game_data.keys() if col.startswith("别名")]
            aliases = [str(game_data[col]).strip() for col in alias_cols if game_data[col] and str(game_data[col]).strip()]
            original_score = 0.0
            if 'score' in game_data and game_data['score']:
                try:
                    original_score = float(game_data['score'])
                except (ValueError, TypeError):
                    pass
            token = self.api_client.token_ref.get("value")
            if not token:
                print("无法获取访问令牌")
                return
            best_match = None
            best_score = -1.0
            match_source = ""
            for i, alias in enumerate(aliases):
                matches = await self._search_game_async(session, alias, token)
                if matches and matches[0]["score"] > best_score:
                    best_match = matches[0]
                    best_score = best_match["score"]
                    match_source = f"别名{i+1}"
            if best_match and best_score > original_score:
                org_id = str(best_match.get("orgId", ""))
                org_info = None
                if org_id:
                    org_details = await self._get_org_details_async(session, org_id, token)
                    if org_details:
                        org_info = org_details
                    else:
                        org_info = {
                            "id": org_id,
                            "name": best_match.get("orgName", ""),
                            "website": "",
                            "description": ""
                        }
                # 强制所有字段都写入，严格对齐DataProcessor.EXCEL_COLUMNS_MATCHED
                result_data = {
                    "bgm_id": bgm_id,
                    "bgm游戏": game_data.get('游戏名', ''),
                    "日文名 (原始)": game_data.get('日文名', ''),
                    "中文名 (原始)": game_data.get('中文名', ''),
                    "name": best_match.get("name", ""),
                    "chineseName": best_match.get("chineseName", ""),
                    "ym_id": best_match.get("ym_id", ""),
                    "score": best_match.get("score", ""),
                    "orgId": org_id,
                    "orgName": (org_info or {}).get("name", ""),
                    "orgWebsite": (org_info or {}).get("website", ""),
                    "orgDescription": (org_info or {}).get("description", ""),
                    "匹配来源": match_source
                }
                await buffer_manager.put_data(result_data)
            self.processed_ids.add(bgm_id)
            await asyncio.sleep(self.config.delay_between_requests)
        except Exception as e:
            print(f"处理游戏别名时出错: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取爬虫统计信息"""
        congestion_stats = self.congestion_controller.get_stats()
        return {
            "processed_count": len(self.processed_ids),
            "congestion_stats": congestion_stats,
            "is_running": self.is_running
        } 