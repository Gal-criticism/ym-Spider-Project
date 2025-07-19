import asyncio
import aiohttp
import time
import json
from typing import List, Dict, Any, Optional, Callable
from asyncio import Semaphore
from tqdm import tqdm

from ..utils.logger import Logger




class AsyncSpiderEngine:
    """异步爬虫引擎，基于asyncio和aiohttp实现高并发异步爬取"""
    
    def __init__(self, 
                 max_concurrent: int = 10,
                 request_delay: float = 0.1,
                 max_retries: int = 3,
                 timeout: int = 30):
        """
        初始化异步爬虫引擎
        
        Args:
            max_concurrent: 最大并发数
            request_delay: 请求间隔（秒）
            max_retries: 最大重试次数
            timeout: 请求超时时间（秒）
        """
        self.max_concurrent = max_concurrent
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout
        
        # 控制并发和限流
        self.semaphore = Semaphore(max_concurrent)
        self.last_request_time = 0
        
        # 统计信息
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.retry_count = 0
        
        # 503错误统计
        self.consecutive_503_errors = 0
        self.total_503_errors = 0
        
        # 日志记录器
        self.logger = Logger(silent_mode=True)
        

        
        # API配置
        self.base_url = "https://www.ym.com"
        self.client_id = "ym"
        self.client_secret = "abc114514"
        self.access_token = None
        
    async def initialize_token(self) -> bool:
        """异步获取访问令牌"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/oauth/token"
                data = {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "public"
                }
                
                async with session.post(url, data=data, timeout=self.timeout) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.access_token = result.get("access_token")
                        return True
                    else:
                        self.logger.log_error(f"获取 token 失败: {response.status}, {await response.text()}")
                        return False
        except Exception as e:
            self.logger.log_error(f"获取 token 异常: {e}")
            return False
    
    async def _rate_limit(self):
        """自适应限流控制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # 动态调整请求间隔，503错误时增加延迟
        if hasattr(self, 'consecutive_503_errors'):
            if self.consecutive_503_errors > 5:
                # 连续503错误超过5次，大幅增加延迟
                delay = max(self.request_delay * 3, 2.0)
            elif self.consecutive_503_errors > 2:
                # 连续503错误超过2次，增加延迟
                delay = max(self.request_delay * 2, 1.0)
            else:
                delay = self.request_delay
        else:
            delay = self.request_delay
        
        if time_since_last < delay:
            await asyncio.sleep(delay - time_since_last)
        
        self.last_request_time = time.time()
    
    async def _make_request_with_retry(self, session: aiohttp.ClientSession, 
                                     url: str, params: Dict = None, 
                                     headers: Dict = None) -> Optional[Dict]:
        """带重试机制的异步请求"""
        for attempt in range(self.max_retries + 1):
            try:
                await self._rate_limit()
                
                async with self.semaphore:
                    async with session.get(url, params=params, headers=headers, 
                                         timeout=self.timeout) as response:
                        self.total_requests += 1
                        
                        if response.status == 200:
                            self.successful_requests += 1
                            # 成功请求后重置503错误计数
                            self.consecutive_503_errors = 0
                            return await response.json()
                        elif response.status == 401:
                            # Token失效，重新获取
                            self.logger.log_important("Token 失效，正在重新获取…")
                            if await self.initialize_token():
                                headers["Authorization"] = f"Bearer {self.access_token}"
                                continue
                            else:
                                self.logger.log_error("重新获取 token 失败")
                                self.failed_requests += 1
                                return None
                        # 新增：403/404/410 致命错误
                        elif response.status in (403, 404, 410):
                            self.logger.log_error(f"接口返回致命错误: {response.status}, {await response.text()}")
                            raise RuntimeError(f"接口返回致命错误: {response.status}")
                        elif response.status == 503:
                            # 503错误，增加延迟并重试
                            self.consecutive_503_errors += 1
                            self.total_503_errors += 1
                            
                            if attempt < self.max_retries:
                                # 503错误使用更长的退避时间
                                backoff_time = min(2 ** attempt + 1, 10)  # 最大10秒
                                await asyncio.sleep(backoff_time)
                                continue
                            else:
                                self.logger.log_error(f"503错误重试失败，已重试{self.max_retries}次")
                                self.failed_requests += 1
                                return None
                        else:
                            if attempt < self.max_retries:
                                await asyncio.sleep(2 ** attempt)  # 指数退避
                                continue
                            else:
                                self.logger.log_error(f"请求失败: {response.status}, {await response.text()}")
                                self.failed_requests += 1
                                return None
                                
            except asyncio.TimeoutError:
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    self.failed_requests += 1
                    return None
            except Exception as e:
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    self.failed_requests += 1
                    return None
        
        return None
    
    async def search_game_async(self, keyword: str, top_k: int = 3, 
                               threshold: float = 0.8) -> List[Dict[str, Any]]:
        """异步搜索游戏"""
        if not self.access_token:
            if not await self.initialize_token():
                return []
        
        url = f"{self.base_url}/open/archive/search-game"
        params = {
            "mode": "list",
            "keyword": keyword,
            "pageNum": 1,
            "pageSize": 20,
            "includeOrg": "true"
        }
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "version": "1"
        }
        
        async with aiohttp.ClientSession() as session:
            response_data = await self._make_request_with_retry(session, url, params, headers)
            
            if not response_data:
                return []
            
            # 记录API响应到日志文件
            self.logger.log_api_response(keyword, response_data)
            
            # 解析响应数据
            results = response_data.get("data", {}).get("result", [])
            parsed_results = []
            
            for item in results:
                try:
                    score = float(item.get("score", 0))
                except (ValueError, TypeError):
                    score = 0.0
                
                org_info = item.get("org", {}) or {
                    "id": item.get("orgId", ""),
                    "name": item.get("orgName", ""),
                    "website": item.get("orgWebsite", ""),
                    "description": item.get("orgDescription", "")
                }
                
                # 记录会社信息到日志
                if org_info:
                    self.logger.log_api_response("org_info_found", {
                        "org_name": org_info.get('name', ''),
                        "org_id": org_info.get('id', '')
                    })
                
                parsed_results.append({
                    "name": item.get("name", ""),
                    "chineseName": item.get("chineseName", ""),
                    "ym_id": item.get("id", ""),
                    "score": round(score, 4),
                    "orgId": org_info.get("id", ""),
                    "orgName": org_info.get("name", ""),
                    "orgWebsite": org_info.get("website", ""),
                    "orgDescription": org_info.get("description", "")
                })
            
            # 排序和过滤
            parsed_results.sort(key=lambda x: x["score"], reverse=True)
            
            if parsed_results and parsed_results[0]["score"] >= threshold:
                return parsed_results[:1]
            
            return parsed_results[:top_k]
    
    async def get_organization_details_async(self, org_id: str) -> Optional[Dict[str, Any]]:
        """异步获取会社详细信息"""
        if not self.access_token:
            if not await self.initialize_token():
                return None
        
        url = f"{self.base_url}/open/archive"
        params = {"orgId": org_id}
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "version": "1"
        }
        
        async with aiohttp.ClientSession() as session:
            response_data = await self._make_request_with_retry(session, url, params, headers)
            
            if not response_data:
                return None
            
            # 记录会社详情API响应到日志文件
            self.logger.log_api_response(f"org_details_{org_id}", response_data)
            
            org_data = response_data.get("data", {}).get("org", {})
            if not org_data:
                self.logger.log_info("API 响应中未找到会社信息")
                return None
            
            # 提取官网地址
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
            
            return {
                "id": org_id,
                "name": org_data.get("name", ""),
                "chineseName": org_data.get("chineseName", ""),
                "website": website,
                "description": org_data.get("introduction", ""),
                "birthday": org_data.get("birthday", "")
            }
    
    async def process_batch_async(self, tasks: List[Dict], 
                                progress_callback: Optional[Callable] = None) -> List[Dict]:
        """批量异步处理任务"""
        results = []
        
        # 创建进度条
        pbar = tqdm(total=len(tasks), desc="异步API处理", unit="个")
        
        async def process_single_task(task):
            """处理单个任务"""
            try:
                # 搜索游戏
                search_results = await self.search_game_async(
                    task.get("keyword", ""),
                    top_k=task.get("top_k", 3),
                    threshold=task.get("threshold", 0.8)
                )
                
                # 获取会社信息
                if search_results and search_results[0].get("orgId"):
                    org_details = await self.get_organization_details_async(
                        search_results[0]["orgId"]
                    )
                    if org_details:
                        search_results[0].update({
                            "orgName": org_details.get("name", search_results[0].get("orgName", "")),
                            "orgWebsite": org_details.get("website", search_results[0].get("orgWebsite", "")),
                            "orgDescription": org_details.get("description", search_results[0].get("orgDescription", ""))
                        })
                
                result = {
                    "task_id": task.get("id"),
                    "keyword": task.get("keyword"),
                    "results": search_results,
                    "success": True
                }
                
            except Exception as e:
                result = {
                    "task_id": task.get("id"),
                    "keyword": task.get("keyword"),
                    "results": [],
                    "success": False,
                    "error": str(e)
                }
            
            # 更新进度
            pbar.update(1)
            if progress_callback:
                progress_callback(result)
            
            return result
        
        try:
            # 并发执行所有任务
            tasks_coros = [process_single_task(task) for task in tasks]
            results = await asyncio.gather(*tasks_coros, return_exceptions=True)
        finally:
            pbar.close()
        
        # 过滤异常结果
        valid_results = []
        for result in results:
            if not isinstance(result, Exception):
                valid_results.append(result)
        
        return valid_results
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": self.successful_requests / max(self.total_requests, 1),
            "retry_count": self.retry_count,
            "total_503_errors": self.total_503_errors,
            "consecutive_503_errors": self.consecutive_503_errors
        } 