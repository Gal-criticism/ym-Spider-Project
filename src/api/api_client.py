import json
import requests
from typing import List, Dict, Any, Optional

from ..utils.logger import Logger


class YMGalAPIClient:
    """月幕游戏API客户端"""
    
    def __init__(self):
        self.base_url = "https://www.ymgal.com"
        self.client_id = ""
        self.client_secret = ""
        self.token_ref = {"value": None}
        self.logger = Logger(silent_mode=True)  # 使用静默模式
    
    def get_access_token(self) -> Optional[str]:
        """
        调用 OAuth2 *Client Credentials* 模式获取 **access_token**，有效期 1 小时。

        Returns
        -------
        str | None
            成功时返回 token 字符串；失败时打印错误并返回 ``None``。
        """
        url = f"{self.base_url}/oauth/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,  # 固定 client_id，由月幕平台提供
            "client_secret": self.client_secret,  # 固定 client_secret，由月幕平台提供
            "scope": "public"  # 只申请公开数据权限
        }
        response = requests.post(url, data=data)

        if response.status_code == 200:
            return response.json().get("access_token")

        # 失败时输出详细信息，方便排查
        self.logger.log_error(f"获取 token 失败: {response.status_code}, {response.text}")
        return None
    
    def parse_search_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        """
        解析 *search-game* 接口返回，提取游戏及其会社信息。

        参数
        ----
        response : requests.Response
            月幕 *search-game* API 响应对象。

        Returns
        -------
        list[dict]
            解析后的结果列表，每个元素均包含：
            - ``name``：日文 / 英文原名
            - ``chineseName``：中文名(可能为空)
            - ``ym_id``：月幕游戏 ID
            - ``score``：月幕算法打分 (匹配度)
            - ``orgId`` / ``orgName`` / ``orgWebsite`` / ``orgDescription``：会社信息
        """
        try:
            response_data = response.json()
            # 将API响应保存到日志文件
            self.logger.log_api_response("search_game", response_data)
            results = response_data.get("data", {}).get("result", [])
        except Exception as exc:
            self.logger.log_error(f"解析 response 失败：{exc}")
            return []

        parsed: List[Dict[str, Any]] = []
        for item in results:
            # 1️⃣ 解析匹配分数，默认 0.0
            try:
                score = float(item.get("score", 0))
            except (ValueError, TypeError):
                score = 0.0

            # 2️⃣ 解析会社信息，API 有时嵌套在 ``org``，有时散落在顶层
            org_info = item.get("org", {}) or {
                "id": item.get("orgId", ""),
                "name": item.get("orgName", ""),
                "website": item.get("orgWebsite", ""),
                "description": item.get("orgDescription", "")
            }

            # 移除会社信息的INFO输出，只记录到日志文件
            if org_info:
                self.logger.log_api_response("org_info_found", {
                    "org_name": org_info.get('name', ''),
                    "org_id": org_info.get('id', '')
                })

            parsed.append({
                "name": item.get("name", ""),
                "chineseName": item.get("chineseName", ""),
                "ym_id": item.get("id", ""),
                "score": round(score, 4),
                "orgId": org_info.get("id", ""),
                "orgName": org_info.get("name", ""),
                "orgWebsite": org_info.get("website", ""),
                "orgDescription": org_info.get("description", "")
            })

        return parsed
    
    def search_ym_top_matches(
        self,
        keyword: str,
        top_k: int = 3,
        threshold: float = 0.8
    ) -> List[Dict[str, Any]]:
        """
        根据 *keyword* 在网站搜索并返回最相关的前 ``top_k`` 条结果。

        特性：
        --------
        - **Token 自动刷新**：若接口返回 401 则重新获取一次 token，最多重试 4 次。
        - **阈值过滤**：若最高得分 >= ``threshold`` 则只返回 1 条最优匹配。

        参数
        ----
        keyword : str
            待搜索的产品名称。
        top_k : int, default=3
            未触发阈值过滤时，返回结果数。
        threshold : float, default=0.8
            最高得分超过该阈值时，视为高度一致，仅返回首条。

        Returns
        -------
        list[dict]
            解析后的匹配结果列表 (可能为空)。
        """

        def _make_request(token: str) -> requests.Response:
            """内部封装：携带 token 调用 search-game 接口。"""
            url = f"{self.base_url}/open/archive/search-game"
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
            return requests.get(url, params=params, headers=headers, timeout=10)

        # --- 主流程：最多尝试 4 次 --------------------------------------------
        for attempt in range(4):
            token = self.token_ref["value"]
            response = _make_request(token)

            # 1. 请求成功 -> 解析
            if response.status_code == 200:
                matches = self.parse_search_response(response)
                matches = sorted(matches, key=lambda x: x["score"], reverse=True)

                # 阈值过滤逻辑
                if matches and matches[0]["score"] >= threshold:
                    return matches[:1]
                return matches[:top_k]

            # 2. Token 失效 -> 刷新后重试
            elif response.status_code == 401:
                self.logger.log_important("Token 失效，正在重新获取…")
                new_token = self.get_access_token()
                if new_token:
                    self.token_ref["value"] = new_token
                    continue
                self.logger.log_error("重新获取 token 失败")
                return []

            # 3. 403/404/410 致命错误 -> 立即报错并终止
            elif response.status_code in (403, 404, 410):
                self.logger.log_error(f"接口返回致命错误: {response.status_code}, {response.text}")
                raise RuntimeError(f"接口返回致命错误: {response.status_code}")

            # 4. 其它错误 -> 直接返回空
            else:
                self.logger.log_error(f"搜索失败: {response.status_code}, {response.text}")
                return []

        # 超出重试次数
        return []
    
    def get_organization_details(self, org_id: str) -> Optional[Dict[str, Any]]:
        """
        根据 ``org_id`` 向月幕查询会社详细资料。

        返回的字段包括：名称、中文名、官网、简介、成立日期等。
        若调用失败或字段缺失，则返回 ``None``。
        """
        url = f"{self.base_url}/open/archive"
        params = {"orgId": org_id}
        headers = {
            "Authorization": f"Bearer {self.token_ref['value']}",
            "Accept": "application/json",
            "version": "1"
        }

        try:
            # 移除控制台输出，只记录到日志文件
            self.logger.log_api_response("org_details_request", {"org_id": org_id})
            response = requests.get(url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                # 记录公司信息API响应
                self.logger.log_api_response(f"org_details_{org_id}", data)
                org_data = data.get("data", {}).get("org", {})
                if not org_data:
                    self.logger.log_info("API 响应中未找到公司信息")
                    return None

                # 按优先级提取官网地址，fallback 使用第一个
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

            if response.status_code == 401:  # token 失效, 交由外层处理
                self.logger.log_info("公司信息获取时 token 失效")
                return None

            # 新增：403/404/410 致命错误
            if response.status_code in (403, 404, 410):
                self.logger.log_error(f"接口返回致命错误: {response.status_code}, {response.text}")
                raise RuntimeError(f"接口返回致命错误: {response.status_code}")

            self.logger.log_error(f"获取会社信息失败: {response.status_code}")
            return None

        except Exception as exc:
            self.logger.log_error(f"获取会社信息时发生错误: {exc}")
            return None
    
    def initialize_token(self) -> bool:
        """初始化token"""
        self.token_ref["value"] = self.get_access_token()
        return self.token_ref["value"] is not None