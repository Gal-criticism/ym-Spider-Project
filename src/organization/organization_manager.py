from typing import Dict, Any, Optional


class OrganizationManager:
    """会社信息管理器，专门处理会社信息的查询和管理"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.processed_orgs = {}
    
    def get_organization_details(self, org_id: str) -> Optional[Dict[str, Any]]:
        """获取公司详细信息"""
        return self.api_client.get_organization_details(org_id)
    
    def should_retry_org_query(self, org_id: str, org_info: Dict[str, Any]) -> bool:
        """判断是否需要重试查询公司信息"""
        if org_id not in self.processed_orgs:
            return True
        
        existing = self.processed_orgs[org_id]["info"]
        # 如果信息不完整，需要重试
        if not existing.get("website") or not existing.get("description"):
            return True
        
        return False
    
    def update_org_info(self, org_id: str, org_info: Dict[str, Any]) -> None:
        """更新公司信息"""
        if org_id not in self.processed_orgs:
            self.processed_orgs[org_id] = {"info": {}, "retry_count": 0}
        
        self.processed_orgs[org_id]["info"] = org_info
    
    def increment_retry_count(self, org_id: str) -> None:
        """增加重试次数"""
        if org_id in self.processed_orgs:
            self.processed_orgs[org_id]["retry_count"] += 1
    
    def can_retry(self, org_id: str, max_retries: int = 3) -> bool:
        """检查是否可以重试"""
        if org_id not in self.processed_orgs:
            return True
        return self.processed_orgs[org_id]["retry_count"] < max_retries
    
    def get_org_info(self, org_id: str) -> Optional[Dict[str, Any]]:
        """获取已缓存的会社信息"""
        if org_id in self.processed_orgs:
            return self.processed_orgs[org_id]["info"]
        return None 