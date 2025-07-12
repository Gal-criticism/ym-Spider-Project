"""
拥塞控制模块
实现类似TCP的拥塞控制算法，自适应调整并发窗口
"""

import asyncio
import time
from typing import Optional
from dataclasses import dataclass


@dataclass
class CongestionStats:
    """拥塞控制统计信息"""
    success_count: int = 0
    failure_count: int = 0
    total_requests: int = 0
    last_adjustment_time: float = 0.0
    current_cwnd: int = 5
    max_cwnd: int = 50
    min_cwnd: int = 1


class CongestionController:
    """TCP风格的拥塞控制器"""
    
    def __init__(self, 
                 initial_cwnd: int = 5,
                 max_cwnd: int = 50,
                 min_cwnd: int = 1,
                 adjustment_interval: float = 1.0):
        """
        初始化拥塞控制器
        
        Args:
            initial_cwnd: 初始并发窗口大小
            max_cwnd: 最大并发窗口大小
            min_cwnd: 最小并发窗口大小
            adjustment_interval: 调整间隔（秒）
        """
        self.stats = CongestionStats(
            current_cwnd=initial_cwnd,
            max_cwnd=max_cwnd,
            min_cwnd=min_cwnd
        )
        self.adjustment_interval = adjustment_interval
        self._lock = asyncio.Lock()
    
    async def get_current_window(self) -> int:
        """获取当前并发窗口大小"""
        async with self._lock:
            return self.stats.current_cwnd
    
    async def record_success(self) -> None:
        """记录成功请求，增加窗口大小"""
        async with self._lock:
            self.stats.success_count += 1
            self.stats.total_requests += 1
            await self._adjust_window()
    
    async def record_failure(self) -> None:
        """记录失败请求，减少窗口大小"""
        async with self._lock:
            self.stats.failure_count += 1
            self.stats.total_requests += 1
            await self._adjust_window()
    
    async def _adjust_window(self) -> None:
        """根据成功/失败率调整窗口大小"""
        current_time = time.time()
        
        # 检查是否需要调整窗口
        if current_time - self.stats.last_adjustment_time < self.adjustment_interval:
            return
        
        self.stats.last_adjustment_time = current_time
        
        # 计算成功率
        if self.stats.total_requests == 0:
            return
        
        success_rate = self.stats.success_count / self.stats.total_requests
        
        # TCP风格的拥塞控制算法
        if success_rate >= 0.8:  # 高成功率，缓慢增长
            self.stats.current_cwnd = min(
                self.stats.max_cwnd,
                self.stats.current_cwnd + 1
            )
        elif success_rate >= 0.6:  # 中等成功率，保持当前窗口
            pass
        else:  # 低成功率，快速退避
            self.stats.current_cwnd = max(
                self.stats.min_cwnd,
                self.stats.current_cwnd // 2
            )
        
        # 重置计数器
        self.stats.success_count = 0
        self.stats.failure_count = 0
        self.stats.total_requests = 0
    
    def get_stats(self) -> dict:
        """获取当前统计信息"""
        return {
            "current_cwnd": self.stats.current_cwnd,
            "max_cwnd": self.stats.max_cwnd,
            "min_cwnd": self.stats.min_cwnd,
            "total_requests": self.stats.total_requests,
            "success_rate": (self.stats.success_count / self.stats.total_requests 
                           if self.stats.total_requests > 0 else 0)
        }
    
    async def reset(self) -> None:
        """重置拥塞控制器状态"""
        async with self._lock:
            self.stats = CongestionStats(
                current_cwnd=self.stats.current_cwnd,
                max_cwnd=self.stats.max_cwnd,
                min_cwnd=self.stats.min_cwnd
            ) 