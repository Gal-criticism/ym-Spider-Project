import asyncio
import pandas as pd
import os
import time
from typing import List, Dict, Any, Optional
from collections import deque
from dataclasses import dataclass
from enum import Enum




class WriteStrategy(Enum):
    """写入策略枚举"""
    TIMER = "timer"      # 定时写入
    SIZE = "size"        # 缓冲区满时写入
    HYBRID = "hybrid"    # 混合策略


@dataclass
class BufferConfig:
    """缓冲配置"""
    buffer_size: int = 1000          # 缓冲区大小
    write_interval: float = 5.0      # 写入间隔（秒）
    strategy: WriteStrategy = WriteStrategy.HYBRID  # 写入策略
    auto_flush: bool = True          # 自动刷新
    backup_on_error: bool = True     # 错误时备份


class BufferManager:
    """异步缓冲池管理器，用于高效批量写入文件"""
    
    def __init__(self, config: Optional[BufferConfig] = None):
        """
        初始化缓冲池管理器
        
        Args:
            config: 缓冲配置
        """
        self.config = config or BufferConfig()
        
        # 数据缓冲区
        self.buffers: Dict[str, deque] = {}
        self.buffer_locks: Dict[str, asyncio.Lock] = {}
        
        # 写入任务
        self.write_tasks: Dict[str, asyncio.Task] = {}
        self.running = False
        
        # 统计信息
        self.total_writes = 0
        self.total_items = 0
        self.last_write_time = time.time()
        
        # 文件路径映射
        self.file_paths: Dict[str, str] = {}
        

        
    async def start(self):
        """启动缓冲池管理器"""
        self.running = True
    
    async def stop(self):
        """停止缓冲池管理器，确保所有数据写盘"""
        self.running = False
        
        # 等待所有写入任务完成
        if self.write_tasks:
            await asyncio.gather(*self.write_tasks.values(), return_exceptions=True)
        
        # 强制刷新所有缓冲区
        for file_id in list(self.buffers.keys()):
            await self._flush_buffer(file_id)
    
    def register_file(self, file_id: str, file_path: str, columns: List[str]):
        """
        注册文件，初始化缓冲区
        
        Args:
            file_id: 文件标识符
            file_path: 文件路径
            columns: 列名列表
        """
        self.file_paths[file_id] = file_path
        self.buffers[file_id] = deque(maxlen=self.config.buffer_size)
        self.buffer_locks[file_id] = asyncio.Lock()
        
        # 初始化文件
        self._init_file(file_path, columns)
        
        # 启动定时写入任务
        if self.config.strategy in [WriteStrategy.TIMER, WriteStrategy.HYBRID]:
            self.write_tasks[file_id] = asyncio.create_task(
                self._periodic_write(file_id)
            )
    
    def _init_file(self, file_path: str, columns: List[str]):
        """初始化文件，如果不存在则创建"""
        if not os.path.exists(file_path):
            df = pd.DataFrame(columns=columns)
            df.to_excel(file_path, index=False)
    
    async def put_data(self, file_id: str, data: Dict[str, Any]) -> bool:
        """
        异步推送数据到缓冲区
        
        Args:
            file_id: 文件标识符
            data: 要写入的数据
            
        Returns:
            bool: 是否成功添加到缓冲区
        """
        if file_id not in self.buffers:
            raise ValueError(f"未注册的文件ID: {file_id}")
        
        async with self.buffer_locks[file_id]:
            buffer = self.buffers[file_id]
            buffer.append(data)
            self.total_items += 1
            
            # 检查是否需要立即写入
            if (self.config.strategy == WriteStrategy.SIZE and 
                len(buffer) >= self.config.buffer_size):
                await self._flush_buffer(file_id)
            
            return True
    
    async def put_batch_data(self, file_id: str, data_list: List[Dict[str, Any]]) -> bool:
        """
        批量推送数据到缓冲区
        
        Args:
            file_id: 文件标识符
            data_list: 数据列表
            
        Returns:
            bool: 是否成功添加
        """
        if file_id not in self.buffers:
            raise ValueError(f"未注册的文件ID: {file_id}")
        
        async with self.buffer_locks[file_id]:
            buffer = self.buffers[file_id]
            
            for data in data_list:
                buffer.append(data)
                self.total_items += 1
            
            # 检查是否需要立即写入
            if (self.config.strategy == WriteStrategy.SIZE and 
                len(buffer) >= self.config.buffer_size):
                await self._flush_buffer(file_id)
            
            return True
    
    async def _flush_buffer(self, file_id: str) -> bool:
        """
        刷新缓冲区，将数据写入文件
        
        Args:
            file_id: 文件标识符
            
        Returns:
            bool: 是否成功写入
        """
        if file_id not in self.buffers:
            return False
        
        async with self.buffer_locks[file_id]:
            buffer = self.buffers[file_id]
            
            if not buffer:
                return True
            
            file_path = self.file_paths[file_id]
            data_to_write = list(buffer)
            buffer.clear()
            
            try:
                # 读取现有数据
                if os.path.exists(file_path):
                    try:
                        df_existing = pd.read_excel(file_path)
                    except Exception:
                        # 文件损坏，创建新的
                        df_existing = pd.DataFrame()
                else:
                    df_existing = pd.DataFrame()
                
                # 创建新数据DataFrame
                df_new = pd.DataFrame(data_to_write)
                
                # 合并数据
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                
                # 写入文件
                df_combined.to_excel(file_path, index=False)
                
                self.total_writes += 1
                self.last_write_time = time.time()
                
                return True
                
            except Exception as e:
                # 错误备份
                if self.config.backup_on_error:
                    backup_path = f"{file_path}.backup_{int(time.time())}"
                    try:
                        pd.DataFrame(data_to_write).to_excel(backup_path, index=False)
                    except Exception:
                        pass
                
                return False
    
    async def _periodic_write(self, file_id: str):
        """定时写入任务"""
        while self.running:
            try:
                await asyncio.sleep(self.config.write_interval)
                if self.running:
                    await self._flush_buffer(file_id)
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    async def force_flush(self, file_id: Optional[str] = None):
        """
        强制刷新缓冲区
        
        Args:
            file_id: 文件标识符，如果为None则刷新所有文件
        """
        if file_id:
            await self._flush_buffer(file_id)
        else:
            for fid in list(self.buffers.keys()):
                await self._flush_buffer(fid)
    
    def get_buffer_status(self) -> Dict[str, Any]:
        """获取缓冲区状态"""
        status = {
            "total_writes": self.total_writes,
            "total_items": self.total_items,
            "last_write_time": self.last_write_time,
            "buffers": {}
        }
        
        for file_id, buffer in self.buffers.items():
            status["buffers"][file_id] = {
                "size": len(buffer),
                "max_size": buffer.maxlen,
                "file_path": self.file_paths.get(file_id, "unknown")
            }
        
        return status
    
    def get_buffer_size(self, file_id: str) -> int:
        """获取指定缓冲区的当前大小"""
        if file_id in self.buffers:
            return len(self.buffers[file_id])
        return 0
    
    def is_buffer_full(self, file_id: str) -> bool:
        """检查缓冲区是否已满"""
        if file_id in self.buffers:
            buffer = self.buffers[file_id]
            return len(buffer) >= buffer.maxlen
        return False 