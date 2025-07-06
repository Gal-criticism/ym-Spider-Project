"""
异步缓冲区管理器
实现生产者-消费者模式的数据缓冲写入，防止I/O阻塞爬虫进程
"""

import asyncio
import csv
import json
import os
import time
import pandas as pd
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from pathlib import Path
from src.data.data_processor import DataProcessor


@dataclass
class BufferConfig:
    """缓冲区配置"""
    max_size: int = 1000  # 最大缓冲区大小
    flush_interval: float = 5.0  # 刷新间隔（秒）
    batch_size: int = 50  # 批量写入大小
    backup_interval: int = 100  # 备份间隔（条记录）


class BufferManager:
    """异步缓冲区管理器"""
    
    def __init__(self, 
                 output_file: str,
                 buffer_config: Optional[BufferConfig] = None,
                 data_processor: Optional[Callable] = None):
        """
        初始化缓冲区管理器
        
        Args:
            output_file: 输出文件路径
            buffer_config: 缓冲区配置
            data_processor: 数据处理函数
        """
        self.output_file = output_file
        self.config = buffer_config or BufferConfig()
        self.data_processor = data_processor
        
        # 异步队列
        self.data_queue = asyncio.Queue(maxsize=self.config.max_size)
        self.is_running = False
        self.consumer_task = None
        
        # 统计信息
        self.total_processed = 0
        self.total_written = 0
        self.last_flush_time = time.time()
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # 初始化文件
        self._init_output_file()
    
    def _init_output_file(self) -> None:
        """初始化输出文件，复用同步DataProcessor的初始化逻辑"""
        data_processor = DataProcessor()
        data_processor.init_excel(self.output_file)
    
    async def start(self) -> None:
        """启动缓冲区管理器"""
        if self.is_running:
            return
        
        self.is_running = True
        self.consumer_task = asyncio.create_task(self._consumer_loop())
        print(f"缓冲区管理器已启动，输出文件: {self.output_file}")
    
    async def stop(self) -> None:
        """停止缓冲区管理器"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # 等待消费者任务完成
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        
        # 刷新剩余数据
        await self._flush_remaining_data()
        print(f"缓冲区管理器已停止，共处理 {self.total_processed} 条记录")
    
    async def put_data(self, data: Dict[str, Any]) -> None:
        """
        将数据放入缓冲区
        
        Args:
            data: 要存储的数据字典
        """
        if not self.is_running:
            raise RuntimeError("缓冲区管理器未启动")
        
        # 处理数据（如果有自定义处理器）
        if self.data_processor:
            data = self.data_processor(data)
        
        await self.data_queue.put(data)
        self.total_processed += 1
    
    async def _consumer_loop(self) -> None:
        """消费者循环，从队列中取出数据并写入文件"""
        buffer = []
        last_flush = time.time()
        
        while self.is_running:
            try:
                # 尝试从队列中获取数据，设置超时以便定期刷新
                try:
                    data = await asyncio.wait_for(
                        self.data_queue.get(), 
                        timeout=1.0
                    )
                    buffer.append(data)
                except asyncio.TimeoutError:
                    pass
                
                current_time = time.time()
                
                # 检查是否需要刷新缓冲区
                should_flush = (
                    len(buffer) >= self.config.batch_size or
                    (current_time - last_flush) >= self.config.flush_interval
                )
                
                if should_flush and buffer:
                    await self._write_batch(buffer)
                    self.total_written += len(buffer)
                    buffer.clear()
                    last_flush = current_time
                
                # 短暂休眠避免CPU占用过高
                await asyncio.sleep(0.01)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"消费者循环错误: {e}")
                await asyncio.sleep(1.0)
    
    async def _write_batch(self, data_batch: List[Dict[str, Any]]) -> None:
        """
        批量写入数据到文件，完全复用同步DataProcessor的写入逻辑
        """
        try:
            # 直接使用同步的DataProcessor，确保写入逻辑完全一致
            data_processor = DataProcessor()
            
            # 使用同步的append_to_excel方法，支持所有兜底机制
            data_processor.append_to_excel(data_batch, self.output_file)
            
        except Exception as e:
            print(f"写入文件错误: {e}")
            # 如果写入失败，将数据重新放回队列
            for data in data_batch:
                await self.data_queue.put(data)
    
    async def _flush_remaining_data(self) -> None:
        """刷新剩余数据"""
        remaining_data = []
        while not self.data_queue.empty():
            try:
                data = self.data_queue.get_nowait()
                remaining_data.append(data)
            except asyncio.QueueEmpty:
                break
        
        if remaining_data:
            await self._write_batch(remaining_data)
            self.total_written += len(remaining_data)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓冲区统计信息"""
        return {
            "queue_size": self.data_queue.qsize(),
            "total_processed": self.total_processed,
            "total_written": self.total_written,
            "is_running": self.is_running,
            "output_file": self.output_file
        }
    
    async def create_backup(self) -> str:
        """创建数据备份"""
        if not os.path.exists(self.output_file):
            return ""
        
        timestamp = int(time.time())
        backup_file = f"{self.output_file}.backup_{timestamp}"
        
        try:
            import shutil
            shutil.copy2(self.output_file, backup_file)
            print(f"已创建备份文件: {backup_file}")
            return backup_file
        except Exception as e:
            print(f"创建备份失败: {e}")
            return "" 