import os
import json
import datetime
from typing import Any, Dict


class Logger:
    """日志管理工具类"""
    
    def __init__(self, silent_mode: bool = True):
        self.logs_dir = "logs"
        self.api_log_file = None
        self.silent_mode = silent_mode  # 静默模式，不输出INFO信息到控制台
        self._ensure_logs_dir()
        self._init_api_log_file()
    
    def _ensure_logs_dir(self):
        """确保日志目录存在"""
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
    
    def _init_api_log_file(self):
        """初始化API日志文件"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.api_log_file = os.path.join(self.logs_dir, f"api_responses_{timestamp}.log")
    
    def log_api_response(self, keyword: str, response_data: Dict[str, Any]):
        """记录API响应信息"""
        if not self.api_log_file:
            return
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            with open(self.api_log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"时间: {timestamp}\n")
                f.write(f"关键词: {keyword}\n")
                f.write(f"API响应:\n")
                f.write(json.dumps(response_data, indent=2, ensure_ascii=False))
                f.write(f"\n{'='*80}\n")
        except Exception as e:
            print(f"写入API日志失败: {e}")
    
    def log_info(self, message: str):
        """记录信息日志"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[INFO] {timestamp} - {message}"
        
        # 在静默模式下，INFO信息只写入日志文件，不输出到控制台
        if not self.silent_mode:
            print(log_message)
    
    def log_error(self, message: str):
        """记录错误日志"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[ERROR] {timestamp} - {message}"
        print(log_message)  # 错误信息总是输出到控制台
    
    def log_warning(self, message: str):
        """记录警告日志"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[WARNING] {timestamp} - {message}"
        print(log_message)  # 警告信息总是输出到控制台
    
    def log_important(self, message: str):
        """记录重要信息（总是输出到控制台）"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[IMPORTANT] {timestamp} - {message}"
        print(log_message) 