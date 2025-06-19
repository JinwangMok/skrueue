"""로깅 유틸리티"""
import logging
import os
from datetime import datetime


def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """통합 로거 생성"""
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 이미 핸들러가 있으면 재사용
    if logger.handlers:
        return logger
    
    # 파일 핸들러
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f'{name}_{timestamp}.log')
    )
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포맷터
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger