"""워크로드 생성 전략"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import random
from datetime import datetime, timedelta

from .templates import JobTemplate, JobTemplateFactory


class WorkloadStrategy(ABC):
    """워크로드 생성 전략 인터페이스"""
    
    @abstractmethod
    def generate_schedule(self, duration_hours: int) -> List[Dict[str, Any]]:
        """워크로드 스케줄 생성"""
        pass


class UniformStrategy(WorkloadStrategy):
    """균등 분포 전략"""
    
    def __init__(self, submission_interval: int = 60, templates: Dict[str, JobTemplate] = None):
        self.submission_interval = submission_interval
        self.templates = templates or JobTemplateFactory.get_default_templates()
    
    def generate_schedule(self, duration_hours: int) -> List[Dict[str, Any]]:
        schedule = []
        start_time = datetime.now()
        
        for minutes in range(0, duration_hours * 60, self.submission_interval // 60):
            submit_time = start_time + timedelta(minutes=minutes)
            template_name = random.choice(list(self.templates.keys()))
            
            schedule.append({
                'submit_time': submit_time,
                'template_name': template_name,
                'template': self.templates[template_name]
            })
        
        return schedule


class RealisticStrategy(WorkloadStrategy):
    """현실적 패턴 전략 (시간대별 다른 워크로드)"""
    
    def __init__(self, hourly_patterns: Dict[str, Dict] = None, templates: Dict[str, JobTemplate] = None):
        self.templates = templates or JobTemplateFactory.get_default_templates()
        self.hourly_patterns = hourly_patterns or self._get_default_patterns()
    
    def _get_default_patterns(self) -> Dict[str, Dict]:
        """기본 시간대별 패턴"""
        return {
            "night": {  # 0-6시
                "hours": list(range(0, 6)),
                "job_rate": 2,  # 시간당 작업 수
                "template_weights": {
                    "etl-pipeline": 0.4,
                    "memory-intensive": 0.3,
                    "io-intensive": 0.2,
                    "quick-task": 0.1
                }
            },
            "morning": {  # 6-9시
                "hours": list(range(6, 9)),
                "job_rate": 3,
                "template_weights": {
                    "quick-task": 0.3,
                    "cpu-intensive": 0.3,
                    "io-intensive": 0.2,
                    "etl-pipeline": 0.2
                }
            },
            "business": {  # 9-18시
                "hours": list(range(9, 18)),
                "job_rate": 5,
                "template_weights": {
                    "ml-training": 0.3,
                    "cpu-intensive": 0.25,
                    "memory-intensive": 0.2,
                    "etl-pipeline": 0.15,
                    "quick-task": 0.1
                }
            },
            "evening": {  # 18-24시
                "hours": list(range(18, 24)),
                "job_rate": 4,
                "template_weights": {
                    "etl-pipeline": 0.35,
                    "ml-training": 0.25,
                    "memory-intensive": 0.2,
                    "io-intensive": 0.15,
                    "quick-task": 0.05
                }
            }
        }
    
    def generate_schedule(self, duration_hours: int) -> List[Dict[str, Any]]:
        schedule = []
        start_time = datetime.now()
        
        for hour in range(duration_hours):
            current_hour = (start_time.hour + hour) % 24
            
            # 해당 시간대 패턴 찾기
            pattern = None
            for pattern_name, pattern_config in self.hourly_patterns.items():
                if current_hour in pattern_config["hours"]:
                    pattern = pattern_config
                    break
            
            if not pattern:
                continue
            
            # 해당 시간에 생성할 작업 수
            num_jobs = max(1, int(random.gauss(pattern["job_rate"], 1)))
            
            for _ in range(num_jobs):
                # 가중치 기반 템플릿 선택
                template_weights = pattern["template_weights"]
                templates = list(template_weights.keys())
                weights = list(template_weights.values())
                
                # 템플릿이 실제로 존재하는지 확인
                valid_templates = [t for t in templates if t in self.templates]
                if not valid_templates:
                    continue
                
                selected_template = random.choices(
                    valid_templates,
                    weights=[template_weights[t] for t in valid_templates]
                )[0]
                
                # 제출 시간 (해당 시간 내 랜덤)
                submit_time = start_time + timedelta(
                    hours=hour,
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                schedule.append({
                    'submit_time': submit_time,
                    'template_name': selected_template,
                    'template': self.templates[selected_template],
                    'hour': current_hour
                })
        
        # 시간순 정렬
        schedule.sort(key=lambda x: x['submit_time'])
        return schedule


class BurstStrategy(WorkloadStrategy):
    """버스트 패턴 전략"""
    
    def __init__(self, burst_size: int = 20, burst_interval_hours: int = 4, 
                 templates: Dict[str, JobTemplate] = None):
        self.burst_size = burst_size
        self.burst_interval_hours = burst_interval_hours
        self.templates = templates or JobTemplateFactory.get_default_templates()
    
    def generate_schedule(self, duration_hours: int) -> List[Dict[str, Any]]:
        schedule = []
        start_time = datetime.now()
        
        for burst_time in range(0, duration_hours, self.burst_interval_hours):
            # 버스트 시작 시간
            burst_start = start_time + timedelta(hours=burst_time)
            
            # 버스트 크기만큼 작업 생성
            for i in range(self.burst_size):
                submit_time = burst_start + timedelta(seconds=i * 5)  # 5초 간격
                template_name = random.choice(list(self.templates.keys()))
                
                schedule.append({
                    'submit_time': submit_time,
                    'template_name': template_name,
                    'template': self.templates[template_name],
                    'burst_id': burst_time // self.burst_interval_hours
                })
        
        return schedule