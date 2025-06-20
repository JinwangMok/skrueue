import numpy as np
from typing import List, Dict, Optional
import uuid
from datetime import datetime

from config.simulation_config import SimulationConfig, JobTypeConfig, TimePatternConfig
from simulator.cluster_simulator import Job

class JobGenerator:
    """시간대별 패턴을 반영한 작업 생성기"""
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.job_counter = 0
        
    def generate_job(self, current_time: float) -> Job:
        """현재 시간에 맞는 작업 생성"""
        # 시간대 확인 (0-24시 기준)
        hour = (current_time / 3600) % 24
        
        # 해당 시간대의 패턴 찾기
        pattern = self._get_time_pattern(hour)
        
        # 작업 유형 선택
        job_type = self._select_job_type(pattern.job_distribution)
        job_config = self.config.job_types[job_type]
        
        # 작업 속성 생성
        job = self._create_job(job_config, current_time)
        
        return job
    
    def generate_batch(self, current_time: float, count: int) -> List[Job]:
        """여러 작업을 한번에 생성"""
        return [self.generate_job(current_time) for _ in range(count)]
    
    def _get_time_pattern(self, hour: float) -> TimePatternConfig:
        """현재 시간에 해당하는 패턴 반환"""
        for pattern in self.config.time_patterns:
            start, end = pattern.time_range
            
            # 자정을 넘는 경우 처리
            if start > end:
                if hour >= start or hour < end:
                    return pattern
            else:
                if start <= hour < end:
                    return pattern
        
        # 기본 패턴 (첫 번째 패턴 사용)
        return self.config.time_patterns[0]
    
    def _select_job_type(self, distribution: Dict[str, float]) -> str:
        """확률 분포에 따라 작업 유형 선택"""
        job_types = list(distribution.keys())
        probabilities = list(distribution.values())
        
        # 확률 정규화
        total = sum(probabilities)
        probabilities = [p/total for p in probabilities]
        
        return np.random.choice(job_types, p=probabilities)
    
    def _create_job(self, job_config: JobTypeConfig, current_time: float) -> Job:
        """작업 구성에 따라 Job 인스턴스 생성"""
        self.job_counter += 1
        
        # 범위 내에서 랜덤 값 선택
        cpu_request = np.random.randint(job_config.cpu_range[0], job_config.cpu_range[1])
        memory_request = np.random.randint(job_config.memory_range[0], job_config.memory_range[1])
        duration = np.random.randint(job_config.duration_range[0], job_config.duration_range[1])
        priority = np.random.randint(job_config.priority_range[0], job_config.priority_range[1])
        
        # 노이즈 추가 (선택적)
        if self.config.noise_factor > 0:
            cpu_request = int(cpu_request * (1 + np.random.normal(0, self.config.noise_factor)))
            memory_request = int(memory_request * (1 + np.random.normal(0, self.config.noise_factor)))
            duration = int(duration * (1 + np.random.normal(0, self.config.noise_factor)))
            
            # 값 범위 제한
            cpu_request = max(100, cpu_request)
            memory_request = max(256, memory_request)
            duration = max(10, duration)
        
        return Job(
            id=f"job-{self.job_counter}-{uuid.uuid4().hex[:8]}",
            job_type=job_config.name,
            cpu_request=cpu_request,
            memory_request=memory_request,
            duration=duration,
            priority=priority,
            submit_time=current_time
        )
    
    def generate_workload_pattern(self, duration: int, avg_arrival_rate: float = 0.5) -> List[tuple[float, Job]]:
        """전체 워크로드 패턴 생성"""
        workload = []
        current_time = 0.0
        
        while current_time < duration:
            # 포아송 분포를 사용한 도착 간격
            interval = np.random.exponential(1.0 / avg_arrival_rate)
            current_time += interval
            
            if current_time >= duration:
                break
            
            # 시간대별 도착률 조정
            hour = (current_time / 3600) % 24
            
            # 업무 시간에는 도착률 증가
            if 9 <= hour <= 18:
                adjusted_rate = avg_arrival_rate * 1.5
            # 심야 시간에는 도착률 감소
            elif 0 <= hour < 6:
                adjusted_rate = avg_arrival_rate * 0.3
            else:
                adjusted_rate = avg_arrival_rate
            
            # 버스트 패턴 추가 (5% 확률)
            if np.random.random() < 0.05:
                burst_size = np.random.randint(3, 10)
                for _ in range(burst_size):
                    job = self.generate_job(current_time)
                    workload.append((current_time, job))
                    current_time += np.random.uniform(0.1, 1.0)
            else:
                job = self.generate_job(current_time)
                workload.append((current_time, job))
        
        return workload