from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import yaml
import json

@dataclass
class NodeConfig:
    """노드 하드웨어 스펙 정의"""
    cpu_cores: int
    cpu_ghz: float
    memory_gb: int
    gpu_count: int = 0
    node_type: str = "worker"
    
    @property
    def total_cpu_capacity(self):
        """총 CPU 용량 (밀리코어)"""
        return self.cpu_cores * 1000
    
    @property
    def total_memory_capacity(self):
        """총 메모리 용량 (MB)"""
        return self.memory_gb * 1024

@dataclass
class JobTypeConfig:
    """작업 유형별 특성 정의"""
    name: str
    cpu_range: Tuple[int, int]  # min, max millicores
    memory_range: Tuple[int, int]  # min, max MB
    duration_range: Tuple[int, int]  # min, max seconds
    priority_range: Tuple[int, int]
    failure_rate: float = 0.05
    
@dataclass
class TimePatternConfig:
    """시간대별 작업 분포 패턴"""
    time_range: Tuple[int, int]  # 시작시간, 종료시간 (0-24)
    job_distribution: Dict[str, float]  # job_type: probability

@dataclass
class SimulationConfig:
    """전체 시뮬레이션 설정"""
    # 클러스터 구성
    nodes: List[NodeConfig] = field(default_factory=list)
    
    # 작업 유형 정의
    job_types: Dict[str, JobTypeConfig] = field(default_factory=dict)
    
    # 시간대별 패턴
    time_patterns: List[TimePatternConfig] = field(default_factory=list)
    
    # 시뮬레이션 파라미터
    max_queue_size: int = 50  # 동적 큐 크기 최대값
    min_queue_size: int = 5   # 동적 큐 크기 최소값
    simulation_duration: int = 86400  # 24시간 (초)
    tick_interval: int = 10  # 시뮬레이션 틱 간격 (초)
    
    # RL 파라미터
    state_dim: int = 67  # 기본 상태 차원
    dynamic_state: bool = True  # 동적 상태 공간 사용 여부
    
    # 데이터 증강 파라미터
    noise_factor: float = 0.1  # 노이즈 추가 비율
    anomaly_rate: float = 0.01  # 이상치 발생 비율
    
    # 보상 가중치
    reward_weights: Dict[str, float] = field(default_factory=lambda: {
        "throughput": 0.4,
        "utilization": 0.3,
        "wait_penalty": 0.2,
        "failure_penalty": 0.1
    })
    
    @classmethod
    def from_yaml(cls, path: str):
        """YAML 파일에서 설정 로드"""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def to_yaml(self, path: str):
        """설정을 YAML 파일로 저장"""
        import yaml
        
        def convert_to_dict(obj):
            """데이터클래스를 딕셔너리로 변환"""
            if hasattr(obj, '__dataclass_fields__'):
                return {k: convert_to_dict(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, list):
                return [convert_to_dict(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: convert_to_dict(v) for k, v in obj.items()}
            elif isinstance(obj, tuple):
                return list(obj)
            else:
                return obj
        
        data = convert_to_dict(self)
        
        with open(path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

# 기본 설정 생성 함수
def create_default_config():
    """현실적인 기본 설정 생성"""
    config = SimulationConfig()
    
    # 다양한 노드 타입 정의
    config.nodes = [
        # 고성능 노드 (4개)
        *[NodeConfig(cpu_cores=32, cpu_ghz=3.5, memory_gb=128, gpu_count=2) for _ in range(4)],
        # 중간 성능 노드 (8개)
        *[NodeConfig(cpu_cores=16, cpu_ghz=2.8, memory_gb=64) for _ in range(8)],
        # 저성능 노드 (4개)
        *[NodeConfig(cpu_cores=8, cpu_ghz=2.4, memory_gb=32) for _ in range(4)],
    ]
    
    # 작업 유형 정의
    config.job_types = {
        "batch": JobTypeConfig(
            name="batch",
            cpu_range=(500, 4000),
            memory_range=(1024, 8192),
            duration_range=(60, 3600),
            priority_range=(1, 5)
        ),
        "ml_training": JobTypeConfig(
            name="ml_training",
            cpu_range=(2000, 16000),
            memory_range=(4096, 32768),
            duration_range=(1800, 14400),
            priority_range=(3, 8),
            failure_rate=0.08
        ),
        "web_service": JobTypeConfig(
            name="web_service",
            cpu_range=(100, 1000),
            memory_range=(512, 2048),
            duration_range=(300, 86400),
            priority_range=(6, 10)
        ),
        "data_processing": JobTypeConfig(
            name="data_processing",
            cpu_range=(1000, 8000),
            memory_range=(2048, 16384),
            duration_range=(600, 7200),
            priority_range=(2, 6)
        ),
        "cron_job": JobTypeConfig(
            name="cron_job",
            cpu_range=(100, 500),
            memory_range=(256, 1024),
            duration_range=(30, 300),
            priority_range=(1, 3)
        )
    }
    
    # 시간대별 패턴 정의
    config.time_patterns = [
        # 업무 시간 (09:00-18:00)
        TimePatternConfig(
            time_range=(9, 18),
            job_distribution={
                "batch": 0.3,
                "ml_training": 0.2,
                "web_service": 0.3,
                "data_processing": 0.15,
                "cron_job": 0.05
            }
        ),
        # 저녁 시간 (18:00-23:00)
        TimePatternConfig(
            time_range=(18, 23),
            job_distribution={
                "batch": 0.4,
                "ml_training": 0.3,
                "web_service": 0.1,
                "data_processing": 0.15,
                "cron_job": 0.05
            }
        ),
        # 심야 시간 (23:00-09:00)
        TimePatternConfig(
            time_range=(23, 9),
            job_distribution={
                "batch": 0.5,
                "ml_training": 0.25,
                "web_service": 0.05,
                "data_processing": 0.1,
                "cron_job": 0.1
            }
        )
    ]
    
    return config