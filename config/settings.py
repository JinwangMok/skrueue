"""중앙 설정 관리"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import os
import yaml
from pathlib import Path


@dataclass
class KueueConfig:
    """Kueue 관련 설정"""
    namespaces: List[str] = field(default_factory=lambda: ["skrueue-test"])
    max_queue_size: int = 10
    admit_interval: int = 5  # 초


@dataclass
class RLConfig:
    """강화학습 설정"""
    algorithm: str = "DQN"
    state_dim: int = 67
    learning_rate: float = 1e-4
    buffer_size: int = 50000
    training_steps: int = 20000
    model_save_dir: str = "models"
    
    # 보상 가중치
    reward_weights: Dict[str, float] = field(default_factory=lambda: {
        'throughput': 0.4,
        'utilization': 0.3,
        'wait_penalty': 0.2,
        'failure_penalty': 0.1
    })


@dataclass
class DataConfig:
    """데이터 수집 설정"""
    collection_interval: int = 10  # 초
    retention_days: int = 30
    db_path: str = "skrueue_training_data.db"
    export_path: str = "data/training_data.csv"


@dataclass
class WorkloadConfig:
    """워크로드 생성 설정"""
    job_templates_path: str = "config/job_templates.yaml"
    submission_interval: int = 20  # 초
    max_concurrent_jobs: int = 8
    simulation_duration_hours: int = 24
    
    # 시간대별 패턴
    hourly_patterns: Dict[str, Dict] = field(default_factory=lambda: {
        "night": {  # 0-6시
            "hours": list(range(0, 6)),
            "job_rate": 2,
            "templates": ["etl-pipeline", "big-data-aggregation"]
        },
        "business": {  # 9-18시
            "hours": list(range(9, 18)),
            "job_rate": 5,
            "templates": ["realtime-analytics", "ml-training"]
        },
        "evening": {  # 19-23시
            "hours": list(range(19, 24)),
            "job_rate": 3,
            "templates": ["big-data-aggregation", "ml-training"]
        }
    })


@dataclass
class TestConfig:
    """테스트 설정"""
    test_duration_minutes: int = 30
    algorithms_to_test: List[str] = field(default_factory=lambda: ["DQN", "PPO", "A2C"])
    baseline_scheduler: str = "kueue-default"
    output_dir: str = "test_results"


@dataclass
class SKRueueConfig:
    """전체 설정"""
    kueue: KueueConfig = field(default_factory=KueueConfig)
    rl: RLConfig = field(default_factory=RLConfig)
    data: DataConfig = field(default_factory=DataConfig)
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)
    test: TestConfig = field(default_factory=TestConfig)
    
    # 환경 변수
    cluster_name: str = field(default_factory=lambda: os.getenv("CLUSTER_NAME", "default"))
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    
    @classmethod
    def from_yaml(cls, config_path: str) -> "SKRueueConfig":
        """YAML 파일에서 설정 로드"""
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        config = cls()
        
        # 재귀적으로 설정 업데이트
        if 'kueue' in data:
            config.kueue = KueueConfig(**data['kueue'])
        if 'rl' in data:
            config.rl = RLConfig(**data['rl'])
        if 'data' in data:
            config.data = DataConfig(**data['data'])
        if 'workload' in data:
            config.workload = WorkloadConfig(**data['workload'])
        if 'test' in data:
            config.test = TestConfig(**data['test'])
            
        return config
    
    def to_yaml(self, config_path: str):
        """설정을 YAML 파일로 저장"""
        data = {
            'kueue': self.kueue.__dict__,
            'rl': self.rl.__dict__,
            'data': self.data.__dict__,
            'workload': self.workload.__dict__,
            'test': self.test.__dict__,
            'cluster_name': self.cluster_name,
            'environment': self.environment,
            'log_level': self.log_level
        }
        
        with open(config_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False)
    
    def validate(self) -> List[str]:
        """설정 검증"""
        errors = []
        
        # 필수 디렉토리 확인
        required_dirs = [
            self.rl.model_save_dir,
            os.path.dirname(self.data.db_path),
            self.test.output_dir
        ]
        
        for dir_path in required_dirs:
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
        
        # 값 범위 검증
        if self.kueue.max_queue_size < 1:
            errors.append("max_queue_size must be at least 1")
        
        if self.data.collection_interval < 1:
            errors.append("collection_interval must be at least 1 second")
            
        if self.rl.algorithm not in ["DQN", "PPO", "A2C"]:
            errors.append(f"Unknown algorithm: {self.rl.algorithm}")
            
        return errors


# 싱글톤 설정 인스턴스
_config_instance: Optional[SKRueueConfig] = None


def get_config(config_path: Optional[str] = None) -> SKRueueConfig:
    """설정 인스턴스 가져오기"""
    global _config_instance
    
    if _config_instance is None:
        if config_path and os.path.exists(config_path):
            _config_instance = SKRueueConfig.from_yaml(config_path)
        else:
            _config_instance = SKRueueConfig()
        
        # 검증
        errors = _config_instance.validate()
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    return _config_instance