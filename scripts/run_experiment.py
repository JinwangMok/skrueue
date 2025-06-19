"""실험 실행 스크립트"""
import argparse
import time
from datetime import datetime

from core.environment import SKRueueEnvironment
from core.agent import RLAgent
from core.interface import KueueInterface
from workload.generator import create_generator
from data.collector import DataCollector
from data.database import DatabaseManager
from data.exporter import DataExporter
from test.benchmark import BenchmarkRunner
from utils import get_logger
from config.settings import get_config


class ExperimentRunner:
    """실험 실행기"""
    
    def __init__(self, experiment_name: str = None):
        self.config = get_config()
        self.logger = get_logger('ExperimentRunner')
        self.experiment_name = experiment_name or f"exp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    def run_training_experiment(self, algorithm: str, timesteps: int):
        """훈련 실험"""
        self.logger.info(f"Starting training experiment: {self.experiment_name}")
        self.logger.info(f"Algorithm: {algorithm}, Timesteps: {timesteps}")
        
        # 환경 생성
        kueue = KueueInterface()
        env = SKRueueEnvironment(kueue)
        
        # 에이전트 생성 및 훈련
        agent = RLAgent(env, algorithm)
        
        start_time = time.time()
        agent.train(total_timesteps=timesteps)
        training_time = time.time() - start_time
        
        # 모델 저장
        model_path = f"{self.config.rl.model_save_dir}/{self.experiment_name}_{algorithm}"
        agent.save(model_path)
        
        # 평가
        eval_results = agent.evaluate(num_episodes=10)
        
        # 결과 기록
        results = {
            'experiment_name': self.experiment_name,
            'algorithm': algorithm,
            'timesteps': timesteps,
            'training_time': training_time,
            'evaluation': eval_results
        }
        
        self.logger.info(f"Training completed in {training_time/60:.2f} minutes")
        self.logger.info(f"Evaluation results: {eval_results}")
        
        return results
    
    def run_data_collection_experiment(self, duration_hours: int, strategy: str = "realistic"):
        """데이터 수집 실험"""
        self.logger.info(f"Starting data collection experiment: {self.experiment_name}")
        self.logger.info(f"Duration: {duration_hours} hours, Strategy: {strategy}")
        
        # 데이터베이스 준비
        db_path = f"data/{self.experiment_name}_data.db"
        db_manager = DatabaseManager(db_path)
        
        # 수집기 시작
        collector = DataCollector(db_manager)
        collector.start_collection()
        
        # 워크로드 생성기
        generator = create_generator(strategy)
        
        # 시뮬레이션 실행
        generator.run_simulation(duration_hours, realtime=False)
        
        # 수집 중지
        collector.stop_collection()
        
        # 통계
        stats = db_manager.get_statistics()
        
        # 데이터 내보내기
        exporter = DataExporter(db_manager)
        export_path = f"data/{self.experiment_name}_training_data.csv"
        exporter.export_training_data(export_path)
        
        self.logger.info(f"Data collection completed")
        self.logger.info(f"Statistics: {stats}")
        self.logger.info(f"Exported to: {export_path}")
        
        return {
            'experiment_name': self.experiment_name,
            'duration_hours': duration_hours,
            'strategy': strategy,
            'statistics': stats,
            'export_path': export_path
        }
    
    def run_benchmark_experiment(self, duration_minutes: int, algorithms: List[str]):
        """벤치마크 실험"""
        self.logger.info(f"Starting benchmark experiment: {self.experiment_name}")
        
        runner = BenchmarkRunner()
        results = runner.run_scheduler_comparison(
            duration_minutes=duration_minutes,
            algorithms=algorithms
        )
        
        # 결과 저장
        import json
        results_path = f"{self.config.test.output_dir}/{self.experiment_name}_benchmark.json"
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Benchmark completed")
        self.logger.info(f"Results saved to: {results_path}")
        
        return results
