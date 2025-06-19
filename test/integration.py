"""통합 테스트"""
import os
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

from core.environment import SKRueueEnvironment
from core.agent import RLAgent
from core.interface import KueueInterface
from workload.generator import create_generator
from data.collector import DataCollector
from data.database import DatabaseManager
from data.exporter import DataExporter
from utils import get_logger
from config.settings import get_config
from .monitor import PerformanceMonitor
from .benchmark import BenchmarkRunner


class IntegrationTester:
    """통합 테스트 실행기"""
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_logger('IntegrationTester')
        
    def run_full_pipeline_test(self):
        """전체 파이프라인 테스트"""
        self.logger.info("Starting full pipeline integration test")
        
        try:
            # 1. 데이터 수집 테스트
            self.logger.info("1. Testing data collection...")
            self._test_data_collection()
            
            # 2. RL 훈련 테스트
            self.logger.info("2. Testing RL training...")
            self._test_rl_training()
            
            # 3. 추론 테스트
            self.logger.info("3. Testing inference...")
            self._test_inference()
            
            # 4. 벤치마크 테스트
            self.logger.info("4. Running benchmark...")
            self._test_benchmark()
            
            self.logger.info("✅ All integration tests passed!")
            
        except Exception as e:
            self.logger.error(f"❌ Integration test failed: {e}")
            raise
    
    def _test_data_collection(self):
        """데이터 수집 테스트"""
        # 데이터베이스 준비
        test_db_path = "test_skrueue.db"
        db_manager = DatabaseManager(test_db_path)
        
        # 수집기 생성
        collector = DataCollector(db_manager)
        
        # 수집 시작
        collector.start_collection()
        
        # 워크로드 생성
        generator = create_generator("uniform")
        
        # 몇 개 작업 생성
        for i in range(5):
            template = list(generator.templates.values())[i % len(generator.templates)]
            generator.submit_job(template)
            time.sleep(2)
        
        # 수집 대기
        time.sleep(30)
        
        # 수집 중지
        collector.stop_collection()
        
        # 데이터 확인
        stats = db_manager.get_statistics()
        
        assert stats['jobs']['total_jobs'] > 0, "No jobs collected"
        assert stats['cluster']['total_records'] > 0, "No cluster states collected"
        
        # 내보내기 테스트
        exporter = DataExporter(db_manager)
        export_path = exporter.export_training_data("test_training_data.csv")
        
        assert export_path and os.path.exists(export_path), "Export failed"
        
        # 정리
        os.remove(test_db_path)
        os.remove(export_path)
        
        self.logger.info("✅ Data collection test passed")
    
    def _test_rl_training(self):
        """RL 훈련 테스트"""
        # 환경 생성
        kueue = KueueInterface()
        env = SKRueueEnvironment(kueue)
        
        # 각 알고리즘 테스트
        for algorithm in ['DQN', 'PPO', 'A2C']:
            self.logger.info(f"Testing {algorithm}...")
            
            # 에이전트 생성
            agent = RLAgent(env, algorithm)
            
            # 짧은 훈련
            agent.train(total_timesteps=1000)
            
            # 모델 저장/로드
            test_path = f"test_{algorithm}_model"
            agent.save(test_path)
            
            # 새 에이전트로 로드
            new_agent = RLAgent(env, algorithm)
            new_agent.load(test_path)
            
            # 예측 테스트
            obs, _ = env.reset()
            action = new_agent.predict(obs)
            
            assert 0 <= action <= env.action_space.n - 1, "Invalid action"
            
            # 정리
            os.remove(test_path + ".zip")
            if os.path.exists(test_path + "_stats.pkl"):
                os.remove(test_path + "_stats.pkl")
        
        self.logger.info("✅ RL training test passed")
    
    def _test_inference(self):
        """추론 테스트"""
        # 환경 생성
        kueue = KueueInterface()
        env = SKRueueEnvironment(kueue)
        
        # 에이전트 생성
        agent = RLAgent(env, 'DQN')
        
        # 에피소드 실행
        obs, info = env.reset()
        total_reward = 0
        steps = 0
        
        for _ in range(10):
            action = agent.predict(obs)
            obs, reward, terminated, truncated, info = env.step(action)
            
            total_reward += reward
            steps += 1
            
            if terminated or truncated:
                break
        
        self.logger.info(f"Episode completed: steps={steps}, reward={total_reward:.3f}")
        self.logger.info("✅ Inference test passed")
    
    def _test_benchmark(self):
        """벤치마크 테스트"""
        runner = BenchmarkRunner()
        
        # 짧은 벤치마크 실행
        results = runner.run_scheduler_comparison(
            duration_minutes=5,
            algorithms=['DQN']
        )
        
        assert 'baseline' in runner.results, "Baseline test failed"
        assert 'RL-DQN' in runner.results, "RL test failed"
        
        self.logger.info("✅ Benchmark test passed")


def run_integration_tests():
    """통합 테스트 실행"""
    tester = IntegrationTester()
    tester.run_full_pipeline_test()