"""성능 벤치마크"""
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

from core.environment import SKRueueEnvironment
from core.agent import RLAgent
from core.interface import KueueInterface
from workload.generator import WorkloadGenerator
from utils import get_logger
from config.settings import get_config
from .monitor import PerformanceMonitor


class BenchmarkRunner:
    """벤치마크 실행기"""
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_logger('BenchmarkRunner')
        self.results = {}
        
    def run_scheduler_comparison(self, duration_minutes: int = 30,
                               algorithms: List[str] = None) -> Dict[str, Any]:
        """스케줄러 비교 벤치마크"""
        algorithms = algorithms or self.config.test.algorithms_to_test
        
        self.logger.info(f"Starting scheduler comparison benchmark")
        self.logger.info(f"Duration: {duration_minutes} minutes")
        self.logger.info(f"Algorithms: {algorithms}")
        
        # 기준 스케줄러 테스트
        self.logger.info("Testing baseline scheduler...")
        baseline_results = self._run_baseline_test(duration_minutes)
        self.results['baseline'] = baseline_results
        
        # RL 스케줄러 테스트
        for algorithm in algorithms:
            self.logger.info(f"Testing {algorithm} scheduler...")
            rl_results = self._run_rl_test(algorithm, duration_minutes)
            self.results[f'RL-{algorithm}'] = rl_results
        
        # 결과 분석
        comparison = self._analyze_results()
        
        # 시각화
        self._create_visualizations()
        
        return comparison
    
    def _run_baseline_test(self, duration_minutes: int) -> Dict[str, Any]:
        """기준 스케줄러 테스트"""
        # 환경 준비
        namespace = f"benchmark-baseline-{int(time.time())}"
        self._prepare_namespace(namespace)
        
        # 모니터링 시작
        monitor = PerformanceMonitor(namespace)
        monitor.start()
        
        # 워크로드 생성기
        generator = WorkloadGenerator(namespace)
        
        start_time = time.time()
        submitted_jobs = 0
        
        try:
            # 작업 생성 (suspend=False로 즉시 실행)
            while time.time() - start_time < duration_minutes * 60:
                template = generator.templates[list(generator.templates.keys())[submitted_jobs % len(generator.templates)]]
                generator.submit_job(template, suspend=False)
                submitted_jobs += 1
                
                time.sleep(self.config.workload.submission_interval)
                
        finally:
            monitor.stop()
            
        # 결과 수집
        summary = monitor.get_summary()
        summary['total_jobs_submitted'] = submitted_jobs
        
        # 정리
        self._cleanup_namespace(namespace)
        
        return summary
    
    def _run_rl_test(self, algorithm: str, duration_minutes: int) -> Dict[str, Any]:
        """RL 스케줄러 테스트"""
        # 환경 준비
        namespace = f"benchmark-{algorithm.lower()}-{int(time.time())}"
        self._prepare_namespace(namespace)
        
        # RL 환경 및 에이전트
        kueue = KueueInterface([namespace])
        env = SKRueueEnvironment(kueue)
        agent = RLAgent(env, algorithm)
        
        # 사전 훈련된 모델 로드 (있는 경우)
        model_path = f"{self.config.rl.model_save_dir}/skrueue_{algorithm.lower()}_model"
        try:
            agent.load(model_path)
            self.logger.info(f"Loaded pre-trained {algorithm} model")
        except Exception:
            self.logger.warning(f"No pre-trained model found, using untrained agent")
        
        # 모니터링 시작
        monitor = PerformanceMonitor(namespace)
        monitor.start()
        
        # 워크로드 생성기
        generator = WorkloadGenerator(namespace)
        
        start_time = time.time()
        submitted_jobs = 0
        
        # RL 에이전트 실행 스레드
        import threading
        
        def run_agent():
            obs, info = env.reset()
            while time.time() - start_time < duration_minutes * 60:
                action = agent.predict(obs)
                obs, reward, terminated, truncated, info = env.step(action)
                
                if terminated or truncated:
                    obs, info = env.reset()
                
                time.sleep(5)
        
        agent_thread = threading.Thread(target=run_agent)
        agent_thread.daemon = True
        agent_thread.start()
        
        try:
            # 작업 생성 (suspend=True로 RL 제어)
            while time.time() - start_time < duration_minutes * 60:
                template = generator.templates[list(generator.templates.keys())[submitted_jobs % len(generator.templates)]]
                generator.submit_job(template, suspend=True)
                submitted_jobs += 1
                
                time.sleep(self.config.workload.submission_interval)
                
        finally:
            monitor.stop()
        
        # 결과 수집
        summary = monitor.get_summary()
        summary['total_jobs_submitted'] = submitted_jobs
        summary['algorithm'] = algorithm
        
        # 정리
        self._cleanup_namespace(namespace)
        
        return summary
    
    def _prepare_namespace(self, namespace: str):
        """네임스페이스 준비"""
        try:
            subprocess.run(['kubectl', 'create', 'namespace', namespace], check=False)
            time.sleep(2)
        except Exception as e:
            self.logger.error(f"Failed to create namespace: {e}")
    
    def _cleanup_namespace(self, namespace: str):
        """네임스페이스 정리"""
        try:
            subprocess.run(['kubectl', 'delete', 'namespace', namespace, '--wait=false'], check=False)
        except Exception as e:
            self.logger.error(f"Failed to cleanup namespace: {e}")
    
    def _analyze_results(self) -> Dict[str, Any]:
        """결과 분석"""
        comparison = {}
        
        for scheduler_name, results in self.results.items():
            if 'latest_metrics' in results:
                metrics = results['latest_metrics']
                
                # 주요 지표 계산
                total_jobs = results.get('total_jobs_submitted', 0)
                succeeded = metrics.get('jobs_succeeded', 0)
                failed = metrics.get('jobs_failed', 0)
                
                comparison[scheduler_name] = {
                    'throughput': succeeded / (results.get('monitoring_duration', 1) / 3600),  # jobs/hour
                    'success_rate': succeeded / max(total_jobs, 1) * 100,
                    'failure_rate': failed / max(total_jobs, 1) * 100,
                    'avg_queue_length': results.get('averages', {}).get('jobs_pending', 0),
                    'oom_incidents': results.get('latest_metrics', {}).get('oom_incidents', 0),
                    'cpu_utilization': results.get('averages', {}).get('cpu_utilization', 0),
                    'memory_utilization': results.get('averages', {}).get('memory_utilization', 0)
                }
        
        return comparison
    
    def _create_visualizations(self):
        """결과 시각화"""
        if not self.results:
            return
        
        # 데이터 준비
        comparison = self._analyze_results()
        df = pd.DataFrame(comparison).T
        
        # 그래프 생성
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        
        # 1. 처리량
        df['throughput'].plot(kind='bar', ax=axes[0, 0], color='skyblue')
        axes[0, 0].set_title('Throughput (jobs/hour)')
        axes[0, 0].set_ylabel('Jobs per Hour')
        
        # 2. 성공률
        df['success_rate'].plot(kind='bar', ax=axes[0, 1], color='lightgreen')
        axes[0, 1].set_title('Success Rate (%)')
        axes[0, 1].set_ylabel('Percentage')
        
        # 3. 평균 큐 길이
        df['avg_queue_length'].plot(kind='bar', ax=axes[0, 2], color='orange')
        axes[0, 2].set_title('Average Queue Length')
        axes[0, 2].set_ylabel('Number of Jobs')
        
        # 4. OOM 사건
        df['oom_incidents'].plot(kind='bar', ax=axes[1, 0], color='red')
        axes[1, 0].set_title('OOM Incidents')
        axes[1, 0].set_ylabel('Count')
        
        # 5. CPU 활용도
        df['cpu_utilization'].plot(kind='bar', ax=axes[1, 1], color='purple')
        axes[1, 1].set_title('CPU Utilization')
        axes[1, 1].set_ylabel('Utilization')
        
        # 6. 메모리 활용도
        df['memory_utilization'].plot(kind='bar', ax=axes[1, 2], color='pink')
        axes[1, 2].set_title('Memory Utilization')
        axes[1, 2].set_ylabel('Utilization')
        
        plt.tight_layout()
        
        # 저장
        output_path = f"{self.config.test.output_dir}/benchmark_results.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info(f"Saved visualization to {output_path}")
