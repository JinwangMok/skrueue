# """
# SKRueue 통합 테스트 시스템
# 실제 Kubernetes 클러스터에서 RL 기반 스케줄러 성능을 테스트하고 평가
# """

import os
import time
import json
import logging
import threading
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import yaml
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

# 이전에 정의한 모듈들 import (실제 환경에서는 별도 파일로 분리)
from kueue_rl_interface import SKRueueEnvironment, RLAgent, KueueInterface, create_skrueue_environment

@dataclass
class TestConfiguration:
    """테스트 설정"""
    test_duration_minutes: int = 60
    job_submission_interval: int = 30  # 초
    max_concurrent_jobs: int = 10
    algorithms_to_test: List[str] = None
    baseline_scheduler: str = "kueue-default"
    output_dir: str = "skrueue_test_results"
    namespace: str = "skrueue-test"
    
    def __post_init__(self):
        if self.algorithms_to_test is None:
            self.algorithms_to_test = ['DQN', 'PPO', 'A2C']

@dataclass
class JobTemplate:
    """테스트 작업 템플릿"""
    name: str
    job_type: str  # 'cpu-intensive', 'memory-intensive', 'mixed'
    cpu_request: str
    memory_request: str
    estimated_duration: int  # 분
    image: str
    command: List[str]
    priority: int = 0

@dataclass
class TestResults:
    """테스트 결과"""
    scheduler_name: str
    total_jobs_submitted: int
    total_jobs_completed: int
    total_jobs_failed: int
    average_wait_time: float  # 분
    average_execution_time: float  # 분
    resource_utilization: Dict[str, float]
    oom_incidents: int
    throughput: float  # jobs/hour
    scheduler_decisions: List[Dict]

class JobGenerator:
    """테스트용 작업 생성기"""
    
    def __init__(self, namespace: str = "skrueue-test"):
        self.namespace = namespace
        self.logger = logging.getLogger('JobGenerator')
        self.job_counter = 0
        
        # 다양한 작업 템플릿 정의
        self.job_templates = [
            JobTemplate(
                name="spark-etl",
                job_type="memory-intensive",
                cpu_request="2000m",
                memory_request="8Gi",
                estimated_duration=45,
                image="apache/spark:3.4.0",
                command=[
                    "/opt/spark/bin/spark-submit",
                    "--class", "org.apache.spark.examples.SparkPi",
                    "--master", "k8s://https://kubernetes.default.svc:443",
                    "--deploy-mode", "cluster",
                    "--executor-memory", "4g",
                    "--num-executors", "3",
                    "local:///opt/spark/examples/jars/spark-examples.jar",
                    "1000"
                ],
                priority=5
            ),
            JobTemplate(
                name="ml-training",
                job_type="cpu-intensive",
                cpu_request="4000m",
                memory_request="6Gi",
                estimated_duration=30,
                image="tensorflow/tensorflow:latest-gpu",
                command=[
                    "python", "-c",
                    "import tensorflow as tf; import time; "
                    "print('Training started'); time.sleep(1800); print('Training completed')"
                ],
                priority=8
            ),
            JobTemplate(
                name="batch-processing",
                job_type="mixed",
                cpu_request="1000m", 
                memory_request="4Gi",
                estimated_duration=20,
                image="python:3.9",
                command=[
                    "python", "-c",
                    "import time; import random; "
                    "print('Batch processing started'); "
                    "time.sleep(random.randint(600, 1800)); "
                    "print('Batch processing completed')"
                ],
                priority=3
            ),
            JobTemplate(
                name="data-analysis",
                job_type="memory-intensive",
                cpu_request="1500m",
                memory_request="12Gi",
                estimated_duration=25,
                image="jupyter/scipy-notebook:latest",
                command=[
                    "python", "-c",
                    "import pandas as pd; import numpy as np; import time; "
                    "print('Data analysis started'); "
                    "df = pd.DataFrame(np.random.randn(1000000, 10)); "
                    "time.sleep(1200); "
                    "print('Data analysis completed')"
                ],
                priority=6
            )
        ]
        
    def create_test_job(self, template: JobTemplate) -> str:
        """테스트 작업 YAML 생성"""
        self.job_counter += 1
        job_name = f"{template.name}-{self.job_counter:04d}"
        
        job_yaml = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': job_name,
                'namespace': self.namespace,
                'labels': {
                    'app': 'skrueue-test',
                    'job-type': template.job_type,
                    'scheduler': 'skrueue',
                    'priority': str(template.priority)
                },
                'annotations': {
                    'skrueue.ai/estimated-duration': str(template.estimated_duration),
                    'skrueue.ai/job-type': template.job_type
                }
            },
            'spec': {
                'suspend': True,  # Kueue가 관리하도록 일시정지 상태로 생성
                'template': {
                    'metadata': {
                        'labels': {
                            'app': 'skrueue-test',
                            'job-name': job_name
                        }
                    },
                    'spec': {
                        'restartPolicy': 'Never',
                        'containers': [{
                            'name': 'worker',
                            'image': template.image,
                            'command': template.command,
                            'resources': {
                                'requests': {
                                    'cpu': template.cpu_request,
                                    'memory': template.memory_request
                                },
                                'limits': {
                                    'cpu': template.cpu_request,
                                    'memory': template.memory_request
                                }
                            }
                        }]
                    }
                }
            }
        }
        
        return yaml.dump(job_yaml, default_flow_style=False)
        
    def submit_job(self, template: JobTemplate) -> bool:
        """작업을 클러스터에 제출"""
        try:
            job_yaml = self.create_test_job(template)
            
            # kubectl로 작업 제출
            process = subprocess.Popen(
                ['kubectl', 'apply', '-f', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=job_yaml)
            
            if process.returncode == 0:
                self.logger.info(f"작업 제출 성공: {template.name}-{self.job_counter:04d}")
                return True
            else:
                self.logger.error(f"작업 제출 실패: {stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"작업 제출 중 오류: {e}")
            return False
            
    def generate_random_workload(self) -> JobTemplate:
        """랜덤한 워크로드 생성"""
        import random
        
        # 가중치를 적용한 템플릿 선택 (메모리 집약적 작업을 더 자주)
        weights = [0.4, 0.2, 0.2, 0.2]  # spark-etl에 더 높은 가중치
        template = random.choices(self.job_templates, weights=weights)[0]
        
        # 약간의 변동성 추가
        variance_factor = random.uniform(0.8, 1.2)
        
        # 새로운 템플릿 생성 (원본 수정 방지)
        new_template = JobTemplate(
            name=template.name,
            job_type=template.job_type,
            cpu_request=template.cpu_request,
            memory_request=template.memory_request,
            estimated_duration=int(template.estimated_duration * variance_factor),
            image=template.image,
            command=template.command,
            priority=template.priority
        )
        
        return new_template

class PerformanceMonitor:
    """성능 모니터링 시스템"""
    
    def __init__(self, namespace: str = "skrueue-test"):
        self.namespace = namespace
        self.logger = logging.getLogger('PerformanceMonitor')
        self.monitoring_active = False
        self.metrics_data = []
        
    def start_monitoring(self):
        """모니터링 시작"""
        self.monitoring_active = True
        self.metrics_data.clear()
        
        monitoring_thread = threading.Thread(target=self._monitoring_loop)
        monitoring_thread.daemon = True
        monitoring_thread.start()
        
        self.logger.info("성능 모니터링 시작")
        
    def stop_monitoring(self):
        """모니터링 중지"""
        self.monitoring_active = False
        self.logger.info("성능 모니터링 중지")
        
    def _monitoring_loop(self):
        """모니터링 메인 루프"""
        while self.monitoring_active:
            try:
                metrics = self._collect_metrics()
                self.metrics_data.append(metrics)
                time.sleep(30)  # 30초마다 수집
                
            except Exception as e:
                self.logger.error(f"메트릭 수집 오류: {e}")
                
    def _collect_metrics(self) -> Dict:
        """현재 시점의 메트릭 수집"""
        try:
            # kubectl 명령으로 메트릭 수집
            
            # 1. 작업 상태 조회
            jobs_result = subprocess.run(
                ['kubectl', 'get', 'jobs', '-n', self.namespace, '-o', 'json'],
                capture_output=True, text=True
            )
            
            # 2. 팟 상태 조회  
            pods_result = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', self.namespace, '-o', 'json'],
                capture_output=True, text=True
            )
            
            # 3. 노드 리소스 사용률 조회 (metrics-server 필요)
            try:
                nodes_metrics = subprocess.run(
                    ['kubectl', 'top', 'nodes', '--no-headers'],
                    capture_output=True, text=True
                )
            except:
                nodes_metrics = None
                
            timestamp = datetime.now()
            
            # JSON 파싱
            jobs_data = json.loads(jobs_result.stdout) if jobs_result.returncode == 0 else {'items': []}
            pods_data = json.loads(pods_result.stdout) if pods_result.returncode == 0 else {'items': []}
            
            # 메트릭 계산
            pending_jobs = sum(1 for job in jobs_data['items'] 
                             if not job.get('status', {}).get('active', 0))
            running_jobs = sum(1 for job in jobs_data['items'] 
                             if job.get('status', {}).get('active', 0))
            completed_jobs = sum(1 for job in jobs_data['items'] 
                               if job.get('status', {}).get('succeeded', 0))
            failed_jobs = sum(1 for job in jobs_data['items'] 
                            if job.get('status', {}).get('failed', 0))
            
            # OOM 팟 카운트
            oom_pods = sum(1 for pod in pods_data['items']
                          if self._is_pod_oom_killed(pod))
            
            return {
                'timestamp': timestamp,
                'pending_jobs': pending_jobs,
                'running_jobs': running_jobs,
                'completed_jobs': completed_jobs,
                'failed_jobs': failed_jobs,
                'oom_incidents': oom_pods,
                'total_pods': len(pods_data['items'])
            }
            
        except Exception as e:
            self.logger.error(f"메트릭 수집 실패: {e}")
            return {'timestamp': datetime.now(), 'error': str(e)}
            
    def _is_pod_oom_killed(self, pod: Dict) -> bool:
        """팟이 OOM으로 종료되었는지 확인"""
        status = pod.get('status', {})
        container_statuses = status.get('containerStatuses', [])
        
        for container_status in container_statuses:
            terminated = container_status.get('state', {}).get('terminated', {})
            if terminated.get('reason') == 'OOMKilled':
                return True
                
            last_state = container_status.get('lastState', {}).get('terminated', {})
            if last_state.get('reason') == 'OOMKilled':
                return True
                
        return False
        
    def get_metrics_summary(self) -> Dict:
        """수집된 메트릭 요약"""
        if not self.metrics_data:
            return {}
            
        df = pd.DataFrame(self.metrics_data)
        
        return {
            'total_samples': len(df),
            'avg_pending_jobs': df['pending_jobs'].mean(),
            'avg_running_jobs': df['running_jobs'].mean(),
            'total_completed': df['completed_jobs'].iloc[-1] if len(df) > 0 else 0,
            'total_failed': df['failed_jobs'].iloc[-1] if len(df) > 0 else 0,
            'total_oom_incidents': df['oom_incidents'].sum(),
            'peak_concurrent_jobs': df['running_jobs'].max()
        }

class SKRueueTester:
    """SKRueue 통합 테스터"""
    
    def __init__(self, config: TestConfiguration):
        self.config = config
        self.logger = logging.getLogger('SKRueueTester')
        
        # 컴포넌트 초기화
        self.job_generator = JobGenerator(config.namespace)
        self.performance_monitor = PerformanceMonitor(config.namespace)
        
        # 결과 저장
        self.test_results: Dict[str, TestResults] = {}
        
        # 출력 디렉토리 생성
        os.makedirs(config.output_dir, exist_ok=True)
        
    def setup_test_environment(self):
        """테스트 환경 설정"""
        self.logger.info("테스트 환경 설정 중...")
        
        # 네임스페이스 생성
        try:
            subprocess.run([
                'kubectl', 'create', 'namespace', self.config.namespace
            ], check=False)  # 이미 존재할 수 있으므로 오류 무시
            
            # 기존 테스트 작업 정리
            subprocess.run([
                'kubectl', 'delete', 'jobs', '--all', 
                '-n', self.config.namespace
            ], check=False)
            
            time.sleep(10)  # 정리 완료 대기
            
            self.logger.info("테스트 환경 설정 완료")
            
        except Exception as e:
            self.logger.error(f"테스트 환경 설정 실패: {e}")
            raise
            
    def run_baseline_test(self) -> TestResults:
        """기준 스케줄러 테스트"""
        self.logger.info("기준 스케줄러 테스트 시작")
        
        # 모니터링 시작
        self.performance_monitor.start_monitoring()
        
        start_time = time.time()
        submitted_jobs = 0
        
        try:
            # 지정된 시간 동안 작업 제출
            while time.time() - start_time < self.config.test_duration_minutes * 60:
                # 랜덤 작업 생성 및 제출
                template = self.job_generator.generate_random_workload()
                
                if self.job_generator.submit_job(template):
                    submitted_jobs += 1
                    
                # 다음 제출까지 대기
                time.sleep(self.config.job_submission_interval)
                
                # 동시 실행 작업 수 제한 확인
                if submitted_jobs % 5 == 0:  # 5개마다 체크
                    self._wait_for_capacity()
                    
        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 테스트 중단")
            
        finally:
            # 모니터링 중지
            self.performance_monitor.stop_monitoring()
            
        # 결과 분석
        results = self._analyze_results(self.config.baseline_scheduler, submitted_jobs)
        self.test_results[self.config.baseline_scheduler] = results
        
        return results
        
    def run_rl_scheduler_test(self, algorithm: str, model_path: Optional[str] = None) -> TestResults:
        """RL 스케줄러 테스트"""
        self.logger.info(f"{algorithm} RL 스케줄러 테스트 시작")
        
        # RL 환경 및 에이전트 설정
        try:
            env = create_skrueue_environment([self.config.namespace])
            agent = RLAgent(env, algorithm)
            
            if model_path and os.path.exists(model_path):
                agent.load_model(model_path)
                self.logger.info(f"사전 훈련된 모델 로드: {model_path}")
            else:
                # 간단한 온라인 학습 (실제로는 사전 훈련 권장)
                self.logger.info("온라인 학습 시작...")
                agent.train(total_timesteps=5000)
                
        except Exception as e:
            self.logger.error(f"RL 에이전트 설정 실패: {e}")
            return None
            
        # 모니터링 시작
        self.performance_monitor.start_monitoring()
        
        start_time = time.time()
        submitted_jobs = 0
        scheduler_decisions = []
        
        # RL 에이전트 실행 스레드
        def run_rl_agent():
            obs = env.reset()
            while time.time() - start_time < self.config.test_duration_minutes * 60:
                try:
                    action = agent.predict(obs)
                    obs, reward, done, info = env.step(action)
                    
                    # 결정 기록
                    scheduler_decisions.append({
                        'timestamp': datetime.now(),
                        'action': int(action),
                        'reward': float(reward),
                        'queue_length': info.get('queue_length', 0),
                        'cluster_utilization': info.get('cluster_utilization', 0)
                    })
                    
                    if done:
                        obs = env.reset()
                        
                    time.sleep(5)  # 5초마다 결정
                    
                except Exception as e:
                    self.logger.error(f"RL 에이전트 실행 오류: {e}")
                    break
                    
        # RL 에이전트를 별도 스레드에서 실행
        agent_thread = threading.Thread(target=run_rl_agent)
        agent_thread.daemon = True
        agent_thread.start()
        
        try:
            # 작업 제출 루프
            while time.time() - start_time < self.config.test_duration_minutes * 60:
                template = self.job_generator.generate_random_workload()
                
                if self.job_generator.submit_job(template):
                    submitted_jobs += 1
                    
                time.sleep(self.config.job_submission_interval)
                
                if submitted_jobs % 5 == 0:
                    self._wait_for_capacity()
                    
        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 테스트 중단")
            
        finally:
            self.performance_monitor.stop_monitoring()
            
        # 결과 분석
        results = self._analyze_results(f"skrueue-{algorithm}", submitted_jobs, scheduler_decisions)
        self.test_results[f"skrueue-{algorithm}"] = results
        
        return results
        
    def _wait_for_capacity(self):
        """클러스터 용량 대기"""
        try:
            # 현재 실행 중인 작업 수 확인
            result = subprocess.run([
                'kubectl', 'get', 'jobs', '-n', self.config.namespace,
                '--field-selector=status.active=1', '--no-headers'
            ], capture_output=True, text=True)
            
            running_count = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
            
            # 최대 동시 실행 수 제한
            while running_count >= self.config.max_concurrent_jobs:
                time.sleep(30)  # 30초 대기
                
                result = subprocess.run([
                    'kubectl', 'get', 'jobs', '-n', self.config.namespace,
                    '--field-selector=status.active=1', '--no-headers'
                ], capture_output=True, text=True)
                
                running_count = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
                
        except Exception as e:
            self.logger.warning(f"용량 확인 실패: {e}")
            
    def _analyze_results(self, scheduler_name: str, submitted_jobs: int, 
                        scheduler_decisions: List[Dict] = None) -> TestResults:
        """테스트 결과 분석"""
        try:
            # 메트릭 요약
            metrics_summary = self.performance_monitor.get_metrics_summary()
            
            # 작업 상태 조회
            jobs_result = subprocess.run([
                'kubectl', 'get', 'jobs', '-n', self.config.namespace, '-o', 'json'
            ], capture_output=True, text=True)
            
            if jobs_result.returncode != 0:
                raise Exception("작업 상태 조회 실패")
                
            jobs_data = json.loads(jobs_result.stdout)
            
            # 결과 계산
            completed_jobs = 0
            failed_jobs = 0
            total_wait_time = 0
            total_execution_time = 0
            
            for job in jobs_data['items']:
                status = job.get('status', {})
                
                if status.get('succeeded'):
                    completed_jobs += 1
                elif status.get('failed'):
                    failed_jobs += 1
                    
                # 시간 계산
                creation_time = job['metadata'].get('creationTimestamp')
                start_time = status.get('startTime')
                completion_time = status.get('completionTime')
                
                if creation_time and start_time:
                    creation_dt = datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    wait_time = (start_dt - creation_dt).total_seconds() / 60.0
                    total_wait_time += wait_time
                    
                if start_time and completion_time:
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    completion_dt = datetime.fromisoformat(completion_time.replace('Z', '+00:00'))
                    execution_time = (completion_dt - start_dt).total_seconds() / 60.0
                    total_execution_time += execution_time
                    
            # 평균 계산
            total_finished = completed_jobs + failed_jobs
            avg_wait_time = total_wait_time / max(total_finished, 1)
            avg_execution_time = total_execution_time / max(completed_jobs, 1)
            
            # 처리량 계산 (jobs/hour)
            throughput = completed_jobs / (self.config.test_duration_minutes / 60.0)
            
            return TestResults(
                scheduler_name=scheduler_name,
                total_jobs_submitted=submitted_jobs,
                total_jobs_completed=completed_jobs,
                total_jobs_failed=failed_jobs,
                average_wait_time=avg_wait_time,
                average_execution_time=avg_execution_time,
                resource_utilization={
                    'cpu': 0.0,  # 실제 구현에서는 메트릭 서버에서 가져옴
                    'memory': 0.0
                },
                oom_incidents=metrics_summary.get('total_oom_incidents', 0),
                throughput=throughput,
                scheduler_decisions=scheduler_decisions or []
            )
            
        except Exception as e:
            self.logger.error(f"결과 분석 실패: {e}")
            return None
            
    def run_full_comparison_test(self):
        """전체 비교 테스트 실행"""
        self.logger.info("SKRueue 전체 비교 테스트 시작")
        
        try:
            # 환경 설정
            self.setup_test_environment()
            
            # 1. 기준 스케줄러 테스트
            baseline_results = self.run_baseline_test()
            
            # 결과 간 간격
            time.sleep(60)
            
            # 2. RL 스케줄러들 테스트
            for algorithm in self.config.algorithms_to_test:
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"{algorithm} 테스트 시작")
                self.logger.info(f"{'='*50}")
                
                # 환경 초기화
                self.setup_test_environment()
                time.sleep(30)
                
                # RL 테스트 실행
                rl_results = self.run_rl_scheduler_test(algorithm)
                
                time.sleep(60)  # 다음 테스트 전 대기
                
            # 3. 결과 분석 및 리포트 생성
            self.generate_comparison_report()
            
        except Exception as e:
            self.logger.error(f"전체 테스트 실패: {e}")
            
    def generate_comparison_report(self):
        """비교 리포트 생성"""
        self.logger.info("비교 리포트 생성 중...")
        
        try:
            # 결과 데이터프레임 생성
            results_data = []
            
            for scheduler_name, results in self.test_results.items():
                if results:
                    results_data.append({
                        'Scheduler': scheduler_name,
                        'Jobs Submitted': results.total_jobs_submitted,
                        'Jobs Completed': results.total_jobs_completed,
                        'Jobs Failed': results.total_jobs_failed,
                        'Success Rate (%)': (results.total_jobs_completed / max(results.total_jobs_submitted, 1)) * 100,
                        'Avg Wait Time (min)': results.average_wait_time,
                        'Avg Execution Time (min)': results.average_execution_time,
                        'Throughput (jobs/hour)': results.throughput,
                        'OOM Incidents': results.oom_incidents
                    })
                    
            df = pd.DataFrame(results_data)
            
            # CSV 저장
            csv_path = os.path.join(self.config.output_dir, 'comparison_results.csv')
            df.to_csv(csv_path, index=False)
            
            # 시각화 생성
            self._create_performance_charts(df)
            
            # 마크다운 리포트 생성
            self._generate_markdown_report(df)
            
            self.logger.info(f"비교 리포트 생성 완료: {self.config.output_dir}")
            
        except Exception as e:
            self.logger.error(f"리포트 생성 실패: {e}")
            
    def _create_performance_charts(self, df: pd.DataFrame):
        """성능 비교 차트 생성"""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            
            # 1. 처리량 비교
            axes[0, 0].bar(df['Scheduler'], df['Throughput (jobs/hour)'])
            axes[0, 0].set_title('Throughput Comparison')
            axes[0, 0].set_ylabel('Jobs per Hour')
            axes[0, 0].tick_params(axis='x', rotation=45)
            
            # 2. 평균 대기시간 비교
            axes[0, 1].bar(df['Scheduler'], df['Avg Wait Time (min)'])
            axes[0, 1].set_title('Average Wait Time Comparison')
            axes[0, 1].set_ylabel('Minutes')
            axes[0, 1].tick_params(axis='x', rotation=45)
            
            # 3. 성공률 비교
            axes[1, 0].bar(df['Scheduler'], df['Success Rate (%)'])
            axes[1, 0].set_title('Job Success Rate Comparison')
            axes[1, 0].set_ylabel('Success Rate (%)')
            axes[1, 0].tick_params(axis='x', rotation=45)
            
            # 4. OOM 사건 수 비교
            axes[1, 1].bar(df['Scheduler'], df['OOM Incidents'])
            axes[1, 1].set_title('OOM Incidents Comparison')
            axes[1, 1].set_ylabel('Number of OOM Incidents')
            axes[1, 1].tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            chart_path = os.path.join(self.config.output_dir, 'performance_comparison.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"성능 차트 저장: {chart_path}")
            
        except Exception as e:
            self.logger.error(f"차트 생성 실패: {e}")
            
    def _generate_markdown_report(self, df: pd.DataFrame):
        """마크다운 리포트 생성"""
        try:
            report_path = os.path.join(self.config.output_dir, 'SKRueue_Test_Report.md')
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# SKRueue Performance Test Report\n\n")
                f.write(f"**Test Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"**Test Configuration:**\n")
                f.write(f"- Duration: {self.config.test_duration_minutes} minutes\n")
                f.write(f"- Job Submission Interval: {self.config.job_submission_interval} seconds\n")
                f.write(f"- Max Concurrent Jobs: {self.config.max_concurrent_jobs}\n")
                f.write(f"- Algorithms Tested: {', '.join(self.config.algorithms_to_test)}\n\n")
                
                f.write("## Test Results Summary\n\n")
                f.write(df.to_markdown(index=False))
                f.write("\n\n")
                
                f.write("## Key Findings\n\n")
                
                # 최고 성능 스케줄러 찾기
                best_throughput = df.loc[df['Throughput (jobs/hour)'].idxmax(), 'Scheduler']
                best_wait_time = df.loc[df['Avg Wait Time (min)'].idxmin(), 'Scheduler']
                best_success_rate = df.loc[df['Success Rate (%)'].idxmax(), 'Scheduler']
                
                f.write(f"- **Best Throughput:** {best_throughput}\n")
                f.write(f"- **Lowest Wait Time:** {best_wait_time}\n")
                f.write(f"- **Highest Success Rate:** {best_success_rate}\n\n")
                
                f.write("## Performance Charts\n\n")
                f.write("![Performance Comparison](performance_comparison.png)\n\n")
                
                f.write("## Conclusion\n\n")
                f.write("이 테스트는 SKRueue RL 기반 스케줄러의 성능을 기존 Kueue 스케줄러와 비교한 결과입니다. ")
                f.write("자세한 분석을 위해서는 더 긴 시간의 테스트와 다양한 워크로드 패턴이 필요합니다.\n")
                
            self.logger.info(f"마크다운 리포트 저장: {report_path}")
            
        except Exception as e:
            self.logger.error(f"마크다운 리포트 생성 실패: {e}")

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SKRueue 통합 테스트 시스템')
    parser.add_argument('--duration', type=int, default=30, help='테스트 지속 시간 (분)')
    parser.add_argument('--algorithms', nargs='*', default=['DQN', 'PPO'], 
                       help='테스트할 RL 알고리즘')
    parser.add_argument('--namespace', type=str, default='skrueue-test', 
                       help='테스트 네임스페이스')
    parser.add_argument('--output-dir', type=str, default='skrueue_test_results',
                       help='결과 출력 디렉토리')
    parser.add_argument('--job-interval', type=int, default=30,
                       help='작업 제출 간격 (초)')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('skrueue_test.log'),
            logging.StreamHandler()
        ]
    )
    
    # 테스트 설정
    config = TestConfiguration(
        test_duration_minutes=args.duration,
        algorithms_to_test=args.algorithms,
        namespace=args.namespace,
        output_dir=args.output_dir,
        job_submission_interval=args.job_interval
    )
    
    # 테스터 생성 및 실행
    tester = SKRueueTester(config)
    
    try:
        tester.run_full_comparison_test()
        print(f"\n✅ 테스트 완료! 결과는 {args.output_dir} 에서 확인하세요.")
        
    except KeyboardInterrupt:
        print("\n❌ 사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 테스트 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    main()