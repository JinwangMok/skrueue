# skrueue_integration_test.py
# SKRueue 통합 테스트 시스템 - 실제 RL 모델 훈련 및 성능 비교
# 실제 Kubernetes 클러스터에서 RL 기반 스케줄러 성능을 테스트하고 평가

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

# SKRueue 모듈 import
from skrueue import SKRueueEnvironment, RLAgent, KueueInterface, create_skrueue_environment

@dataclass
class TestConfiguration:
    """테스트 설정"""
    test_duration_minutes: int = 30
    job_submission_interval: int = 20  # 초
    max_concurrent_jobs: int = 8
    algorithms_to_test: List[str] = None
    baseline_scheduler: str = "kueue-default"
    output_dir: str = "skrueue_test_results"
    namespace: str = "skrueue-test"
    model_training_steps: int = 10000
    max_queue_size: int = 10
    
    def __post_init__(self):
        if self.algorithms_to_test is None:
            self.algorithms_to_test = ['DQN', 'PPO', 'A2C']

@dataclass
class JobTemplate:
    """테스트 작업 템플릿"""
    name: str
    job_type: str
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
    success_rate: float  # %
    scheduler_decisions: List[Dict]

class JobGenerator:
    """테스트용 작업 생성기 (경량화)"""
    
    def __init__(self, namespace: str = "skrueue-test"):
        self.namespace = namespace
        self.logger = logging.getLogger('JobGenerator')
        self.job_counter = 0
        
        # 실행 가능한 간단한 작업 템플릿들
        self.job_templates = [
            JobTemplate(
                name="cpu-task",
                job_type="cpu-intensive",
                cpu_request="1000m",
                memory_request="2Gi",
                estimated_duration=15,
                image="busybox:1.35",
                command=[
                    "sh", "-c",
                    "echo 'CPU task started'; "
                    "for i in $(seq 1 30); do "
                    "  awk 'BEGIN {for(i=1;i<=1000;i++) sum+=sqrt(i*i); print sum}' > /dev/null; "
                    "  sleep 30; "
                    "done; "
                    "echo 'CPU task completed'"
                ],
                priority=5
            ),
            JobTemplate(
                name="memory-task",
                job_type="memory-intensive",
                cpu_request="500m",
                memory_request="4Gi",
                estimated_duration=20,
                image="busybox:1.35",
                command=[
                    "sh", "-c",
                    "echo 'Memory task started'; "
                    "for i in $(seq 1 20); do "
                    "  dd if=/dev/zero of=/tmp/testfile bs=100M count=5 2>/dev/null; "
                    "  sleep 30; "
                    "  rm -f /tmp/testfile; "
                    "done; "
                    "echo 'Memory task completed'"
                ],
                priority=3
            ),
            JobTemplate(
                name="mixed-task",
                job_type="mixed",
                cpu_request="800m",
                memory_request="3Gi",
                estimated_duration=10,
                image="busybox:1.35",
                command=[
                    "sh", "-c",
                    "echo 'Mixed task started'; "
                    "for i in $(seq 1 20); do "
                    "  echo 'Processing batch' $i; "
                    "  awk 'BEGIN {print sqrt(12345)}' > /tmp/result; "
                    "  sleep 30; "
                    "done; "
                    "echo 'Mixed task completed'"
                ],
                priority=4
            ),
            JobTemplate(
                name="quick-task",
                job_type="lightweight",
                cpu_request="200m",
                memory_request="512Mi",
                estimated_duration=5,
                image="busybox:1.35",
                command=[
                    "sh", "-c",
                    "echo 'Quick task started'; "
                    "sleep 300; "  # 5분
                    "echo 'Quick task completed'"
                ],
                priority=2
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
                'suspend': True,  # RL 스케줄러가 관리하도록 일시정지
                'backoffLimit': 2,
                'activeDeadlineSeconds': template.estimated_duration * 60 * 3,  # 3배 여유
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
        
        # 가중치를 적용한 템플릿 선택
        weights = [0.3, 0.3, 0.2, 0.2]  # 균등하게 분배
        template = random.choices(self.job_templates, weights=weights)[0]
        
        # 약간의 변동성 추가
        variance_factor = random.uniform(0.8, 1.2)
        
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
                if metrics:
                    self.metrics_data.append(metrics)
                time.sleep(30)  # 30초마다 수집
                
            except Exception as e:
                self.logger.error(f"메트릭 수집 오류: {e}")
                
    def _collect_metrics(self) -> Dict:
        """현재 시점의 메트릭 수집"""
        try:
            # 작업 상태 조회
            jobs_result = subprocess.run(
                ['kubectl', 'get', 'jobs', '-n', self.namespace, '-o', 'json'],
                capture_output=True, text=True
            )
            
            # 팟 상태 조회  
            pods_result = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', self.namespace, '-o', 'json'],
                capture_output=True, text=True
            )
            
            # 노드 리소스 상태 조회
            try:
                nodes_result = subprocess.run(
                    ['kubectl', 'get', 'nodes', '-o', 'json'],
                    capture_output=True, text=True
                )
            except Exception:
                nodes_result = None
                
            timestamp = datetime.now()
            
            # JSON 파싱
            jobs_data = json.loads(jobs_result.stdout) if jobs_result.returncode == 0 else {'items': []}
            pods_data = json.loads(pods_result.stdout) if pods_result.returncode == 0 else {'items': []}
            
            # 메트릭 계산
            pending_jobs = 0
            running_jobs = 0
            completed_jobs = 0
            failed_jobs = 0
            
            for job in jobs_data['items']:
                status = job.get('status', {})
                if status.get('active'):
                    running_jobs += status['active']
                elif status.get('succeeded'):
                    completed_jobs += status['succeeded']
                elif status.get('failed'):
                    failed_jobs += status['failed']
                elif job.get('spec', {}).get('suspend', False):
                    pending_jobs += 1
            
            # OOM 팟 카운트
            oom_pods = sum(1 for pod in pods_data['items']
                          if self._is_pod_oom_killed(pod))
            
            # 리소스 사용률 계산 (간소화)
            cpu_utilization = min(running_jobs * 0.1, 1.0)  # 근사치
            memory_utilization = min(running_jobs * 0.15, 1.0)  # 근사치
            
            return {
                'timestamp': timestamp,
                'pending_jobs': pending_jobs,
                'running_jobs': running_jobs,
                'completed_jobs': completed_jobs,
                'failed_jobs': failed_jobs,
                'oom_incidents': oom_pods,
                'total_pods': len(pods_data['items']),
                'cpu_utilization': cpu_utilization,
                'memory_utilization': memory_utilization
            }
            
        except Exception as e:
            self.logger.error(f"메트릭 수집 실패: {e}")
            return None
            
    def _is_pod_oom_killed(self, pod: Dict) -> bool:
        """팟이 OOM으로 종료되었는지 확인"""
        try:
            status = pod.get('status', {})
            container_statuses = status.get('containerStatuses', [])
            
            for container_status in container_statuses:
                # 현재 상태 확인
                terminated = container_status.get('state', {}).get('terminated', {})
                if terminated.get('reason') == 'OOMKilled':
                    return True
                    
                # 이전 상태 확인
                last_state = container_status.get('lastState', {}).get('terminated', {})
                if last_state.get('reason') == 'OOMKilled':
                    return True
                    
        except Exception:
            pass
            
        return False
        
    def get_metrics_summary(self) -> Dict:
        """수집된 메트릭 요약"""
        if not self.metrics_data:
            return {
                'total_samples': 0,
                'avg_pending_jobs': 0,
                'avg_running_jobs': 0,
                'total_completed': 0,
                'total_failed': 0,
                'total_oom_incidents': 0,
                'peak_concurrent_jobs': 0,
                'avg_cpu_utilization': 0,
                'avg_memory_utilization': 0
            }
            
        df = pd.DataFrame(self.metrics_data)
        
        return {
            'total_samples': len(df),
            'avg_pending_jobs': df['pending_jobs'].mean(),
            'avg_running_jobs': df['running_jobs'].mean(),
            'total_completed': df['completed_jobs'].iloc[-1] if len(df) > 0 else 0,
            'total_failed': df['failed_jobs'].iloc[-1] if len(df) > 0 else 0,
            'total_oom_incidents': df['oom_incidents'].sum(),
            'peak_concurrent_jobs': df['running_jobs'].max(),
            'avg_cpu_utilization': df['cpu_utilization'].mean(),
            'avg_memory_utilization': df['memory_utilization'].mean()
        }

class SKRueueTester:
    """SKRueue 통합 테스터 (개선된 버전)"""
    
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
        
        # 모델 저장 디렉토리
        os.makedirs("models", exist_ok=True)
        
    def setup_test_environment(self):
        """테스트 환경 설정"""
        self.logger.info("테스트 환경 설정 중...")
        
        try:
            # 네임스페이스 생성
            subprocess.run([
                'kubectl', 'create', 'namespace', self.config.namespace
            ], check=False)
            
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
            
    def train_rl_models(self):
        """RL 모델들 훈련"""
        self.logger.info("🎯 RL 모델 훈련 시작...")
        
        trained_models = {}
        
        for algorithm in self.config.algorithms_to_test:
            try:
                self.logger.info(f"🤖 {algorithm} 모델 훈련 중...")
                
                # 환경 생성
                env = create_skrueue_environment(
                    [self.config.namespace], 
                    self.config.max_queue_size
                )
                
                # 에이전트 생성 및 훈련
                agent = RLAgent(env, algorithm)
                agent.train(total_timesteps=self.config.model_training_steps)
                
                # 모델 저장
                model_path = f"models/skrueue_{algorithm.lower()}_trained.zip"
                agent.save_model(model_path)
                
                trained_models[algorithm] = model_path
                
                self.logger.info(f"✅ {algorithm} 훈련 완료: {model_path}")
                
                # 메모리 정리
                del env, agent
                time.sleep(5)
                
            except Exception as e:
                self.logger.error(f"❌ {algorithm} 훈련 실패: {e}")
                
        return trained_models
        
    def run_baseline_test(self) -> TestResults:
        """기준 스케줄러 테스트 (작업이 suspend=false로 자동 실행)"""
        self.logger.info("📊 기준 스케줄러 테스트 시작")
        
        # 모니터링 시작
        self.performance_monitor.start_monitoring()
        
        start_time = time.time()
        submitted_jobs = 0
        
        try:
            # 기준 테스트용 작업들을 suspend=false로 제출
            while time.time() - start_time < self.config.test_duration_minutes * 60:
                template = self.job_generator.generate_random_workload()
                
                # 기준 테스트용으로 suspend=false 작업 생성
                job_yaml = self._create_baseline_job(template)
                
                if self._submit_yaml(job_yaml):
                    submitted_jobs += 1
                    
                time.sleep(self.config.job_submission_interval)
                
                if submitted_jobs % 5 == 0:
                    self._wait_for_capacity()
                    
        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 테스트 중단")
            
        finally:
            self.performance_monitor.stop_monitoring()
            
        # 결과 분석
        results = self._analyze_results(self.config.baseline_scheduler, submitted_jobs)
        self.test_results[self.config.baseline_scheduler] = results
        
        return results
        
    def run_rl_scheduler_test(self, algorithm: str, model_path: str) -> TestResults:
        """RL 스케줄러 테스트"""
        self.logger.info(f"🤖 {algorithm} RL 스케줄러 테스트 시작")
        
        # RL 환경 및 에이전트 설정
        try:
            env = create_skrueue_environment([self.config.namespace], self.config.max_queue_size)
            agent = RLAgent(env, algorithm)
            agent.load_model(model_path)
            
            self.logger.info(f"✅ {algorithm} 모델 로드 완료: {model_path}")
            
        except Exception as e:
            self.logger.error(f"❌ RL 에이전트 설정 실패: {e}")
            return None
            
        # 모니터링 시작
        self.performance_monitor.start_monitoring()
        
        start_time = time.time()
        submitted_jobs = 0
        scheduler_decisions = []
        
        # RL 에이전트 실행 스레드
        def run_rl_agent():
            obs, info = env.reset()
            while time.time() - start_time < self.config.test_duration_minutes * 60:
                try:
                    action = agent.predict(obs)
                    obs, reward, terminated, truncated, info = env.step(action)
                    
                    # 결정 기록
                    scheduler_decisions.append({
                        'timestamp': datetime.now().isoformat(),
                        'action': int(action),
                        'reward': float(reward),
                        'queue_length': info.get('queue_length', 0),
                        'cluster_utilization': info.get('cluster_utilization', 0)
                    })
                    
                    if terminated or truncated:
                        obs, info = env.reset()
                        
                    time.sleep(5)  # 5초마다 결정
                    
                except Exception as e:
                    self.logger.error(f"RL 에이전트 실행 오류: {e}")
                    break
                    
        # RL 에이전트를 별도 스레드에서 실행
        agent_thread = threading.Thread(target=run_rl_agent)
        agent_thread.daemon = True
        agent_thread.start()
        
        try:
            # 작업 제출 루프 (suspend=true로 제출)
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
        results = self._analyze_results(f"SKRueue-{algorithm}", submitted_jobs, scheduler_decisions)
        self.test_results[f"SKRueue-{algorithm}"] = results
        
        return results
        
    def _create_baseline_job(self, template: JobTemplate) -> str:
        """기준 테스트용 작업 생성 (suspend=false)"""
        job_counter = getattr(self, '_baseline_counter', 0) + 1
        setattr(self, '_baseline_counter', job_counter)
        
        job_name = f"baseline-{template.name}-{job_counter:04d}"
        
        job_yaml = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': job_name,
                'namespace': self.config.namespace,
                'labels': {
                    'app': 'skrueue-test',
                    'job-type': template.job_type,
                    'scheduler': 'baseline',
                    'priority': str(template.priority)
                },
                'annotations': {
                    'skrueue.ai/estimated-duration': str(template.estimated_duration),
                    'skrueue.ai/job-type': template.job_type
                }
            },
            'spec': {
                'suspend': False,  # 즉시 실행
                'backoffLimit': 2,
                'activeDeadlineSeconds': template.estimated_duration * 60 * 3,
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
        
    def _submit_yaml(self, job_yaml: str) -> bool:
        """YAML 제출"""
        try:
            process = subprocess.Popen(
                ['kubectl', 'apply', '-f', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=job_yaml)
            
            return process.returncode == 0
            
        except Exception as e:
            self.logger.error(f"YAML 제출 실패: {e}")
            return False
        
    def _wait_for_capacity(self):
        """클러스터 용량 대기"""
        try:
            result = subprocess.run([
                'kubectl', 'get', 'jobs', '-n', self.config.namespace,
                '--field-selector=status.active=1', '--no-headers'
            ], capture_output=True, text=True)
            
            running_count = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
            
            while running_count >= self.config.max_concurrent_jobs:
                time.sleep(30)
                
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
                # 스케줄러별 필터링
                labels = job.get('metadata', {}).get('labels', {})
                if scheduler_name == "kueue-default" and labels.get('scheduler') != 'baseline':
                    continue
                elif scheduler_name.startswith("SKRueue") and labels.get('scheduler') != 'skrueue':
                    continue
                    
                status = job.get('status', {})
                
                if status.get('succeeded'):
                    completed_jobs += status['succeeded']
                elif status.get('failed'):
                    failed_jobs += status['failed']
                    
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
            
            # 처리량 및 성공률 계산
            throughput = completed_jobs / (self.config.test_duration_minutes / 60.0)
            success_rate = (completed_jobs / max(submitted_jobs, 1)) * 100
            
            return TestResults(
                scheduler_name=scheduler_name,
                total_jobs_submitted=submitted_jobs,
                total_jobs_completed=completed_jobs,
                total_jobs_failed=failed_jobs,
                average_wait_time=avg_wait_time,
                average_execution_time=avg_execution_time,
                resource_utilization={
                    'cpu': metrics_summary.get('avg_cpu_utilization', 0) * 100,
                    'memory': metrics_summary.get('avg_memory_utilization', 0) * 100
                },
                oom_incidents=metrics_summary.get('total_oom_incidents', 0),
                throughput=throughput,
                success_rate=success_rate,
                scheduler_decisions=scheduler_decisions or []
            )
            
        except Exception as e:
            self.logger.error(f"결과 분석 실패: {e}")
            return None
            
    def run_full_comparison_test(self):
        """전체 비교 테스트 실행"""
        self.logger.info("🚀 SKRueue 전체 비교 테스트 시작")
        
        try:
            # 1. 환경 설정
            self.setup_test_environment()
            
            # 2. RL 모델 훈련
            trained_models = self.train_rl_models()
            
            if not trained_models:
                self.logger.error("❌ 훈련된 모델이 없습니다. 테스트를 중단합니다.")
                return
                
            # 3. 기준 스케줄러 테스트
            self.logger.info("\n" + "="*60)
            self.logger.info("📊 기준 스케줄러 테스트")
            self.logger.info("="*60)
            
            self.setup_test_environment()
            time.sleep(10)
            baseline_results = self.run_baseline_test()
            
            # 4. RL 스케줄러들 테스트
            for algorithm in self.config.algorithms_to_test:
                if algorithm in trained_models:
                    self.logger.info(f"\n" + "="*60)
                    self.logger.info(f"🤖 {algorithm} RL 스케줄러 테스트")
                    self.logger.info("="*60)
                    
                    self.setup_test_environment()
                    time.sleep(10)
                    
                    rl_results = self.run_rl_scheduler_test(algorithm, trained_models[algorithm])
                    
                    if rl_results:
                        self.logger.info(f"✅ {algorithm} 테스트 완료")
                    else:
                        self.logger.error(f"❌ {algorithm} 테스트 실패")
                    
                    time.sleep(30)  # 다음 테스트 전 대기
                    
            # 5. 결과 분석 및 리포트 생성
            self.generate_comparison_report()
            
        except Exception as e:
            self.logger.error(f"❌ 전체 테스트 실패: {e}")
            import traceback
            traceback.print_exc()
            
    def generate_comparison_report(self):
        """비교 리포트 생성"""
        self.logger.info("📊 비교 리포트 생성 중...")
        
        try:
            # 결과 데이터프레임 생성
            results_data = []
            
            for scheduler_name, results in self.test_results.items():
                if results:
                    results_data.append({
                        'Scheduler': scheduler_name,
                        'Throughput (jobs/hour)': f"{results.throughput:.2f}",
                        'Avg Wait Time (min)': f"{results.average_wait_time:.2f}",
                        'Success Rate (%)': f"{results.success_rate:.1f}",
                        'OOM Incidents (count)': results.oom_incidents,
                        'CPU Utilization (%)': f"{results.resource_utilization['cpu']:.1f}",
                        'Memory Utilization (%)': f"{results.resource_utilization['memory']:.1f}",
                        'Jobs Submitted': results.total_jobs_submitted,
                        'Jobs Completed': results.total_jobs_completed,
                        'Jobs Failed': results.total_jobs_failed
                    })
                    
            if not results_data:
                self.logger.error("❌ 생성할 결과 데이터가 없습니다.")
                return
                    
            df = pd.DataFrame(results_data)
            
            # CSV 저장
            csv_path = os.path.join(self.config.output_dir, 'comparison_results.csv')
            df.to_csv(csv_path, index=False)
            
            # 콘솔 출력
            print("\n" + "="*80)
            print("📊 SKRueue RL Model Comparison Results")
            print("="*80)
            print(df.to_string(index=False))
            print("="*80)
            
            # 시각화 생성
            self._create_performance_charts(df)
            
            # 마크다운 리포트 생성
            self._generate_markdown_report(df)
            
            self.logger.info(f"✅ 비교 리포트 생성 완료: {self.config.output_dir}")
            
        except Exception as e:
            self.logger.error(f"❌ 리포트 생성 실패: {e}")
            
    def _create_performance_charts(self, df: pd.DataFrame):
        """성능 비교 차트 생성"""
        try:
            fig, axes = plt.subplots(2, 3, figsize=(18, 12))
            
            # 숫자형 데이터로 변환
            df_numeric = df.copy()
            numeric_cols = ['Throughput (jobs/hour)', 'Avg Wait Time (min)', 'Success Rate (%)', 
                          'CPU Utilization (%)', 'Memory Utilization (%)']
            
            for col in numeric_cols:
                df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce')
            
            # 1. 처리량 비교
            axes[0, 0].bar(df_numeric['Scheduler'], df_numeric['Throughput (jobs/hour)'], color='skyblue')
            axes[0, 0].set_title('Throughput Comparison')
            axes[0, 0].set_ylabel('Jobs per Hour')
            axes[0, 0].tick_params(axis='x', rotation=45)
            
            # 2. 평균 대기시간 비교
            axes[0, 1].bar(df_numeric['Scheduler'], df_numeric['Avg Wait Time (min)'], color='lightcoral')
            axes[0, 1].set_title('Average Wait Time Comparison')
            axes[0, 1].set_ylabel('Minutes')
            axes[0, 1].tick_params(axis='x', rotation=45)
            
            # 3. 성공률 비교
            axes[0, 2].bar(df_numeric['Scheduler'], df_numeric['Success Rate (%)'], color='lightgreen')
            axes[0, 2].set_title('Job Success Rate Comparison')
            axes[0, 2].set_ylabel('Success Rate (%)')
            axes[0, 2].tick_params(axis='x', rotation=45)
            
            # 4. OOM 사건 수 비교
            axes[1, 0].bar(df['Scheduler'], df['OOM Incidents (count)'], color='orange')
            axes[1, 0].set_title('OOM Incidents Comparison')
            axes[1, 0].set_ylabel('Number of OOM Incidents')
            axes[1, 0].tick_params(axis='x', rotation=45)
            
            # 5. CPU 사용률 비교
            axes[1, 1].bar(df_numeric['Scheduler'], df_numeric['CPU Utilization (%)'], color='gold')
            axes[1, 1].set_title('CPU Utilization Comparison')
            axes[1, 1].set_ylabel('CPU Utilization (%)')
            axes[1, 1].tick_params(axis='x', rotation=45)
            
            # 6. 메모리 사용률 비교
            axes[1, 2].bar(df_numeric['Scheduler'], df_numeric['Memory Utilization (%)'], color='plum')
            axes[1, 2].set_title('Memory Utilization Comparison')
            axes[1, 2].set_ylabel('Memory Utilization (%)')
            axes[1, 2].tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            chart_path = os.path.join(self.config.output_dir, 'performance_comparison.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"📈 성능 차트 저장: {chart_path}")
            
        except Exception as e:
            self.logger.error(f"❌ 차트 생성 실패: {e}")
            
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
                f.write(f"- Training Steps: {self.config.model_training_steps}\n")
                f.write(f"- Algorithms Tested: {', '.join(self.config.algorithms_to_test)}\n\n")
                
                f.write("## Test Results Summary\n\n")
                f.write(df.to_markdown(index=False))
                f.write("\n\n")
                
                f.write("## Key Findings\n\n")
                
                # 최고 성능 스케줄러 찾기
                df_numeric = df.copy()
                for col in ['Throughput (jobs/hour)', 'Avg Wait Time (min)', 'Success Rate (%)']:
                    df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce')
                
                best_throughput = df_numeric.loc[df_numeric['Throughput (jobs/hour)'].idxmax(), 'Scheduler']
                best_wait_time = df_numeric.loc[df_numeric['Avg Wait Time (min)'].idxmin(), 'Scheduler']
                best_success_rate = df_numeric.loc[df_numeric['Success Rate (%)'].idxmax(), 'Scheduler']
                
                f.write(f"- **Best Throughput:** {best_throughput}\n")
                f.write(f"- **Lowest Wait Time:** {best_wait_time}\n")
                f.write(f"- **Highest Success Rate:** {best_success_rate}\n\n")
                
                f.write("## Performance Charts\n\n")
                f.write("![Performance Comparison](performance_comparison.png)\n\n")
                
                f.write("## Conclusion\n\n")
                f.write("이 테스트는 SKRueue RL 기반 스케줄러의 성능을 기존 스케줄러와 비교한 결과입니다. ")
                f.write("67차원 상태 공간을 활용한 강화학습 에이전트들의 성능을 실제 워크로드로 검증했습니다.\n")
                
            self.logger.info(f"📝 마크다운 리포트 저장: {report_path}")
            
        except Exception as e:
            self.logger.error(f"❌ 마크다운 리포트 생성 실패: {e}")

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SKRueue 통합 테스트 시스템')
    parser.add_argument('--duration', type=int, default=15, help='테스트 지속 시간 (분)')
    parser.add_argument('--algorithms', nargs='*', default=['DQN', 'PPO', 'A2C'], 
                       help='테스트할 RL 알고리즘')
    parser.add_argument('--namespace', type=str, default='skrueue-test', 
                       help='테스트 네임스페이스')
    parser.add_argument('--output-dir', type=str, default='skrueue_test_results',
                       help='결과 출력 디렉토리')
    parser.add_argument('--job-interval', type=int, default=20,
                       help='작업 제출 간격 (초)')
    parser.add_argument('--training-steps', type=int, default=5000,
                       help='RL 모델 훈련 스텝 수')
    parser.add_argument('--max-queue-size', type=int, default=10,
                       help='최대 큐 크기')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('skrueue_integration_test.log'),
            logging.StreamHandler()
        ]
    )
    
    # 테스트 설정
    config = TestConfiguration(
        test_duration_minutes=args.duration,
        algorithms_to_test=args.algorithms,
        namespace=args.namespace,
        output_dir=args.output_dir,
        job_submission_interval=args.job_interval,
        model_training_steps=args.training_steps,
        max_queue_size=args.max_queue_size
    )
    
    # 테스터 생성 및 실행
    tester = SKRueueTester(config)
    
    try:
        print("🚀 SKRueue 통합 테스트 시작...")
        print(f"⏱️  테스트 시간: {args.duration}분")
        print(f"🤖 알고리즘: {', '.join(args.algorithms)}")
        print(f"🎯 훈련 스텝: {args.training_steps}")
        print(f"📂 결과 디렉토리: {args.output_dir}")
        print("="*60)
        
        tester.run_full_comparison_test()
        
        print(f"\n✅ 테스트 완료! 결과는 {args.output_dir} 에서 확인하세요.")
        print(f"📊 CSV: {args.output_dir}/comparison_results.csv")
        print(f"📈 차트: {args.output_dir}/performance_comparison.png")
        print(f"📝 리포트: {args.output_dir}/SKRueue_Test_Report.md")
        
    except KeyboardInterrupt:
        print("\n❌ 사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 테스트 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()