# """
# SKRueue: Kueue와 RL 에이전트 통합 인터페이스
# Kubernetes Kueue 스케줄러에 강화학습을 적용하는 핵심 모듈
# """

import gym
import numpy as np
import pandas as pd
import time
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import threading
import queue

# Kubernetes 및 RL 라이브러리
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import gym
from gym import spaces
import torch
import torch.nn as nn
from stable_baselines3 import DQN, PPO, A2C
from stable_baselines3.common.env_checker import check_env

@dataclass
class JobInfo:
    """작업 정보"""
    job_id: str
    name: str
    namespace: str
    cpu_request: float
    memory_request: float  # GB
    priority: int
    arrival_time: datetime
    estimated_duration: float  # 분
    job_type: str  # 'spark', 'batch', 'ml' 등

@dataclass
class ClusterResource:
    """클러스터 리소스 정보"""
    cpu_total: float
    cpu_available: float
    memory_total: float  # GB
    memory_available: float  # GB
    running_jobs: int
    avg_cpu_utilization: float
    avg_memory_utilization: float
    recent_oom_rate: float

class KueueInterface:
    """Kueue 스케줄러와의 인터페이스"""
    
    def __init__(self, namespaces: List[str] = None):
        self.namespaces = namespaces or ['default']
        self.logger = logging.getLogger('KueueInterface')
        
        # Kubernetes 클라이언트 초기화
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
            
        self.k8s_core = client.CoreV1Api()
        self.k8s_batch = client.BatchV1Api()
        self.k8s_custom = client.CustomObjectsApi()
        
    def get_pending_jobs(self) -> List[JobInfo]:
        """대기 중인 작업 목록 조회"""
        pending_jobs = []
        
        for namespace in self.namespaces:
            try:
                # Kueue Workloads 조회
                workloads = self.k8s_custom.list_namespaced_custom_object(
                    group="kueue.x-k8s.io",
                    version="v1beta1",
                    namespace=namespace,
                    plural="workloads"
                )
                
                for workload in workloads.get('items', []):
                    if self._is_workload_pending(workload):
                        job_info = self._extract_job_info(workload, namespace)
                        if job_info:
                            pending_jobs.append(job_info)
                            
            except ApiException as e:
                if e.status != 404:
                    self.logger.warning(f"Workload 조회 실패 (namespace: {namespace}): {e}")
                    
        return pending_jobs
        
    def get_cluster_resources(self) -> ClusterResource:
        """현재 클러스터 리소스 상태 조회"""
        try:
            nodes = self.k8s_core.list_node()
            
            total_cpu = total_memory = 0.0
            available_cpu = available_memory = 0.0
            
            for node in nodes.items:
                capacity = node.status.capacity
                allocatable = node.status.allocatable
                
                total_cpu += self._parse_cpu(capacity.get('cpu', '0'))
                total_memory += self._parse_memory(capacity.get('memory', '0'))
                available_cpu += self._parse_cpu(allocatable.get('cpu', '0'))
                available_memory += self._parse_memory(allocatable.get('memory', '0'))
                
            # 실행 중인 작업 수 계산
            running_jobs = self._count_running_jobs()
            
            # 사용률 계산
            cpu_used = total_cpu - available_cpu
            memory_used = total_memory - available_memory
            
            avg_cpu_util = cpu_used / total_cpu if total_cpu > 0 else 0
            avg_memory_util = memory_used / total_memory if total_memory > 0 else 0
            
            # 최근 OOM 비율 계산 (지난 1시간)
            recent_oom_rate = self._calculate_recent_oom_rate()
            
            return ClusterResource(
                cpu_total=total_cpu,
                cpu_available=available_cpu,
                memory_total=total_memory,
                memory_available=available_memory,
                running_jobs=running_jobs,
                avg_cpu_utilization=avg_cpu_util,
                avg_memory_utilization=avg_memory_util,
                recent_oom_rate=recent_oom_rate
            )
            
        except Exception as e:
            self.logger.error(f"클러스터 리소스 조회 실패: {e}")
            return None
            
    def admit_job(self, job_info: JobInfo) -> bool:
        """작업을 승인하여 스케줄링 시작"""
        try:
            # Workload의 suspend 상태를 false로 변경
            patch_body = {
                "spec": {
                    "suspend": False
                }
            }
            
            # Workload 이름으로 패치 실행
            result = self.k8s_custom.patch_namespaced_custom_object(
                group="kueue.x-k8s.io",
                version="v1beta1",
                namespace=job_info.namespace,
                plural="workloads",
                name=job_info.name,
                body=patch_body
            )
            
            self.logger.info(f"작업 승인 성공: {job_info.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"작업 승인 실패 {job_info.name}: {e}")
            return False
            
    def _is_workload_pending(self, workload: Dict) -> bool:
        """Workload가 대기 상태인지 확인"""
        spec = workload.get('spec', {})
        status = workload.get('status', {})
        
        # suspend가 True이거나 admitted가 False인 경우 대기 중
        return (spec.get('suspend', False) or 
                not status.get('admitted', False))
                
    def _extract_job_info(self, workload: Dict, namespace: str) -> Optional[JobInfo]:
        """Workload에서 JobInfo 추출"""
        try:
            metadata = workload.get('metadata', {})
            spec = workload.get('spec', {})
            
            # 기본 정보
            job_id = metadata.get('uid', '')
            name = metadata.get('name', '')
            
            # 리소스 요구사항
            pod_sets = spec.get('podSets', [])
            if not pod_sets:
                return None
                
            # 첫 번째 podSet에서 리소스 추출
            pod_set = pod_sets[0]
            template = pod_set.get('template', {}).get('spec', {})
            containers = template.get('containers', [])
            
            if not containers:
                return None
                
            container = containers[0]
            resources = container.get('resources', {})
            requests = resources.get('requests', {})
            
            cpu_request = self._parse_cpu(requests.get('cpu', '0'))
            memory_request = self._parse_memory(requests.get('memory', '0'))
            
            # 우선순위 추출
            priority = spec.get('priority', 0)
            
            # 도착 시간
            creation_timestamp = metadata.get('creationTimestamp', '')
            arrival_time = datetime.fromisoformat(creation_timestamp.replace('Z', '+00:00')) if creation_timestamp else datetime.now()
            
            # 예상 실행 시간 (annotation이나 레이블에서 추출, 없으면 기본값)
            annotations = metadata.get('annotations', {})
            estimated_duration = float(annotations.get('skrueue.ai/estimated-duration', '30'))  # 기본 30분
            
            # 작업 타입 추정
            job_type = self._estimate_job_type(workload)
            
            return JobInfo(
                job_id=job_id,
                name=name,
                namespace=namespace,
                cpu_request=cpu_request,
                memory_request=memory_request,
                priority=priority,
                arrival_time=arrival_time,
                estimated_duration=estimated_duration,
                job_type=job_type
            )
            
        except Exception as e:
            self.logger.error(f"JobInfo 추출 실패: {e}")
            return None
            
    def _parse_cpu(self, cpu_str: str) -> float:
        """CPU 문자열 파싱"""
        if not cpu_str:
            return 0.0
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000.0
        return float(cpu_str)
        
    def _parse_memory(self, memory_str: str) -> float:
        """메모리 문자열을 GB로 파싱"""
        if not memory_str:
            return 0.0
            
        units = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4}
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                value = float(memory_str[:-len(unit)])
                return (value * multiplier) / (1024**3)
                
        return float(memory_str) / (1024**3)
        
    def _count_running_jobs(self) -> int:
        """실행 중인 작업 수 계산"""
        running_count = 0
        
        for namespace in self.namespaces:
            try:
                jobs = self.k8s_batch.list_namespaced_job(namespace=namespace)
                for job in jobs.items:
                    if job.status and job.status.active:
                        running_count += 1
            except Exception:
                pass
                
        return running_count
        
    def _calculate_recent_oom_rate(self) -> float:
        """최근 1시간 OOM 비율 계산"""
        try:
            # 최근 이벤트에서 OOM 관련 이벤트 조회
            cutoff_time = datetime.now() - timedelta(hours=1)
            oom_count = 0
            total_events = 0
            
            for namespace in self.namespaces:
                events = self.k8s_core.list_namespaced_event(namespace=namespace)
                
                for event in events.items:
                    event_time = event.first_timestamp or event.event_time
                    if event_time and event_time > cutoff_time:
                        total_events += 1
                        if 'OOM' in event.reason or 'OutOfMemory' in event.message:
                            oom_count += 1
                            
            return oom_count / max(total_events, 1)
            
        except Exception:
            return 0.0
            
    def _estimate_job_type(self, workload: Dict) -> str:
        """작업 타입 추정"""
        metadata = workload.get('metadata', {})
        labels = metadata.get('labels', {})
        annotations = metadata.get('annotations', {})
        
        # 레이블이나 어노테이션에서 타입 정보 추출
        for key, value in {**labels, **annotations}.items():
            if 'spark' in key.lower() or 'spark' in str(value).lower():
                return 'spark'
            elif 'ml' in key.lower() or 'machine-learning' in str(value).lower():
                return 'ml'
            elif 'etl' in key.lower() or 'batch' in str(value).lower():
                return 'batch'
                
        return 'generic'

class SKRueueEnvironment(gym.Env):
    """SKRueue RL 환경"""
    
    def __init__(self, kueue_interface: KueueInterface, max_queue_size: int = 50):
        super(SKRueueEnvironment, self).__init__()
        
        self.kueue = kueue_interface
        self.max_queue_size = max_queue_size
        self.logger = logging.getLogger('SKRueueEnvironment')
        
        # 상태 공간 정의
        # [클러스터 리소스(4) + 히스토리(3) + 작업 큐(max_queue_size * 6)]
        self.state_dim = 4 + 3 + (max_queue_size * 6)
        self.observation_space = spaces.Box(
            low=0.0, high=1.0, shape=(self.state_dim,), dtype=np.float32
        )
        
        # 행동 공간 정의 (대기열 인덱스 + wait 액션)
        self.action_space = spaces.Discrete(max_queue_size + 1)
        
        # 환경 상태
        self.current_jobs: List[JobInfo] = []
        self.current_resources: Optional[ClusterResource] = None
        self.step_count = 0
        self.episode_start_time = time.time()
        
        # 보상 계산용 히스토리
        self.completed_jobs_history = []
        self.oom_history = []
        self.wait_time_history = []
        
        # 보상 함수 가중치
        self.alpha = 0.4  # 처리량 가중치
        self.beta = 0.3   # 효율성 가중치  
        self.gamma = 0.3  # 페널티 가중치
        self.lambda_oom = 10.0    # OOM 페널티
        self.lambda_wait = 5.0    # 대기시간 페널티
        
    def reset(self) -> np.ndarray:
        """환경 초기화"""
        self.step_count = 0
        self.episode_start_time = time.time()
        self.completed_jobs_history.clear()
        self.oom_history.clear()
        self.wait_time_history.clear()
        
        # 현재 상태 갱신
        self._update_state()
        
        return self._get_observation()
        
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, Dict]:
        """환경 스텝 실행"""
        # 이전 상태 저장 (보상 계산용)
        prev_resources = self.current_resources
        prev_jobs_count = len(self.current_jobs)
        
        # 액션 실행
        action_success = self._execute_action(action)
        
        # 상태 갱신
        self._update_state()
        
        # 보상 계산
        reward = self._calculate_reward(prev_resources, prev_jobs_count, action_success)
        
        # 에피소드 종료 조건
        done = self._is_episode_done()
        
        # 정보 구성
        info = {
            'step_count': self.step_count,
            'queue_length': len(self.current_jobs),
            'action_success': action_success,
            'cluster_utilization': self._get_cluster_utilization(),
            'recent_oom_rate': self.current_resources.recent_oom_rate if self.current_resources else 0
        }
        
        self.step_count += 1
        
        return self._get_observation(), reward, done, info
        
    def _update_state(self):
        """현재 클러스터 및 작업 상태 갱신"""
        self.current_jobs = self.kueue.get_pending_jobs()
        self.current_resources = self.kueue.get_cluster_resources()
        
    def _get_observation(self) -> np.ndarray:
        """현재 상태를 observation으로 변환"""
        obs = np.zeros(self.state_dim, dtype=np.float32)
        
        if self.current_resources:
            # 클러스터 리소스 정보 (정규화)
            obs[0] = self.current_resources.cpu_available / max(self.current_resources.cpu_total, 1)
            obs[1] = self.current_resources.memory_available / max(self.current_resources.memory_total, 1)
            obs[2] = self.current_resources.avg_cpu_utilization
            obs[3] = self.current_resources.avg_memory_utilization
            
            # 히스토리 정보
            obs[4] = min(self.current_resources.running_jobs / 100.0, 1.0)  # 정규화
            obs[5] = self.current_resources.avg_cpu_utilization
            obs[6] = self.current_resources.recent_oom_rate
            
        # 작업 큐 정보
        queue_start_idx = 7
        for i, job in enumerate(self.current_jobs[:self.max_queue_size]):
            base_idx = queue_start_idx + (i * 6)
            
            # 작업 리소스 요구사항 (정규화)
            max_cpu = self.current_resources.cpu_total if self.current_resources else 100
            max_memory = self.current_resources.memory_total if self.current_resources else 100
            
            obs[base_idx] = min(job.cpu_request / max_cpu, 1.0)
            obs[base_idx + 1] = min(job.memory_request / max_memory, 1.0)
            obs[base_idx + 2] = min(job.priority / 10.0, 1.0)  # 우선순위 정규화
            
            # 대기 시간 (분 단위, 정규화)
            wait_time = (datetime.now() - job.arrival_time).total_seconds() / 60.0
            obs[base_idx + 3] = min(wait_time / 60.0, 1.0)  # 최대 1시간으로 정규화
            
            # 예상 실행 시간 (정규화)
            obs[base_idx + 4] = min(job.estimated_duration / 120.0, 1.0)  # 최대 2시간으로 정규화
            
            # 작업 타입 (원-핫 인코딩 단순화)
            job_type_map = {'spark': 0.8, 'ml': 0.6, 'batch': 0.4, 'generic': 0.2}
            obs[base_idx + 5] = job_type_map.get(job.job_type, 0.2)
            
        return obs
        
    def _execute_action(self, action: int) -> bool:
        """선택된 액션 실행"""
        if action >= len(self.current_jobs):
            # Wait 액션 또는 잘못된 액션
            return True
            
        # 선택된 작업 승인
        selected_job = self.current_jobs[action]
        success = self.kueue.admit_job(selected_job)
        
        if success:
            self.logger.info(f"작업 승인: {selected_job.name}")
            
        return success
        
    def _calculate_reward(self, prev_resources: ClusterResource, prev_jobs_count: int, action_success: bool) -> float:
        """보상 함수 계산"""
        if not self.current_resources:
            return -1.0  # 상태 조회 실패 페널티
            
        # 1. 처리량 보상 (R_throughput)
        jobs_processed = max(0, prev_jobs_count - len(self.current_jobs))
        max_possible = min(prev_jobs_count, 10)  # 한 스텝에서 최대 처리 가능한 작업 수
        throughput_reward = jobs_processed / max(max_possible, 1)
        
        # 2. 효율성 보상 (R_efficiency)
        cpu_efficiency = self.current_resources.avg_cpu_utilization
        memory_efficiency = self.current_resources.avg_memory_utilization
        efficiency_reward = (cpu_efficiency + memory_efficiency) / 2.0
        
        # 3. 페널티 계산
        penalty = 0.0
        
        # OOM 페널티
        oom_penalty = self.lambda_oom * self.current_resources.recent_oom_rate
        
        # 대기시간 페널티 (현재 큐의 평균 대기시간)
        if self.current_jobs:
            avg_wait_time = np.mean([
                (datetime.now() - job.arrival_time).total_seconds() / 60.0 
                for job in self.current_jobs
            ])
            wait_penalty = self.lambda_wait * min(avg_wait_time / 60.0, 1.0)  # 1시간 기준 정규화
        else:
            wait_penalty = 0.0
            
        penalty = oom_penalty + wait_penalty
        
        # 최종 보상 계산
        reward = (self.alpha * throughput_reward + 
                 self.beta * efficiency_reward - 
                 self.gamma * penalty)
        
        # 액션 실패 페널티
        if not action_success:
            reward -= 0.1
            
        return reward
        
    def _get_cluster_utilization(self) -> float:
        """클러스터 전체 사용률 계산"""
        if not self.current_resources:
            return 0.0
            
        cpu_util = self.current_resources.avg_cpu_utilization
        memory_util = self.current_resources.avg_memory_utilization
        
        return (cpu_util + memory_util) / 2.0
        
    def _is_episode_done(self) -> bool:
        """에피소드 종료 조건 확인"""
        # 1. 최대 스텝 수 도달
        if self.step_count >= 1000:
            return True
            
        # 2. 큐가 비어있고 일정 시간 경과
        if len(self.current_jobs) == 0 and self.step_count > 50:
            return True
            
        # 3. 클러스터 상태 조회 실패
        if self.current_resources is None:
            return True
            
        return False

class RLAgent:
    """SKRueue RL 에이전트"""
    
    def __init__(self, env: SKRueueEnvironment, algorithm: str = 'DQN'):
        self.env = env
        self.algorithm = algorithm
        self.logger = logging.getLogger('RLAgent')
        
        # 모델 초기화
        if algorithm == 'DQN':
            self.model = DQN(
                'MlpPolicy', 
                env, 
                learning_rate=1e-4,
                buffer_size=50000,
                learning_starts=1000,
                target_update_interval=500,
                train_freq=4,
                verbose=1
            )
        elif algorithm == 'PPO':
            self.model = PPO(
                'MlpPolicy', 
                env, 
                learning_rate=3e-4,
                n_steps=2048,
                batch_size=64,
                verbose=1
            )
        elif algorithm == 'A2C':
            self.model = A2C(
                'MlpPolicy', 
                env, 
                learning_rate=7e-4,
                n_steps=5,
                verbose=1
            )
        else:
            raise ValueError(f"지원하지 않는 알고리즘: {algorithm}")
            
    def train(self, total_timesteps: int = 100000):
        """모델 훈련"""
        self.logger.info(f"{self.algorithm} 훈련 시작 (총 {total_timesteps} 스텝)")
        
        try:
            self.model.learn(total_timesteps=total_timesteps)
            self.logger.info("훈련 완료")
        except Exception as e:
            self.logger.error(f"훈련 실패: {e}")
            
    def save_model(self, path: str):
        """모델 저장"""
        self.model.save(path)
        self.logger.info(f"모델 저장 완료: {path}")
        
    def load_model(self, path: str):
        """모델 로드"""
        if self.algorithm == 'DQN':
            self.model = DQN.load(path, env=self.env)
        elif self.algorithm == 'PPO':
            self.model = PPO.load(path, env=self.env)
        elif self.algorithm == 'A2C':
            self.model = A2C.load(path, env=self.env)
            
        self.logger.info(f"모델 로드 완료: {path}")
        
    def predict(self, observation: np.ndarray) -> int:
        """액션 예측"""
        action, _ = self.model.predict(observation, deterministic=True)
        return int(action)
        
    def evaluate(self, num_episodes: int = 10) -> Dict[str, float]:
        """모델 평가"""
        total_rewards = []
        episode_lengths = []
        success_rates = []
        
        for episode in range(num_episodes):
            obs = self.env.reset()
            total_reward = 0
            steps = 0
            successful_actions = 0
            total_actions = 0
            
            done = False
            while not done:
                action = self.predict(obs)
                obs, reward, done, info = self.env.step(action)
                
                total_reward += reward
                steps += 1
                total_actions += 1
                
                if info.get('action_success', False):
                    successful_actions += 1
                    
            total_rewards.append(total_reward)
            episode_lengths.append(steps)
            success_rates.append(successful_actions / max(total_actions, 1))
            
        return {
            'mean_reward': np.mean(total_rewards),
            'std_reward': np.std(total_rewards),
            'mean_episode_length': np.mean(episode_lengths),
            'mean_success_rate': np.mean(success_rates)
        }

def create_skrueue_environment(namespaces: List[str] = None) -> SKRueueEnvironment:
    """SKRueue 환경 생성 헬퍼 함수"""
    kueue_interface = KueueInterface(namespaces)
    env = SKRueueEnvironment(kueue_interface)
    
    # 환경 검증
    check_env(env)
    
    return env

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SKRueue RL 시스템')
    parser.add_argument('--mode', choices=['train', 'eval', 'inference'], required=True)
    parser.add_argument('--algorithm', choices=['DQN', 'PPO', 'A2C'], default='DQN')
    parser.add_argument('--model-path', type=str, help='모델 파일 경로')
    parser.add_argument('--timesteps', type=int, default=100000, help='훈련 스텝 수')
    parser.add_argument('--namespaces', nargs='*', default=['default'], help='모니터링 네임스페이스')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(level=logging.INFO)
    
    # 환경 생성
    env = create_skrueue_environment(args.namespaces)
    agent = RLAgent(env, args.algorithm)
    
    if args.mode == 'train':
        # 훈련 모드
        agent.train(args.timesteps)
        if args.model_path:
            agent.save_model(args.model_path)
            
    elif args.mode == 'eval':
        # 평가 모드
        if args.model_path:
            agent.load_model(args.model_path)
        
        results = agent.evaluate()
        print(f"평가 결과: {results}")
        
    elif args.mode == 'inference':
        # 추론 모드 (실제 운용)
        if not args.model_path:
            raise ValueError("추론 모드에서는 모델 경로가 필요합니다")
            
        agent.load_model(args.model_path)
        
        print("SKRueue 에이전트 실행 중...")
        obs = env.reset()
        
        try:
            while True:
                action = agent.predict(obs)
                obs, reward, done, info = env.step(action)
                
                print(f"Step: {info['step_count']}, Action: {action}, "
                      f"Reward: {reward:.3f}, Queue: {info['queue_length']}")
                
                if done:
                    obs = env.reset()
                    
                time.sleep(10)  # 10초 간격으로 실행
                
        except KeyboardInterrupt:
            print("사용자에 의해 중단됨")

if __name__ == "__main__":
    main()