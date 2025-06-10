# skrueue.py (수정된 버전)
# SKRueue: Kueue와 RL 에이전트 통합 인터페이스
# Kubernetes Kueue 스케줄러에 강화학습을 적용하는 핵심 모듈

import os
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

# Kubernetes 라이브러리
try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException
except ImportError:
    print("❌ kubernetes 패키지가 설치되지 않았습니다: pip install kubernetes")
    exit(1)

# Gym/Gymnasium 호환성 처리
try:
    import gymnasium as gym
    from gymnasium import spaces
    print("✅ Gymnasium 사용")
except ImportError:
    try:
        import gym
        from gym import spaces
        print("✅ 구 Gym 버전 사용")
    except ImportError:
        print("❌ gym 또는 gymnasium 패키지가 필요합니다: pip install gymnasium")
        exit(1)

# RL 라이브러리
try:
    import torch
    import torch.nn as nn
    from stable_baselines3 import DQN, PPO, A2C
    from stable_baselines3.common.env_checker import check_env
    print("✅ Stable-Baselines3 사용 가능")
except ImportError:
    print("❌ 필요한 RL 라이브러리가 설치되지 않았습니다:")
    print("pip install stable-baselines3[extra] torch")
    exit(1)

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
    """Kueue 스케줄러와의 인터페이스 (개선된 버전)"""
    
    def __init__(self, namespaces: List[str] = None):
        self.namespaces = namespaces or ['default']
        self.logger = logging.getLogger('KueueInterface')
        
        # Kubernetes 클라이언트 초기화
        try:
            config.load_incluster_config()
            self.logger.info("✅ In-cluster 설정 로드 성공")
        except:
            try:
                config.load_kube_config()
                self.logger.info("✅ 로컬 kubeconfig 로드 성공")
            except Exception as e:
                self.logger.error(f"❌ Kubernetes 설정 로드 실패: {e}")
                raise
            
        self.k8s_core = client.CoreV1Api()
        self.k8s_batch = client.BatchV1Api()
        self.k8s_custom = client.CustomObjectsApi()
        
    def get_pending_jobs(self) -> List[JobInfo]:
        """대기 중인 작업 목록 조회 (suspend=true 작업 포함)"""
        pending_jobs = []
        
        for namespace in self.namespaces:
            try:
                jobs = self.k8s_batch.list_namespaced_job(namespace=namespace)
                
                for job in jobs.items:
                    # 핵심 수정: suspend=true인 작업만 선택
                    if job.spec and job.spec.suspend == True:
                        job_info = self._extract_job_info_from_k8s_job(job, namespace)
                        if job_info:
                            pending_jobs.append(job_info)
                            
            except Exception as e:
                self.logger.error(f"Jobs 조회 실패 (namespace: {namespace}): {e}")
        
        self.logger.info(f"Found {len(pending_jobs)} suspended jobs")
        return pending_jobs
        
    def get_cluster_resources(self) -> ClusterResource:
        """현재 클러스터 리소스 상태 조회"""
        try:
            nodes = self.k8s_core.list_node()
            
            total_cpu = total_memory = 0.0
            available_cpu = available_memory = 0.0
            
            for node in nodes.items:
                # 스케줄 가능한 노드만 계산
                if self._is_node_schedulable(node):
                    capacity = node.status.capacity
                    allocatable = node.status.allocatable
                    
                    total_cpu += self._parse_cpu(capacity.get('cpu', '0'))
                    total_memory += self._parse_memory(capacity.get('memory', '0'))
                    available_cpu += self._parse_cpu(allocatable.get('cpu', '0'))
                    available_memory += self._parse_memory(allocatable.get('memory', '0'))
                
            # 실행 중인 작업 수 계산
            running_jobs = self._count_running_jobs()
            
            # 사용률 계산
            cpu_used = max(0, total_cpu - available_cpu)
            memory_used = max(0, total_memory - available_memory)
            
            avg_cpu_util = cpu_used / max(total_cpu, 1)
            avg_memory_util = memory_used / max(total_memory, 1)
            
            # 최근 OOM 비율 계산
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
            return ClusterResource(
                cpu_total=1.0, cpu_available=1.0, memory_total=1.0, 
                memory_available=1.0, running_jobs=0, avg_cpu_utilization=0.0,
                avg_memory_utilization=0.0, recent_oom_rate=0.0
            )
            
    def admit_job(self, job_info: JobInfo) -> bool:
        """작업을 승인하여 스케줄링 시작"""
        try:
            # Kubernetes Job의 suspend 상태를 false로 변경
            patch_body = {"spec": {"suspend": False}}
            
            result = self.k8s_batch.patch_namespaced_job(
                name=job_info.name,
                namespace=job_info.namespace,
                body=patch_body
            )
            
            self.logger.info(f"작업 승인 성공: {job_info.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"작업 승인 실패 {job_info.name}: {e}")
            
            # Workload 패치 시도
            try:
                patch_body = {"spec": {"suspend": False}}
                
                result = self.k8s_custom.patch_namespaced_custom_object(
                    group="kueue.x-k8s.io",
                    version="v1beta1",
                    namespace=job_info.namespace,
                    plural="workloads",
                    name=job_info.name,
                    body=patch_body
                )
                
                self.logger.info(f"Workload 승인 성공: {job_info.name}")
                return True
                
            except Exception as e2:
                self.logger.error(f"Workload 승인도 실패 {job_info.name}: {e2}")
                return False
            
    def _extract_job_info_from_k8s_job(self, job, namespace: str) -> Optional[JobInfo]:
        """Kubernetes Job에서 JobInfo 추출"""
        try:
            metadata = job.metadata
            spec = job.spec
            
            # 기본 정보
            job_id = metadata.uid
            name = metadata.name
            
            # 리소스 요구사항 추출
            cpu_request, memory_request, _, _ = self._extract_resources_from_job_spec(spec)
            
            # 우선순위 추출
            priority = self._extract_priority_from_labels(metadata.labels or {})
            
            # 도착 시간
            creation_timestamp = metadata.creation_timestamp
            arrival_time = creation_timestamp if creation_timestamp else datetime.now()
            
            # 예상 실행 시간
            annotations = metadata.annotations or {}
            estimated_duration = float(annotations.get('skrueue.ai/estimated-duration', '30'))
            
            # 작업 타입
            job_type = self._estimate_job_type_from_labels(metadata.labels or {})
            
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
            self.logger.error(f"K8s Job JobInfo 추출 실패: {e}")
            return None

    def _extract_job_info_from_workload(self, workload: Dict, namespace: str) -> Optional[JobInfo]:
        """Workload에서 JobInfo 추출"""
        try:
            metadata = workload.get('metadata', {})
            spec = workload.get('spec', {})
            
            # 기본 정보
            job_id = metadata.get('uid', '')
            name = metadata.get('name', '')
            
            # 리소스 요구사항 (간소화)
            pod_sets = spec.get('podSets', [])
            cpu_request = memory_request = 1.0  # 기본값
            
            if pod_sets:
                pod_set = pod_sets[0]
                template = pod_set.get('template', {}).get('spec', {})
                containers = template.get('containers', [])
                
                if containers:
                    container = containers[0]
                    resources = container.get('resources', {})
                    requests = resources.get('requests', {})
                    
                    cpu_request = self._parse_cpu(requests.get('cpu', '0'))
                    memory_request = self._parse_memory(requests.get('memory', '0'))
            
            # 우선순위 추출
            priority = spec.get('priority', 0)
            
            # 도착 시간
            creation_timestamp = metadata.get('creationTimestamp', '')
            if creation_timestamp:
                arrival_time = datetime.fromisoformat(creation_timestamp.replace('Z', '+00:00'))
            else:
                arrival_time = datetime.now()
            
            # 예상 실행 시간
            annotations = metadata.get('annotations', {})
            estimated_duration = float(annotations.get('skrueue.ai/estimated-duration', '30'))
            
            # 작업 타입
            job_type = self._estimate_job_type_from_labels(metadata.get('labels', {}))
            
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
            self.logger.error(f"Workload JobInfo 추출 실패: {e}")
            return None
            
    def _is_workload_pending(self, workload: Dict) -> bool:
        """Workload가 대기 상태인지 확인"""
        spec = workload.get('spec', {})
        status = workload.get('status', {})
        
        return (spec.get('suspend', False) or not status.get('admitted', False))
        
    def _is_node_schedulable(self, node) -> bool:
        """노드가 스케줄링 가능한지 확인"""
        # Taint 확인
        if node.spec.taints:
            for taint in node.spec.taints:
                if taint.effect in ['NoSchedule', 'NoExecute']:
                    return False
        
        # Ready 상태 확인
        if node.status.conditions:
            for condition in node.status.conditions:
                if condition.type == 'Ready' and condition.status == 'True':
                    return True
        
        return False
        
    def _extract_resources_from_job_spec(self, spec) -> Tuple[float, float, float, float]:
        """Job Spec에서 리소스 요구사항 추출"""
        cpu_request = cpu_limit = 0.0
        memory_request = memory_limit = 0.0
        
        if (spec.template and 
            spec.template.spec and 
            spec.template.spec.containers):
            
            container = spec.template.spec.containers[0]
            
            if container.resources:
                if container.resources.requests:
                    cpu_request = self._parse_cpu(container.resources.requests.get('cpu', '0'))
                    memory_request = self._parse_memory(container.resources.requests.get('memory', '0'))
                    
                if container.resources.limits:
                    cpu_limit = self._parse_cpu(container.resources.limits.get('cpu', '0'))
                    memory_limit = self._parse_memory(container.resources.limits.get('memory', '0'))
                    
        return cpu_request, memory_request, cpu_limit, memory_limit
        
    def _parse_cpu(self, cpu_str: str) -> float:
        """CPU 문자열 파싱"""
        if not cpu_str or cpu_str == '0':
            return 0.0
        cpu_str = str(cpu_str)
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000.0
        return float(cpu_str)
        
    def _parse_memory(self, memory_str: str) -> float:
        """메모리 문자열을 GB로 파싱"""
        if not memory_str or memory_str == '0':
            return 0.0
            
        memory_str = str(memory_str)
        units = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4}
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                value = float(memory_str[:-len(unit)])
                return (value * multiplier) / (1024**3)
                
        try:
            return float(memory_str) / (1024**3)
        except ValueError:
            return 0.0
        
    def _count_running_jobs(self) -> int:
        """실행 중인 작업 수 계산"""
        running_count = 0
        
        for namespace in self.namespaces:
            try:
                jobs = self.k8s_batch.list_namespaced_job(namespace=namespace)
                for job in jobs.items:
                    if job.status and job.status.active:
                        running_count += job.status.active
            except Exception:
                pass
                
        return running_count
        
    def _calculate_recent_oom_rate(self) -> float:
        """최근 1시간 OOM 비율 계산"""
        try:
            cutoff_time = datetime.now(tz=None) - timedelta(hours=1)
            oom_count = 0
            total_events = 0
            
            for namespace in self.namespaces:
                try:
                    events = self.k8s_core.list_namespaced_event(namespace=namespace)
                    
                    for event in events.items:
                        event_time = event.first_timestamp or event.event_time
                        if event_time and event_time.replace(tzinfo=None) > cutoff_time:
                            total_events += 1
                            if ('OOM' in str(event.reason) or 
                                'OutOfMemory' in str(event.message)):
                                oom_count += 1
                except Exception:
                    continue
                            
            return oom_count / max(total_events, 1)
            
        except Exception:
            return 0.0
            
    def _extract_priority_from_labels(self, labels: Dict) -> int:
        """레이블에서 우선순위 추출"""
        try:
            return int(labels.get('priority', 0))
        except (ValueError, TypeError):
            return 0
            
    def _estimate_job_type_from_labels(self, labels: Dict) -> str:
        """레이블에서 작업 타입 추정"""
        for key, value in labels.items():
            key_lower = key.lower()
            value_lower = str(value).lower()
            
            if 'spark' in key_lower or 'spark' in value_lower:
                return 'spark'
            elif any(x in key_lower for x in ['ml', 'machine-learning', 'training']):
                return 'ml'
            elif any(x in key_lower for x in ['etl', 'batch', 'data']):
                return 'batch'
                
        return 'generic'

class SKRueueEnvironment(gym.Env):
    """SKRueue RL 환경 (원래 고차원 상태 공간 유지)"""
    
    def __init__(self, kueue_interface: KueueInterface, max_queue_size: int = 10):
        super(SKRueueEnvironment, self).__init__()
        
        self.kueue = kueue_interface
        self.max_queue_size = max_queue_size
        self.logger = logging.getLogger('SKRueueEnvironment')
        
        # 상태 공간 정의 (원래 버전 복원)
        # [클러스터 리소스(4) + 히스토리(3) + 작업 큐(max_queue_size * 6)]
        self.state_dim = 4 + 3 + (max_queue_size * 6)
        self.observation_space = spaces.Box(
            low=0.0, high=1.0, shape=(self.state_dim,), dtype=np.float32
        )
        
        self.logger.info(f"상태 공간 차원: {self.state_dim} (큐 크기: {max_queue_size})")
        
        # 행동 공간 정의 (대기열 인덱스 + wait 액션)
        self.action_space = spaces.Discrete(max_queue_size + 1)
        
        self.logger.info(f"행동 공간 크기: {max_queue_size + 1} (작업 선택 {max_queue_size}개 + 대기 1개)")
        
        # 환경 상태
        self.current_jobs: List[JobInfo] = []
        self.current_resources: Optional[ClusterResource] = None
        self.step_count = 0
        self.episode_start_time = time.time()
        
        # 성능 지표
        self.completed_jobs_count = 0
        self.failed_jobs_count = 0
        self.total_wait_time = 0.0
        
        # 보상 함수 가중치
        self.reward_weights = {
            'throughput': 0.4,
            'utilization': 0.3,
            'wait_penalty': 0.2,
            'failure_penalty': 0.1
        }
        
    def reset(self, seed=None, options=None) -> Tuple[np.ndarray, Dict]:
        """환경 초기화 (Gymnasium 표준 준수)"""
        if seed is not None:
            np.random.seed(seed)
            
        self.step_count = 0
        self.episode_start_time = time.time()
        self.completed_jobs_count = 0
        self.failed_jobs_count = 0
        self.total_wait_time = 0.0
        
        # 현재 상태 갱신
        self._update_state()
        
        obs = self._get_observation()
        info = self._get_info()
        
        # Gymnasium 표준: 항상 (observation, info) 튜플 반환
        return obs, info
        
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """환경 스텝 실행"""
        # 이전 상태 저장
        prev_queue_length = len(self.current_jobs)
        prev_utilization = self._get_cluster_utilization()
        
        # 액션 실행
        action_success = self._execute_action(action)
        
        # 상태 갱신
        self._update_state()
        
        # 보상 계산
        reward = self._calculate_reward(prev_queue_length, prev_utilization, action_success)
        
        # 에피소드 종료 조건
        terminated = self._is_episode_terminated()
        truncated = self._is_episode_truncated()
        
        # 정보 구성
        info = self._get_info()
        info.update({
            'action_success': action_success,
            'prev_queue_length': prev_queue_length
        })
        
        self.step_count += 1
        
        obs = self._get_observation()
        
        # Gymnasium 스타일 반환 (5개 값)
        return obs, reward, terminated, truncated, info
        
    def _update_state(self):
        """현재 클러스터 및 작업 상태 갱신"""
        try:
            self.current_jobs = self.kueue.get_pending_jobs()
            self.current_resources = self.kueue.get_cluster_resources()
        except Exception as e:
            self.logger.error(f"상태 갱신 실패: {e}")
            
    def _get_observation(self) -> np.ndarray:
        """현재 상태를 observation으로 변환"""
            # 상태 공간 정의 (원래 버전 복원)
            # [클러스터 리소스(4) + 히스토리(3) + 작업 큐(max_queue_size * 6)]
                # 차원 0-3: 클러스터 리소스 정보
                    # obs[0]: CPU 가용률 (available/total)
                    # obs[1]: 메모리 가용률 (available/total)
                    # obs[2]: 평균 CPU 사용률
                    # obs[3]: 평균 메모리 사용률
                # 차원 4-6: 클러스터 히스토리
                    # obs[4]: 실행 중인 작업 수 (정규화)
                    # obs[5]: CPU 사용률 (중복, 히스토리용)
                    # obs[6]: 최근 OOM 발생률
                # 차원 7-66: 작업 큐 정보 (10개 작업 × 6차원)
                    # 각 작업당 6차원:
                        # [i*6 + 0]: CPU 요청량 (정규화)
                        # [i*6 + 1]: 메모리 요청량 (정규화)
                        # [i*6 + 2]: 우선순위 (정규화)
                        # [i*6 + 3]: 대기 시간 (정규화)
                        # [i*6 + 4]: 예상 실행 시간 (정규화)
                        # [i*6 + 5]: 작업 타입 (인코딩)
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
            
        # 작업 큐 정보 (각 작업당 6차원)
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
        
    def _calculate_reward(self, prev_queue_length: int, prev_utilization: float, action_success: bool) -> float:
        """보상 함수 계산 (단순화)"""
        reward = 0.0
        
        if not self.current_resources:
            return -1.0  # 상태 조회 실패 페널티
            
        # 1. 처리량 보상 (큐 길이 감소)
        queue_reduction = max(0, prev_queue_length - len(self.current_jobs))
        throughput_reward = queue_reduction * self.reward_weights['throughput']
        
        # 2. 리소스 사용률 보상
        current_utilization = self._get_cluster_utilization()
        utilization_reward = current_utilization * self.reward_weights['utilization']
        
        # 3. 대기시간 페널티
        if self.current_jobs:
            avg_wait_time = np.mean([
                (datetime.now() - job.arrival_time).total_seconds() / 60.0 
                for job in self.current_jobs
            ])
            wait_penalty = min(avg_wait_time / 60.0, 1.0) * self.reward_weights['wait_penalty']
        else:
            wait_penalty = 0.0
            
        # 4. 실패 페널티
        failure_penalty = 0.0
        if not action_success:
            failure_penalty = self.reward_weights['failure_penalty']
            
        # 최종 보상
        reward = throughput_reward + utilization_reward - wait_penalty - failure_penalty
        
        return reward
        
    def _get_cluster_utilization(self) -> float:
        """클러스터 전체 사용률 계산"""
        if not self.current_resources:
            return 0.0
            
        cpu_util = self.current_resources.avg_cpu_utilization
        memory_util = self.current_resources.avg_memory_utilization
        
        return (cpu_util + memory_util) / 2.0
        
    def _is_episode_terminated(self) -> bool:
        """에피소드 정상 종료 조건"""
        # 큐가 비고 충분한 시간이 지났을 때
        return (len(self.current_jobs) == 0 and self.step_count > 20)
        
    def _is_episode_truncated(self) -> bool:
        """에피소드 강제 종료 조건"""
        # 최대 스텝 수 도달 또는 시간 초과
        max_steps = 200
        max_time = 600  # 10분
        
        return (self.step_count >= max_steps or 
                time.time() - self.episode_start_time > max_time or
                self.current_resources is None)
                
    def _get_info(self) -> Dict:
        """정보 딕셔너리 생성"""
        return {
            'step_count': self.step_count,
            'queue_length': len(self.current_jobs),
            'cluster_utilization': self._get_cluster_utilization(),
            'completed_jobs': self.completed_jobs_count,
            'failed_jobs': self.failed_jobs_count
        }

class RLAgent:
    """SKRueue RL 에이전트 (개선된 버전)"""
    
    def __init__(self, env: SKRueueEnvironment, algorithm: str = 'DQN'):
        self.env = env
        self.algorithm = algorithm
        self.logger = logging.getLogger('RLAgent')
        self.model = None
        
        # 모델 초기화
        self._initialize_model()
        
    def _initialize_model(self):
        """모델 초기화 (고차원 상태 공간에 최적화)"""
        try:
            # 상태 공간 크기에 따른 하이퍼파라미터 조정
            state_dim = self.env.observation_space.shape[0]
            
            if self.algorithm == 'DQN':
                # 고차원 상태에 맞는 DQN 설정
                self.model = DQN(
                    'MlpPolicy', 
                    self.env, 
                    learning_rate=1e-4,
                    buffer_size=50000,  # 더 큰 버퍼
                    learning_starts=1000,
                    target_update_interval=500,
                    train_freq=4,
                    exploration_initial_eps=1.0,
                    exploration_final_eps=0.05,
                    exploration_fraction=0.5,  # 더 긴 탐험 기간
                    policy_kwargs=dict(net_arch=[256, 256, 128]),  # 더 큰 네트워크
                    verbose=1
                )
            elif self.algorithm == 'PPO':
                # 고차원 상태에 맞는 PPO 설정
                self.model = PPO(
                    'MlpPolicy', 
                    self.env, 
                    learning_rate=3e-4,
                    n_steps=1024,  # 더 많은 스텝
                    batch_size=64,
                    n_epochs=10,
                    gamma=0.99,
                    gae_lambda=0.95,
                    clip_range=0.2,
                    policy_kwargs=dict(net_arch=[256, 256, 128]),  # 더 큰 네트워크
                    verbose=1
                )
            elif self.algorithm == 'A2C':
                # 고차원 상태에 맞는 A2C 설정
                self.model = A2C(
                    'MlpPolicy', 
                    self.env, 
                    learning_rate=7e-4,
                    n_steps=8,  # 더 많은 스텝
                    gamma=0.99,
                    gae_lambda=1.0,
                    ent_coef=0.01,
                    vf_coef=0.25,
                    policy_kwargs=dict(net_arch=[256, 256]),
                    verbose=1
                )
            else:
                raise ValueError(f"지원하지 않는 알고리즘: {self.algorithm}")
                
            self.logger.info(f"{self.algorithm} 모델 초기화 완료 (상태 차원: {state_dim})")
            
        except Exception as e:
            self.logger.error(f"모델 초기화 실패: {e}")
            raise
            
    def train(self, total_timesteps: int = 10000):
        """모델 훈련"""
        self.logger.info(f"{self.algorithm} 훈련 시작 (총 {total_timesteps} 스텝)")
        
        try:
            self.model.learn(total_timesteps=total_timesteps)
            self.logger.info("훈련 완료")
        except Exception as e:
            self.logger.error(f"훈련 실패: {e}")
            raise
            
    def save_model(self, path: str):
        """모델 저장"""
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.model.save(path)
            self.logger.info(f"모델 저장 완료: {path}")
        except Exception as e:
            self.logger.error(f"모델 저장 실패: {e}")
            
    def load_model(self, path: str):
        """모델 로드"""
        try:
            if self.algorithm == 'DQN':
                self.model = DQN.load(path, env=self.env)
            elif self.algorithm == 'PPO':
                self.model = PPO.load(path, env=self.env)
            elif self.algorithm == 'A2C':
                self.model = A2C.load(path, env=self.env)
                
            self.logger.info(f"모델 로드 완료: {path}")
        except Exception as e:
            self.logger.error(f"모델 로드 실패: {e}")
            
    def predict(self, observation: np.ndarray) -> int:
        """액션 예측"""
        try:
            action, _ = self.model.predict(observation, deterministic=True)
            return int(action)
        except Exception as e:
            self.logger.error(f"예측 실패: {e}")
            return 0  # 기본 액션
        
    def evaluate(self, num_episodes: int = 5) -> Dict[str, float]:
        """모델 평가"""
        total_rewards = []
        episode_lengths = []
        success_rates = []
        
        for episode in range(num_episodes):
            obs, info = self.env.reset()  # 튜플 언패킹 수정
            total_reward = 0
            steps = 0
            successful_actions = 0
            total_actions = 0
            
            terminated = truncated = False
            while not (terminated or truncated):
                action = self.predict(obs)
                obs, reward, terminated, truncated, info = self.env.step(action)
                
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

def create_skrueue_environment(namespaces: List[str] = None, max_queue_size: int = 10) -> SKRueueEnvironment:
    """SKRueue 환경 생성 헬퍼 함수"""
    if namespaces is None:
        namespaces = ['skrueue-test']
        
    try:
        kueue_interface = KueueInterface(namespaces)
        env = SKRueueEnvironment(kueue_interface, max_queue_size=max_queue_size)
        
        # 환경 검증
        check_env(env)
        
        print(f"✅ SKRueue 환경 생성 완료:")
        print(f"   - 상태 공간: {env.state_dim}차원")
        print(f"   - 행동 공간: {env.action_space.n}개 액션")
        print(f"   - 최대 큐 크기: {max_queue_size}")
        
        return env
    except Exception as e:
        print(f"❌ 환경 생성 실패: {e}")
        raise

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SKRueue RL 시스템')
    parser.add_argument('--mode', choices=['train', 'eval', 'inference', 'test'], required=True)
    parser.add_argument('--algorithm', choices=['DQN', 'PPO', 'A2C'], default='DQN')
    parser.add_argument('--model-path', type=str, help='모델 파일 경로')
    parser.add_argument('--timesteps', type=int, default=20000, help='훈련 스텝 수 (고차원 환경 고려)')
    parser.add_argument('--namespaces', nargs='*', default=['skrueue-test'], help='모니터링 네임스페이스')
    parser.add_argument('--episodes', type=int, default=5, help='평가 에피소드 수')
    parser.add_argument('--max-queue-size', type=int, default=10, help='최대 큐 크기 (상태 공간 차원에 영향)')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 환경 생성
        print(f"🔧 SKRueue 환경 생성 중... (네임스페이스: {args.namespaces}, 큐 크기: {args.max_queue_size})")
        env = create_skrueue_environment(args.namespaces, args.max_queue_size)
        print("✅ 환경 생성 완료")
        
        # 에이전트 생성
        print(f"🤖 {args.algorithm} 에이전트 생성 중...")
        agent = RLAgent(env, args.algorithm)
        print("✅ 에이전트 생성 완료")
        
        if args.mode == 'test':
            # 환경 테스트
            print("🧪 환경 테스트 실행...")
            obs, info = env.reset()  # 튜플 언패킹 수정
            print(f"관찰 공간: {env.observation_space} ({env.state_dim}차원)")
            print(f"행동 공간: {env.action_space} ({env.action_space.n}개 액션)")
            print(f"초기 관찰 (처음 10차원): {obs[:10]}")
            print(f"초기 정보: {info}")
            print(f"상태 구조:")
            print(f"  - 클러스터 리소스: 차원 0-3")
            print(f"  - 히스토리: 차원 4-6") 
            print(f"  - 작업 큐: 차원 7-{env.state_dim-1} (작업당 6차원)")
            
            for i in range(3):
                action = env.action_space.sample()
                obs, reward, terminated, truncated, info = env.step(action)
                print(f"Step {i+1}: action={action}, reward={reward:.3f}, info={info}")
                
                if terminated or truncated:
                    obs, info = env.reset()  # 튜플 언패킹 수정
                    
            print("✅ 환경 테스트 완료")
            
        elif args.mode == 'train':
            # 훈련 모드
            print(f"🎯 {args.algorithm} 훈련 시작...")
            agent.train(args.timesteps)
            
            if args.model_path:
                agent.save_model(args.model_path)
                print(f"💾 모델 저장 완료: {args.model_path}")
                
        elif args.mode == 'eval':
            # 평가 모드
            if args.model_path:
                print(f"📂 모델 로드 중: {args.model_path}")
                agent.load_model(args.model_path)
            
            print(f"📊 모델 평가 중... ({args.episodes} 에피소드)")
            results = agent.evaluate(args.episodes)
            
            print("📈 평가 결과:")
            for key, value in results.items():
                print(f"  {key}: {value:.4f}")
                
        elif args.mode == 'inference':
            # 추론 모드 (실제 운용)
            if not args.model_path:
                raise ValueError("추론 모드에서는 모델 경로가 필요합니다")
                
            print(f"📂 모델 로드 중: {args.model_path}")
            agent.load_model(args.model_path)
            
            print("🚀 SKRueue 에이전트 실행 중...")
            obs, info = env.reset()  # 튜플 언패킹 수정
            
            try:
                step_count = 0
                while True:
                    action = agent.predict(obs)
                    obs, reward, terminated, truncated, info = env.step(action)
                    
                    step_count += 1
                    print(f"Step {step_count}: action={action}, reward={reward:.3f}, "
                          f"queue={info['queue_length']}, util={info['cluster_utilization']:.3f}")
                    
                    if terminated or truncated:
                        print("에피소드 종료, 환경 리셋")
                        obs, info = env.reset()  # 튜플 언패킹 수정
                        step_count = 0
                        
                    time.sleep(5)  # 5초 간격으로 실행
                    
            except KeyboardInterrupt:
                print("\n👋 사용자에 의해 중단됨")
                
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()