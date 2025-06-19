"""SKRueue RL 환경"""
import numpy as np
import time
from typing import Tuple, Dict, Optional, List
from datetime import datetime

try:
    import gymnasium as gym
    from gymnasium import spaces
except ImportError:
    import gym
    from gym import spaces

from .interface import KueueInterface, JobInfo, ClusterResource
from utils import get_logger
from config.settings import get_config


class SKRueueEnvironment(gym.Env):
    """SKRueue 강화학습 환경"""
    
    def __init__(self, kueue_interface: KueueInterface = None):
        super().__init__()
        
        self.config = get_config()
        self.logger = get_logger('SKRueueEnvironment')
        
        # Kueue 인터페이스
        self.kueue = kueue_interface or KueueInterface()
        
        # 환경 설정
        self.max_queue_size = self.config.kueue.max_queue_size
        self.state_dim = self._calculate_state_dim()
        
        # 상태/행동 공간 정의
        self.observation_space = spaces.Box(
            low=0.0, high=1.0, shape=(self.state_dim,), dtype=np.float32
        )
        self.action_space = spaces.Discrete(self.max_queue_size + 1)  # +1 for wait action
        
        # 상태 변수
        self.current_jobs: List[JobInfo] = []
        self.current_resources: Optional[ClusterResource] = None
        self.step_count = 0
        self.episode_start_time = time.time()
        
        # 성능 메트릭
        self.metrics = {
            'completed_jobs': 0,
            'failed_jobs': 0,
            'total_wait_time': 0.0,
            'total_execution_time': 0.0
        }
        
        self.logger.info(f"Environment initialized: state_dim={self.state_dim}, action_space={self.action_space.n}")
    
    def _calculate_state_dim(self) -> int:
        """상태 공간 차원 계산"""
        # [클러스터 리소스(4) + 히스토리(3) + 작업 큐(max_queue_size * 6)]
        return 4 + 3 + (self.max_queue_size * 6)
    
    def reset(self, seed=None, options=None) -> Tuple[np.ndarray, Dict]:
        """환경 초기화"""
        if seed is not None:
            np.random.seed(seed)
        
        self.step_count = 0
        self.episode_start_time = time.time()
        self.metrics = {
            'completed_jobs': 0,
            'failed_jobs': 0,
            'total_wait_time': 0.0,
            'total_execution_time': 0.0
        }
        
        self._update_state()
        obs = self._get_observation()
        info = self._get_info()
        
        return obs, info
    
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """환경 스텝 실행"""
        # 이전 상태 저장
        prev_state = {
            'queue_length': len(self.current_jobs),
            'utilization': self._get_cluster_utilization()
        }
        
        # 액션 실행
        action_result = self._execute_action(action)
        
        # 상태 갱신
        self._update_state()
        
        # 보상 계산
        reward = self._calculate_reward(prev_state, action_result)
        
        # 종료 조건
        terminated = self._is_terminated()
        truncated = self._is_truncated()
        
        # 관찰 및 정보
        obs = self._get_observation()
        info = self._get_info()
        info['action_result'] = action_result
        
        self.step_count += 1
        
        return obs, reward, terminated, truncated, info
    
    def _update_state(self):
        """현재 상태 갱신"""
        try:
            self.current_jobs = self.kueue.get_pending_jobs()
            self.current_resources = self.kueue.get_cluster_resources()
        except Exception as e:
            self.logger.error(f"Failed to update state: {e}")
    
    def _get_observation(self) -> np.ndarray:
        """관찰 벡터 생성"""
        obs = np.zeros(self.state_dim, dtype=np.float32)
        
        if self.current_resources:
            # 클러스터 리소스 정보 (0-3)
            obs[0] = self.current_resources.cpu_available / max(self.current_resources.cpu_total, 1)
            obs[1] = self.current_resources.memory_available / max(self.current_resources.memory_total, 1)
            obs[2] = self.current_resources.avg_cpu_utilization
            obs[3] = self.current_resources.avg_memory_utilization
            
            # 히스토리 정보 (4-6)
            obs[4] = min(self.current_resources.running_jobs / 100.0, 1.0)
            obs[5] = self.current_resources.avg_cpu_utilization  # redundant but for history
            obs[6] = self.current_resources.recent_oom_rate
        
        # 작업 큐 정보 (7+)
        queue_start_idx = 7
        for i, job in enumerate(self.current_jobs[:self.max_queue_size]):
            base_idx = queue_start_idx + (i * 6)
            
            # 정규화된 작업 특성
            max_cpu = self.current_resources.cpu_total if self.current_resources else 100
            max_memory = self.current_resources.memory_total if self.current_resources else 100
            
            obs[base_idx] = min(job.cpu_request / max_cpu, 1.0)
            obs[base_idx + 1] = min(job.memory_request / max_memory, 1.0)
            obs[base_idx + 2] = min(job.priority / 10.0, 1.0)
            
            # 대기 시간 (분)
            wait_time = (datetime.now() - job.arrival_time).total_seconds() / 60.0
            obs[base_idx + 3] = min(wait_time / 60.0, 1.0)  # 최대 1시간
            
            # 예상 실행 시간
            obs[base_idx + 4] = min(job.estimated_duration / 120.0, 1.0)  # 최대 2시간
            
            # 작업 타입 인코딩
            job_type_encoding = {
                'spark': 0.8, 'ml': 0.6, 'batch': 0.4, 
                'etl': 0.5, 'realtime': 0.7, 'generic': 0.2
            }
            obs[base_idx + 5] = job_type_encoding.get(job.job_type, 0.2)
        
        return obs
    
    def _execute_action(self, action: int) -> Dict:
        """액션 실행"""
        if action >= len(self.current_jobs) or action == self.max_queue_size:
            # Wait 액션
            return {'success': True, 'action_type': 'wait'}
        
        # 작업 승인
        selected_job = self.current_jobs[action]
        success = self.kueue.admit_job(selected_job)
        
        if success:
            self.logger.info(f"Admitted job: {selected_job.name}")
            self.metrics['completed_jobs'] += 1
        
        return {
            'success': success,
            'action_type': 'admit',
            'job_name': selected_job.name
        }
    
    def _calculate_reward(self, prev_state: Dict, action_result: Dict) -> float:
        """보상 계산"""
        if not self.current_resources:
            return -1.0
        
        weights = self.config.rl.reward_weights
        reward = 0.0
        
        # 1. 처리량 보상
        queue_reduction = max(0, prev_state['queue_length'] - len(self.current_jobs))
        reward += queue_reduction * weights['throughput']
        
        # 2. 자원 활용도 보상
        current_util = self._get_cluster_utilization()
        reward += current_util * weights['utilization']
        
        # 3. 대기시간 페널티
        if self.current_jobs:
            avg_wait = np.mean([
                (datetime.now() - job.arrival_time).total_seconds() / 60.0
                for job in self.current_jobs
            ])
            wait_penalty = min(avg_wait / 60.0, 1.0) * weights['wait_penalty']
            reward -= wait_penalty
        
        # 4. 실패 페널티
        if not action_result['success'] and action_result['action_type'] == 'admit':
            reward -= weights['failure_penalty']
        
        return np.clip(reward, -1.0, 1.0)
    
    def _get_cluster_utilization(self) -> float:
        """클러스터 활용도"""
        if not self.current_resources:
            return 0.0
        
        cpu_util = self.current_resources.avg_cpu_utilization
        mem_util = self.current_resources.avg_memory_utilization
        
        return (cpu_util + mem_util) / 2.0
    
    def _is_terminated(self) -> bool:
        """정상 종료 조건"""
        # 큐가 비고 충분한 시간이 지났을 때
        return len(self.current_jobs) == 0 and self.step_count > 20
    
    def _is_truncated(self) -> bool:
        """강제 종료 조건"""
        max_steps = 200
        max_time = 600  # 10분
        
        return (self.step_count >= max_steps or 
                time.time() - self.episode_start_time > max_time)
    
    def _get_info(self) -> Dict:
        """정보 딕셔너리"""
        return {
            'step_count': self.step_count,
            'queue_length': len(self.current_jobs),
            'cluster_utilization': self._get_cluster_utilization(),
            'metrics': self.metrics.copy()
        }
