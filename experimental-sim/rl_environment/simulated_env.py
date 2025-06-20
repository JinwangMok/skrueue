import gymnasium as gym
from gymnasium import spaces
import numpy as np
from typing import Dict, Tuple, Any, Optional

from config.simulation_config import SimulationConfig
from simulator.cluster_simulator import ClusterSimulator
from simulator.job_generator import JobGenerator

class SimulatedKubernetesEnv(gym.Env):
    """시뮬레이션 기반 Kubernetes 스케줄링 환경"""
    
    def __init__(self, config: SimulationConfig):
        super().__init__()
        self.config = config
        self.simulator = ClusterSimulator(config)
        self.job_generator = JobGenerator(config)
        
        # 상태 공간 정의
        if config.dynamic_state:
            # 동적 상태 공간
            max_state_dim = 7 + config.max_queue_size * 6
            self.observation_space = spaces.Box(
                low=0, high=1, shape=(max_state_dim,), dtype=np.float32
            )
        else:
            # 고정 상태 공간 (67차원)
            self.observation_space = spaces.Box(
                low=0, high=1, shape=(67,), dtype=np.float32
            )
        
        # 행동 공간 정의 (작업 인덱스 × 노드 인덱스)
        max_jobs = config.max_queue_size if config.dynamic_state else 10
        self.action_space = spaces.Discrete(max_jobs * len(config.nodes))
        
        # 에피소드 설정
        self.max_steps = config.simulation_duration // config.tick_interval
        self.current_step = 0
        
        # 워크로드 생성
        self.workload = []
        self.workload_idx = 0
        
        # 메트릭
        self.episode_metrics = {
            "total_reward": 0,
            "completed_jobs": 0,
            "failed_jobs": 0,
            "avg_wait_time": 0,
            "avg_utilization": 0
        }
    
    def reset(self, seed: Optional[int] = None, options: Optional[Dict] = None) -> Tuple[np.ndarray, Dict]:
        """환경 리셋"""
        super().reset(seed=seed)
        
        # 시뮬레이터 리셋
        self.simulator.reset()
        self.current_step = 0
        
        # 새 워크로드 생성
        self.workload = self.job_generator.generate_workload_pattern(
            self.config.simulation_duration
        )
        self.workload_idx = 0
        
        # 메트릭 리셋
        for key in self.episode_metrics:
            self.episode_metrics[key] = 0
        
        # 초기 상태 반환
        state = self._get_observation()
        info = self._get_info()
        
        return state, info
    
    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """환경 스텝 실행"""
        # 새 작업 제출
        self._submit_pending_jobs()
        
        # 행동 실행 (action을 전달)
        reward = self._execute_action(action)
        
        # 시뮬레이션 시간 진행
        self.simulator.step(self.config.tick_interval)
        self.current_step += 1
        
        # 다음 상태 및 종료 조건
        next_state = self._get_observation()
        terminated = self.current_step >= self.max_steps
        truncated = False
        
        # 정보 수집
        info = self._get_info()
        self.episode_metrics["total_reward"] += reward
        
        return next_state, reward, terminated, truncated, info
    
    def _submit_pending_jobs(self):
        """대기 중인 작업 제출"""
        while (self.workload_idx < len(self.workload) and 
               self.workload[self.workload_idx][0] <= self.simulator.current_time):
            _, job = self.workload[self.workload_idx]
            self.simulator.submit_job(job)
            self.workload_idx += 1
    
    def _execute_action(self, action: int) -> float:  # 파라미터 변경
        """행동 실행 및 보상 계산"""
        # action을 job_idx와 node_idx로 분해
        max_jobs = self.config.max_queue_size if self.config.dynamic_state else 10
        job_idx = action // len(self.config.nodes)
        node_idx = action % len(self.config.nodes)
        
        # 유효성 검사
        if (job_idx >= len(self.simulator.pending_queue) or 
            node_idx >= len(self.simulator.nodes)):
            return -0.1  # 잘못된 행동 페널티
        
        # 스케줄링 시도
        job = self.simulator.pending_queue[job_idx]
        success = self.simulator.schedule_job(job.id, node_idx)
        
        if success:
            self.simulator.pending_queue.remove(job)
            
            # 보상 계산
            reward = self._calculate_reward(job, node_idx)
            return reward
        else:
            return -0.5  # 스케줄링 실패 페널티
    
    def _calculate_reward(self, job, node_idx: int) -> float:
        """보상 함수"""
        reward = 0.0
        
        # 1. 처리량 보상 (작업 완료)
        reward += 0.1
        
        # 2. 대기 시간 페널티
        wait_time = self.simulator.current_time - job.submit_time
        wait_penalty = min(wait_time / 3600.0, 1.0) * 0.2
        reward -= wait_penalty
        
        # 3. 자원 활용도 보상
        node = self.simulator.nodes[node_idx]
        utilization = (node.cpu_utilization + node.memory_utilization) / 2
        reward += utilization * 0.3
        
        # 4. 로드 밸런싱 보상
        cpu_utils = [n.cpu_utilization for n in self.simulator.nodes]
        std_dev = np.std(cpu_utils)
        balance_reward = (1.0 - std_dev) * 0.2
        reward += balance_reward
        
        # 5. 우선순위 보상
        priority_reward = job.priority / 10.0 * 0.1
        reward += priority_reward
        
        return reward
    
    def _get_observation(self) -> np.ndarray:
        """현재 상태 관측"""
        # 클러스터 상태
        cluster_state = self.simulator.get_cluster_state()
        state_vector = [
            cluster_state["cpu_available_ratio"],
            cluster_state["memory_available_ratio"],
            cluster_state["cpu_utilization"],
            cluster_state["memory_utilization"],
            cluster_state["running_jobs"] / 100.0,
            self._get_cpu_history(),
            self._get_oom_rate()
        ]
        
        # 작업 큐 상태
        if self.config.dynamic_state:
            queue_size = min(len(self.simulator.pending_queue), self.config.max_queue_size)
        else:
            queue_size = 10
        
        queue_states = self.simulator.get_queue_state(queue_size)
        
        for job_state in queue_states:
            state_vector.extend([
                job_state["cpu_request"],
                job_state["memory_request"],
                job_state["priority"],
                job_state["wait_time"],
                job_state["estimated_duration"],
                job_state["job_type"]
            ])
        
        # 동적 상태의 경우 패딩
        if self.config.dynamic_state:
            while len(state_vector) < self.observation_space.shape[0]:
                state_vector.append(0.0)
        
        return np.array(state_vector, dtype=np.float32)
    
    def _get_cpu_history(self) -> float:
        """CPU 사용률 히스토리 평균"""
        history = self.simulator.metrics["cpu_utilization_history"]
        if not history:
            return 0.0
        
        recent = history[-10:] if len(history) > 10 else history
        return np.mean([h["value"] for h in recent])
    
    def _get_oom_rate(self) -> float:
        """OOM 발생률"""
        total_jobs = len(self.simulator.jobs)
        if total_jobs == 0:
            return 0.0
        
        return self.simulator.metrics["oom_events"] / total_jobs
    
    def _get_info(self) -> Dict[str, Any]:
        """추가 정보 반환"""
        return {
            "time": self.simulator.current_time,
            "pending_jobs": len(self.simulator.pending_queue),
            "running_jobs": sum(len(n.running_jobs) for n in self.simulator.nodes),
            "completed_jobs": self.simulator.metrics["completed_jobs"],
            "failed_jobs": self.simulator.metrics["failed_jobs"],
            "cpu_utilization": self.simulator.get_cluster_state()["cpu_utilization"],
            "memory_utilization": self.simulator.get_cluster_state()["memory_utilization"]
        }
    
    def render(self, mode='human'):
        """환경 시각화 (선택적)"""
        if mode == 'human':
            print(f"\n=== Step {self.current_step} ===")
            print(f"Time: {self.simulator.current_time:.1f}s")
            print(f"Pending Jobs: {len(self.simulator.pending_queue)}")
            print(f"Running Jobs: {sum(len(n.running_jobs) for n in self.simulator.nodes)}")
            
            cluster_state = self.simulator.get_cluster_state()
            print(f"CPU Utilization: {cluster_state['cpu_utilization']:.2%}")
            print(f"Memory Utilization: {cluster_state['memory_utilization']:.2%}")