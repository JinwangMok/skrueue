import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional
import logging
from datetime import datetime, timedelta

from config.simulation_config import SimulationConfig
from simulator.cluster_simulator import ClusterSimulator, Job
from simulator.job_generator import JobGenerator

class DataAugmenter:
    """시뮬레이션 데이터 증강 생성기"""
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.simulator = ClusterSimulator(config)
        self.job_generator = JobGenerator(config)
    
    def generate_training_data(self, 
                             duration: int,
                             num_episodes: int = 100,
                             scheduler_policy: str = "random") -> pd.DataFrame:
        """RL 학습용 데이터 생성"""
        all_data = []
        
        for episode in range(num_episodes):
            self.logger.info(f"Generating episode {episode + 1}/{num_episodes}")
            
            # 시뮬레이터 리셋
            self.simulator.reset()
            
            # 워크로드 생성
            workload = self.job_generator.generate_workload_pattern(duration)
            
            # 시뮬레이션 실행
            episode_data = self._run_simulation(workload, scheduler_policy)
            
            # 데이터 증강
            augmented_data = self._augment_data(episode_data)
            
            all_data.extend(augmented_data)
        
        # DataFrame으로 변환
        df = pd.DataFrame(all_data)
        self.logger.info(f"Generated {len(df)} training samples")
        
        return df
    
    def _run_simulation(self, workload: List[Tuple[float, Job]], 
                       scheduler_policy: str) -> List[Dict]:
        """시뮬레이션 실행 및 상태-행동-보상 데이터 수집"""
        data = []
        workload_idx = 0
        
        while self.simulator.current_time < self.config.simulation_duration:
            # 새 작업 제출
            while (workload_idx < len(workload) and 
                   workload[workload_idx][0] <= self.simulator.current_time):
                _, job = workload[workload_idx]
                self.simulator.submit_job(job)
                workload_idx += 1
            
            # 현재 상태 수집
            state = self._get_state_vector()
            
            # 스케줄링 결정
            if len(self.simulator.pending_queue) > 0:
                action = self._make_scheduling_decision(scheduler_policy)
                
                # 행동 실행
                success = False
                if action is not None:
                    job_idx, node_idx = action
                    if job_idx < len(self.simulator.pending_queue):
                        job = self.simulator.pending_queue[job_idx]
                        success = self.simulator.schedule_job(job.id, node_idx)
                        
                        if success:
                            self.simulator.pending_queue.remove(job)
                
                # 즉각 보상 계산
                immediate_reward = self._calculate_immediate_reward(success)
                
                # 데이터 포인트 저장
                data.append({
                    "state": state,
                    "action": action,
                    "reward": immediate_reward,
                    "next_state": self._get_state_vector(),
                    "done": False,
                    "info": {
                        "time": self.simulator.current_time,
                        "queue_length": len(self.simulator.pending_queue),
                        "running_jobs": sum(len(n.running_jobs) for n in self.simulator.nodes)
                    }
                })
            
            # 시간 진행
            self.simulator.step(self.config.tick_interval)
        
        return data
    
    def _get_state_vector(self) -> np.ndarray:
        """현재 상태를 벡터로 변환"""
        # 클러스터 상태 (4차원)
        cluster_state = self.simulator.get_cluster_state()
        cluster_vector = np.array([
            cluster_state["cpu_available_ratio"],
            cluster_state["memory_available_ratio"],
            cluster_state["cpu_utilization"],
            cluster_state["memory_utilization"]
        ])
        
        # 클러스터 히스토리 (3차원)
        history_vector = np.array([
            cluster_state["running_jobs"] / 100.0,  # 정규화
            np.mean([h["value"] for h in self.simulator.metrics["cpu_utilization_history"][-10:]]) 
                if self.simulator.metrics["cpu_utilization_history"] else 0.0,
            self.simulator.metrics["oom_events"] / 100.0  # 정규화
        ])
        
        # 작업 큐 상태 (가변 차원)
        if self.config.dynamic_state:
            # 동적 큐 크기
            queue_size = min(len(self.simulator.pending_queue), self.config.max_queue_size)
            queue_states = self.simulator.get_queue_state(queue_size)
        else:
            # 고정 큐 크기
            queue_states = self.simulator.get_queue_state(10)
        
        # 큐 상태를 평면 벡터로 변환
        queue_vector = []
        for job_state in queue_states:
            queue_vector.extend([
                job_state["cpu_request"],
                job_state["memory_request"],
                job_state["priority"],
                job_state["wait_time"],
                job_state["estimated_duration"],
                job_state["job_type"]
            ])
        
        # 전체 상태 벡터 결합
        state_vector = np.concatenate([
            cluster_vector,
            history_vector,
            np.array(queue_vector)
        ])
        
        return state_vector
    
    def _make_scheduling_decision(self, policy: str) -> Optional[Tuple[int, int]]:
        """스케줄링 정책에 따른 결정"""
        if policy == "random":
            # 랜덤 스케줄링
            if len(self.simulator.pending_queue) > 0:
                job_idx = np.random.randint(0, min(len(self.simulator.pending_queue), 10))
                node_idx = np.random.randint(0, len(self.simulator.nodes))
                return (job_idx, node_idx)
        
        elif policy == "first_fit":
            # First-fit 스케줄링
            for i, job in enumerate(self.simulator.pending_queue[:10]):
                for j, node in enumerate(self.simulator.nodes):
                    if node.can_fit_job(job):
                        return (i, j)
        
        elif policy == "best_fit":
            # Best-fit 스케줄링
            best_fit = None
            min_waste = float('inf')
            
            for i, job in enumerate(self.simulator.pending_queue[:10]):
                for j, node in enumerate(self.simulator.nodes):
                    if node.can_fit_job(job):
                        waste = (node.cpu_available - job.cpu_request) + \
                               (node.memory_available - job.memory_request)
                        if waste < min_waste:
                            min_waste = waste
                            best_fit = (i, j)
            
            return best_fit
        
        return None
    
    def _calculate_immediate_reward(self, success: bool) -> float:
        """즉각적인 보상 계산"""
        if not success:
            return -1.0  # 스케줄링 실패 페널티
        
        # 성공 시 보상 계산
        reward = 0.0
        
        # 처리량 보상
        throughput = self.simulator.metrics["completed_jobs"] / max(self.simulator.current_time, 1)
        reward += self.config.reward_weights.get("throughput", 0.4) * throughput
        
        # 자원 활용도 보상
        cluster_state = self.simulator.get_cluster_state()
        utilization = (cluster_state["cpu_utilization"] + cluster_state["memory_utilization"]) / 2
        reward += self.config.reward_weights.get("utilization", 0.3) * utilization
        
        # 대기 시간 페널티
        avg_wait = self.simulator.metrics["total_wait_time"] / max(len(self.simulator.jobs), 1)
        wait_penalty = min(avg_wait / 3600.0, 1.0)  # 최대 1시간 기준
        reward -= self.config.reward_weights.get("wait_penalty", 0.2) * wait_penalty
        
        return reward
    
    def _augment_data(self, episode_data: List[Dict]) -> List[Dict]:
        """데이터 증강 적용"""
        augmented = []
        
        for data_point in episode_data:
            # 원본 데이터 추가
            augmented.append(data_point)
            
            # 노이즈 추가된 버전
            if np.random.random() < 0.5:
                noisy_state = data_point["state"] + np.random.normal(
                    0, self.config.noise_factor, size=data_point["state"].shape
                )
                augmented.append({
                    **data_point,
                    "state": noisy_state,
                    "augmented": True
                })
            
            # 이상치 생성
            if np.random.random() < self.config.anomaly_rate:
                anomaly_state = data_point["state"].copy()
                # 랜덤하게 일부 차원을 극단값으로 변경
                anomaly_indices = np.random.choice(
                    len(anomaly_state), 
                    size=np.random.randint(1, 5), 
                    replace=False
                )
                for idx in anomaly_indices:
                    anomaly_state[idx] = np.random.choice([0.0, 1.0])
                
                augmented.append({
                    **data_point,
                    "state": anomaly_state,
                    "anomaly": True
                })
        
        return augmented
    
    def generate_evaluation_scenarios(self) -> List[Dict]:
        """평가용 시나리오 생성"""
        scenarios = []
        
        # 1. 정상 부하 시나리오
        scenarios.append({
            "name": "normal_load",
            "duration": 3600,
            "arrival_rate": 0.5,
            "description": "일반적인 업무 시간 부하"
        })
        
        # 2. 고부하 시나리오
        scenarios.append({
            "name": "high_load",
            "duration": 3600,
            "arrival_rate": 2.0,
            "description": "피크 시간대 고부하"
        })
        
        # 3. 버스트 시나리오
        scenarios.append({
            "name": "burst_load",
            "duration": 3600,
            "arrival_rate": 0.3,
            "burst_probability": 0.1,
            "burst_size": 20,
            "description": "간헐적 버스트 부하"
        })
        
        # 4. 리소스 제약 시나리오
        scenarios.append({
            "name": "resource_constrained",
            "duration": 3600,
            "arrival_rate": 0.8,
            "node_failure_rate": 0.2,
            "description": "일부 노드 실패 상황"
        })
        
        # 5. 대형 작업 시나리오
        scenarios.append({
            "name": "large_jobs",
            "duration": 3600,
            "arrival_rate": 0.3,
            "large_job_ratio": 0.5,
            "description": "대형 작업이 많은 상황"
        })
        
        return scenarios