import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import heapq
from datetime import datetime, timedelta
import logging

from config.simulation_config import SimulationConfig, NodeConfig

@dataclass
class Job:
    """작업 정의"""
    id: str
    job_type: str
    cpu_request: int  # millicores
    memory_request: int  # MB
    duration: int  # seconds
    priority: int
    submit_time: float
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    status: str = "pending"  # pending, running, completed, failed
    assigned_node: Optional[int] = None
    
    def __lt__(self, other):
        """우선순위 큐를 위한 비교 연산자"""
        return self.priority > other.priority

@dataclass
class Node:
    """클러스터 노드 상태"""
    id: int
    config: NodeConfig
    cpu_used: int = 0
    memory_used: int = 0
    running_jobs: List[str] = field(default_factory=list)
    
    @property
    def cpu_available(self):
        return self.config.total_cpu_capacity - self.cpu_used
    
    @property
    def memory_available(self):
        return self.config.total_memory_capacity - self.memory_used
    
    @property
    def cpu_utilization(self):
        return self.cpu_used / self.config.total_cpu_capacity
    
    @property
    def memory_utilization(self):
        return self.memory_used / self.config.total_memory_capacity
    
    def can_fit_job(self, job: Job) -> bool:
        """작업이 노드에 할당 가능한지 확인"""
        return (self.cpu_available >= job.cpu_request and 
                self.memory_available >= job.memory_request)
    
    def allocate_job(self, job: Job):
        """작업을 노드에 할당"""
        self.cpu_used += job.cpu_request
        self.memory_used += job.memory_request
        self.running_jobs.append(job.id)
    
    def release_job(self, job: Job):
        """작업 완료 후 리소스 해제"""
        self.cpu_used -= job.cpu_request
        self.memory_used -= job.memory_request
        self.running_jobs.remove(job.id)

class ClusterSimulator:
    """Kubernetes 클러스터 시뮬레이터"""
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 클러스터 상태
        self.nodes: List[Node] = []
        self.jobs: Dict[str, Job] = {}
        self.pending_queue: List[Job] = []
        self.event_queue: List[Tuple[float, str, str]] = []  # (time, event_type, job_id)
        
        # 시뮬레이션 상태
        self.current_time: float = 0.0
        self.job_counter: int = 0
        
        # 메트릭
        self.metrics = {
            "completed_jobs": 0,
            "failed_jobs": 0,
            "total_wait_time": 0.0,
            "total_execution_time": 0.0,
            "cpu_utilization_history": [],
            "memory_utilization_history": [],
            "queue_length_history": [],
            "oom_events": 0
        }
        
        self._initialize_cluster()
    
    def _initialize_cluster(self):
        """클러스터 초기화"""
        for i, node_config in enumerate(self.config.nodes):
            self.nodes.append(Node(id=i, config=node_config))
        self.logger.info(f"Initialized cluster with {len(self.nodes)} nodes")
    
    def reset(self):
        """시뮬레이터 리셋"""
        self.current_time = 0.0
        self.job_counter = 0
        self.jobs.clear()
        self.pending_queue.clear()
        self.event_queue.clear()
        
        # 노드 상태 리셋
        for node in self.nodes:
            node.cpu_used = 0
            node.memory_used = 0
            node.running_jobs.clear()
        
        # 메트릭 리셋
        for key in self.metrics:
            if isinstance(self.metrics[key], list):
                self.metrics[key].clear()
            else:
                self.metrics[key] = 0
    
    def submit_job(self, job: Job):
        """작업 제출"""
        job.submit_time = self.current_time
        self.jobs[job.id] = job
        heapq.heappush(self.pending_queue, job)
        self.logger.debug(f"Job {job.id} submitted at {self.current_time}")
    
    def schedule_job(self, job_id: str, node_id: int) -> bool:
        """특정 노드에 작업 스케줄링"""
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        node = self.nodes[node_id]
        
        if not node.can_fit_job(job):
            return False
        
        # 작업 할당
        node.allocate_job(job)
        job.assigned_node = node_id
        job.start_time = self.current_time
        job.status = "running"
        
        # 종료 이벤트 스케줄
        end_time = self.current_time + job.duration
        heapq.heappush(self.event_queue, (end_time, "job_complete", job.id))
        
        # 대기 시간 메트릭 업데이트
        wait_time = job.start_time - job.submit_time
        self.metrics["total_wait_time"] += wait_time
        
        self.logger.debug(f"Job {job.id} scheduled on node {node_id}")
        return True
    
    def complete_job(self, job_id: str):
        """작업 완료 처리"""
        if job_id not in self.jobs:
            return
        
        job = self.jobs[job_id]
        if job.status != "running":
            return
        
        node = self.nodes[job.assigned_node]
        node.release_job(job)
        
        job.end_time = self.current_time
        job.status = "completed"
        
        # 메트릭 업데이트
        self.metrics["completed_jobs"] += 1
        self.metrics["total_execution_time"] += job.duration
        
        self.logger.debug(f"Job {job.id} completed")
    
    def fail_job(self, job_id: str, reason: str = "unknown"):
        """작업 실패 처리"""
        if job_id not in self.jobs:
            return
        
        job = self.jobs[job_id]
        
        if job.status == "running" and job.assigned_node is not None:
            node = self.nodes[job.assigned_node]
            node.release_job(job)
        
        job.status = "failed"
        job.end_time = self.current_time
        
        # 메트릭 업데이트
        self.metrics["failed_jobs"] += 1
        if reason == "oom":
            self.metrics["oom_events"] += 1
        
        self.logger.debug(f"Job {job.id} failed: {reason}")
    
    def step(self, delta_time: float):
        """시뮬레이션 한 스텝 진행"""
        self.current_time += delta_time
        
        # 이벤트 처리
        while self.event_queue and self.event_queue[0][0] <= self.current_time:
            event_time, event_type, job_id = heapq.heappop(self.event_queue)
            
            if event_type == "job_complete":
                self.complete_job(job_id)
        
        # 메트릭 수집
        self._collect_metrics()
    
    def _collect_metrics(self):
        """현재 상태의 메트릭 수집"""
        # CPU/메모리 사용률
        total_cpu_used = sum(node.cpu_used for node in self.nodes)
        total_cpu_capacity = sum(node.config.total_cpu_capacity for node in self.nodes)
        cpu_utilization = total_cpu_used / total_cpu_capacity if total_cpu_capacity > 0 else 0
        
        total_memory_used = sum(node.memory_used for node in self.nodes)
        total_memory_capacity = sum(node.config.total_memory_capacity for node in self.nodes)
        memory_utilization = total_memory_used / total_memory_capacity if total_memory_capacity > 0 else 0
        
        self.metrics["cpu_utilization_history"].append({
            "time": self.current_time,
            "value": cpu_utilization
        })
        
        self.metrics["memory_utilization_history"].append({
            "time": self.current_time,
            "value": memory_utilization
        })
        
        # 큐 길이
        self.metrics["queue_length_history"].append({
            "time": self.current_time,
            "value": len(self.pending_queue)
        })
    
    def get_cluster_state(self) -> Dict:
        """현재 클러스터 상태 반환"""
        total_cpu_capacity = sum(node.config.total_cpu_capacity for node in self.nodes)
        total_cpu_used = sum(node.cpu_used for node in self.nodes)
        total_memory_capacity = sum(node.config.total_memory_capacity for node in self.nodes)
        total_memory_used = sum(node.memory_used for node in self.nodes)
        
        running_jobs = sum(len(node.running_jobs) for node in self.nodes)
        
        return {
            "cpu_available_ratio": (total_cpu_capacity - total_cpu_used) / total_cpu_capacity,
            "memory_available_ratio": (total_memory_capacity - total_memory_used) / total_memory_capacity,
            "cpu_utilization": total_cpu_used / total_cpu_capacity,
            "memory_utilization": total_memory_used / total_memory_capacity,
            "running_jobs": running_jobs,
            "pending_jobs": len(self.pending_queue),
            "nodes": len(self.nodes),
            "time": self.current_time
        }
    
    def get_queue_state(self, max_jobs: int = 10) -> List[Dict]:
        """큐 상태 반환 (RL 상태 공간용)"""
        queue_state = []
        
        for i, job in enumerate(self.pending_queue[:max_jobs]):
            wait_time = self.current_time - job.submit_time
            queue_state.append({
                "cpu_request": job.cpu_request / 1000.0,  # 정규화
                "memory_request": job.memory_request / 1024.0,  # GB로 정규화
                "priority": job.priority / 10.0,  # 정규화
                "wait_time": wait_time / 3600.0,  # 시간으로 정규화
                "estimated_duration": job.duration / 3600.0,  # 시간으로 정규화
                "job_type": self._encode_job_type(job.job_type)
            })
        
        # 빈 슬롯 채우기
        while len(queue_state) < max_jobs:
            queue_state.append({
                "cpu_request": 0,
                "memory_request": 0,
                "priority": 0,
                "wait_time": 0,
                "estimated_duration": 0,
                "job_type": 0
            })
        
        return queue_state
    
    def _encode_job_type(self, job_type: str) -> float:
        """작업 유형을 숫자로 인코딩"""
        job_types = list(self.config.job_types.keys())
        if job_type in job_types:
            return job_types.index(job_type) / len(job_types)
        return 0.0
    
    def get_node_states(self) -> List[Dict]:
        """노드별 상태 반환"""
        return [{
            "id": node.id,
            "cpu_utilization": node.cpu_utilization,
            "memory_utilization": node.memory_utilization,
            "running_jobs": len(node.running_jobs),
            "cpu_available": node.cpu_available,
            "memory_available": node.memory_available
        } for node in self.nodes]