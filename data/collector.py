"""데이터 수집기"""
import time
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading

from core.interface import KueueInterface, JobInfo, ClusterResource
from utils import K8sClient, ResourceParser, get_logger
from config.settings import get_config
from .database import DatabaseManager


class DataCollector:
    """실시간 데이터 수집기"""
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.config = get_config()
        self.logger = get_logger('DataCollector')
        self.db = db_manager or DatabaseManager()
        self.kueue = KueueInterface()
        self.k8s = K8sClient()
        self.parser = ResourceParser()
        
        # 수집 상태
        self.is_collecting = False
        self.collection_thread = None
        self.job_cache = {}
        
        # 메트릭
        self.collection_metrics = {
            'jobs_collected': 0,
            'states_collected': 0,
            'errors': 0,
            'last_collection': None
        }
    
    def start_collection(self):
        """데이터 수집 시작"""
        if self.is_collecting:
            self.logger.warning("Collection already running")
            return
        
        self.is_collecting = True
        self.collection_thread = threading.Thread(target=self._collection_loop)
        self.collection_thread.daemon = True
        self.collection_thread.start()
        
        self.logger.info("Data collection started")
    
    def stop_collection(self):
        """데이터 수집 중지"""
        self.is_collecting = False
        if self.collection_thread:
            self.collection_thread.join(timeout=10)
        
        self.logger.info("Data collection stopped")
        self._log_metrics()
    
    def _collection_loop(self):
        """수집 메인 루프"""
        while self.is_collecting:
            try:
                start_time = time.time()
                
                # 작업 데이터 수집
                jobs_collected = self._collect_job_data()
                
                # 클러스터 상태 수집
                cluster_collected = self._collect_cluster_state()
                
                # 메트릭 업데이트
                if jobs_collected:
                    self.collection_metrics['jobs_collected'] += jobs_collected
                if cluster_collected:
                    self.collection_metrics['states_collected'] += 1
                
                self.collection_metrics['last_collection'] = datetime.now()
                
                # 수집 간격 유지
                elapsed = time.time() - start_time
                sleep_time = max(0, self.config.data.collection_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"Collection loop error: {e}")
                self.collection_metrics['errors'] += 1
                time.sleep(self.config.data.collection_interval)
    
    def _collect_job_data(self) -> int:
        """작업 데이터 수집"""
        try:
            jobs_collected = 0
            
            for namespace in self.config.kueue.namespaces:
                # Kubernetes Jobs
                jobs = self.k8s.batch_v1.list_namespaced_job(namespace=namespace)
                
                for job in jobs.items:
                    job_data = self._extract_job_data(job, namespace)
                    if job_data:
                        self.db.insert_job(job_data)
                        
                        # 캐시 업데이트
                        self.job_cache[job_data['job_id']] = job_data
                        jobs_collected += 1
                        
                        # 스케줄링 이벤트 기록
                        self._record_scheduling_event(job_data)
            
            return jobs_collected
            
        except Exception as e:
            self.logger.error(f"Failed to collect job data: {e}")
            return 0
    
    def _collect_cluster_state(self) -> bool:
        """클러스터 상태 수집"""
        try:
            # 클러스터 리소스 정보
            cluster_resource = self.kueue.get_cluster_resources()
            
            state_data = {
                'timestamp': datetime.now(),
                'total_cpu_capacity': cluster_resource.cpu_total,
                'total_memory_capacity': cluster_resource.memory_total,
                'available_cpu': cluster_resource.cpu_available,
                'available_memory': cluster_resource.memory_available,
                'pending_jobs_count': len(self.kueue.get_pending_jobs()),
                'running_jobs_count': cluster_resource.running_jobs,
                'queue_length': len(self.kueue.get_pending_jobs()),
                'node_count': self._count_nodes(),
                'avg_cpu_utilization': cluster_resource.avg_cpu_utilization,
                'avg_memory_utilization': cluster_resource.avg_memory_utilization,
                'metadata': json.dumps({
                    'oom_rate': cluster_resource.recent_oom_rate,
                    'collection_time': time.time()
                })
            }
            
            self.db.insert_cluster_state(state_data)
            
            # 시스템 메트릭 기록
            self._record_system_metrics(cluster_resource)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to collect cluster state: {e}")
            return False
    
    def _extract_job_data(self, job: Any, namespace: str) -> Optional[Dict[str, Any]]:
        """Job 객체에서 데이터 추출"""
        try:
            metadata = job.metadata
            spec = job.spec
            status = job.status
            
            # 리소스 추출
            cpu_req, mem_req, cpu_lim, mem_lim = self.parser.extract_resources(spec)
            
            # 상태 결정
            job_status = self._determine_job_status(status, spec)
            
            # OOM 확인
            oom_killed = self._check_oom_status(job, namespace)
            
            return {
                'job_id': metadata.uid,
                'name': metadata.name,
                'namespace': namespace,
                'submission_time': metadata.creation_timestamp,
                'start_time': status.start_time if status else None,
                'completion_time': status.completion_time if status else None,
                'cpu_request': cpu_req,
                'memory_request': mem_req,
                'cpu_limit': cpu_lim,
                'memory_limit': mem_lim,
                'priority': int(metadata.labels.get('priority', '0')) if metadata.labels else 0,
                'user_name': metadata.labels.get('user', 'unknown') if metadata.labels else 'unknown',
                'queue_name': metadata.labels.get('queue', 'default') if metadata.labels else 'default',
                'status': job_status,
                'job_type': metadata.annotations.get('skrueue.ai/job-type', 'generic') if metadata.annotations else 'generic',
                'restart_count': status.failed if status else 0,
                'oom_killed': oom_killed,
                'metadata': json.dumps({
                    'labels': metadata.labels,
                    'annotations': metadata.annotations
                }) if metadata.labels or metadata.annotations else None
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract job data: {e}")
            return None
    
    def _determine_job_status(self, status: Any, spec: Any) -> str:
        """작업 상태 결정"""
        if not status:
            return 'Unknown'
        
        if hasattr(status, 'succeeded') and status.succeeded:
            return 'Succeeded'
        elif hasattr(status, 'failed') and status.failed:
            return 'Failed'
        elif hasattr(status, 'active') and status.active:
            return 'Running'
        elif hasattr(spec, 'suspend') and spec.suspend:
            return 'Suspended'
        else:
            return 'Pending'
    
    def _check_oom_status(self, job: Any, namespace: str) -> bool:
        """OOM 상태 확인"""
        try:
            # 작업과 연관된 팟 확인
            pods = self.k8s.core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job.metadata.name}"
            )
            
            for pod in pods.items:
                if hasattr(pod.status, 'container_statuses'):
                    for container_status in pod.status.container_statuses:
                        # 현재 상태
                        if (hasattr(container_status.state, 'terminated') and
                            container_status.state.terminated and
                            container_status.state.terminated.reason == 'OOMKilled'):
                            return True
                        
                        # 이전 상태
                        if (hasattr(container_status, 'last_state') and
                            hasattr(container_status.last_state, 'terminated') and
                            container_status.last_state.terminated and
                            container_status.last_state.terminated.reason == 'OOMKilled'):
                            return True
            
            return False
            
        except Exception:
            return False
    
    def _record_scheduling_event(self, job_data: Dict[str, Any]):
        """스케줄링 이벤트 기록"""
        try:
            # 캐시에서 이전 상태 확인
            job_id = job_data['job_id']
            prev_data = self.job_cache.get(job_id)
            
            # 상태 변경 감지
            if prev_data and prev_data.get('status') != job_data.get('status'):
                event_type = self._determine_event_type(prev_data['status'], job_data['status'])
                
                if event_type:
                    # 대기 시간 계산
                    queue_wait_time = None
                    if (job_data.get('start_time') and job_data.get('submission_time')):
                        wait_delta = job_data['start_time'] - job_data['submission_time']
                        queue_wait_time = wait_delta.total_seconds() / 60.0
                    
                    event_data = {
                        'timestamp': datetime.now(),
                        'job_id': job_id,
                        'event_type': event_type,
                        'scheduler_name': 'skrueue',
                        'queue_wait_time': queue_wait_time,
                        'resource_efficiency': self._calculate_resource_efficiency(job_data),
                        'decision_metadata': json.dumps({
                            'prev_status': prev_data['status'],
                            'new_status': job_data['status']
                        })
                    }
                    
                    self.db.insert_scheduling_event(event_data)
                    
        except Exception as e:
            self.logger.error(f"Failed to record scheduling event: {e}")
    
    def _determine_event_type(self, prev_status: str, new_status: str) -> Optional[str]:
        """이벤트 타입 결정"""
        transitions = {
            ('Suspended', 'Pending'): 'admitted',
            ('Pending', 'Running'): 'scheduled',
            ('Running', 'Succeeded'): 'completed',
            ('Running', 'Failed'): 'failed',
            ('Pending', 'Failed'): 'rejected'
        }
        
        return transitions.get((prev_status, new_status))
    
    def _calculate_resource_efficiency(self, job_data: Dict[str, Any]) -> float:
        """리소스 효율성 계산"""
        # 간단한 효율성 메트릭 (실제 사용량/요청량)
        # 실제로는 더 복잡한 계산이 필요
        return 0.8  # 임시 값
    
    def _record_system_metrics(self, cluster_resource: ClusterResource):
        """시스템 메트릭 기록"""
        metrics = [
            {
                'timestamp': datetime.now(),
                'metric_name': 'cluster_cpu_utilization',
                'metric_value': cluster_resource.avg_cpu_utilization,
                'metric_type': 'gauge'
            },
            {
                'timestamp': datetime.now(),
                'metric_name': 'cluster_memory_utilization',
                'metric_value': cluster_resource.avg_memory_utilization,
                'metric_type': 'gauge'
            },
            {
                'timestamp': datetime.now(),
                'metric_name': 'cluster_running_jobs',
                'metric_value': cluster_resource.running_jobs,
                'metric_type': 'gauge'
            },
            {
                'timestamp': datetime.now(),
                'metric_name': 'cluster_oom_rate',
                'metric_value': cluster_resource.recent_oom_rate,
                'metric_type': 'gauge'
            }
        ]
        
        for metric in metrics:
            self.db.insert_metric(metric)
    
    def _count_nodes(self) -> int:
        """노드 수 계산"""
        try:
            nodes = self.k8s.core_v1.list_node()
            return len([n for n in nodes.items if self._is_node_ready(n)])
        except Exception:
            return 0
    
    def _is_node_ready(self, node: Any) -> bool:
        """노드 Ready 상태 확인"""
        if hasattr(node.status, 'conditions'):
            for condition in node.status.conditions:
                if condition.type == 'Ready' and condition.status == 'True':
                    return True
        return False
    
    def _log_metrics(self):
        """수집 메트릭 로깅"""
        self.logger.info(
            f"Collection metrics: "
            f"jobs={self.collection_metrics['jobs_collected']}, "
            f"states={self.collection_metrics['states_collected']}, "
            f"errors={self.collection_metrics['errors']}"
        )
