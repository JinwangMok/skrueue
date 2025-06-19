"""Kueue 인터페이스"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from utils import K8sClient, ResourceParser, get_logger
from config.settings import get_config


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
    job_type: str
    labels: Dict[str, str] = None
    annotations: Dict[str, str] = None


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
    """Kueue 스케줄러 인터페이스"""
    
    def __init__(self, namespaces: List[str] = None):
        self.config = get_config()
        self.namespaces = namespaces or self.config.kueue.namespaces
        self.logger = get_logger('KueueInterface')
        self.k8s = K8sClient()
        self.parser = ResourceParser()
        
    def get_pending_jobs(self) -> List[JobInfo]:
        """대기 중인 작업 목록 조회"""
        pending_jobs = []
        
        for namespace in self.namespaces:
            try:
                # Kubernetes Jobs 조회
                jobs = self.k8s.batch_v1.list_namespaced_job(namespace=namespace)
                
                for job in jobs.items:
                    if self._is_job_pending(job):
                        job_info = self._create_job_info(job, namespace)
                        if job_info:
                            pending_jobs.append(job_info)
                            
                # Kueue Workloads 조회
                pending_jobs.extend(self._get_pending_workloads(namespace))
                
            except Exception as e:
                self.logger.error(f"Failed to get pending jobs in {namespace}: {e}")
        
        self.logger.info(f"Found {len(pending_jobs)} pending jobs")
        return pending_jobs
    
    def get_cluster_resources(self) -> ClusterResource:
        """클러스터 리소스 상태 조회"""
        try:
            nodes = self.k8s.core_v1.list_node()
            
            total_cpu = total_memory = 0.0
            available_cpu = available_memory = 0.0
            
            for node in nodes.items:
                if self._is_node_schedulable(node):
                    capacity = node.status.capacity
                    allocatable = node.status.allocatable
                    
                    total_cpu += self.parser.parse_cpu(capacity.get('cpu', '0'))
                    total_memory += self.parser.parse_memory(capacity.get('memory', '0'))
                    available_cpu += self.parser.parse_cpu(allocatable.get('cpu', '0'))
                    available_memory += self.parser.parse_memory(allocatable.get('memory', '0'))
            
            # 리소스 사용률 계산
            running_jobs = self._count_running_jobs()
            cpu_used = max(0, total_cpu - available_cpu)
            memory_used = max(0, total_memory - available_memory)
            
            return ClusterResource(
                cpu_total=total_cpu,
                cpu_available=available_cpu,
                memory_total=total_memory,
                memory_available=available_memory,
                running_jobs=running_jobs,
                avg_cpu_utilization=cpu_used / max(total_cpu, 1),
                avg_memory_utilization=memory_used / max(total_memory, 1),
                recent_oom_rate=self._calculate_oom_rate()
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster resources: {e}")
            return self._get_default_cluster_resource()
    
    def admit_job(self, job_info: JobInfo) -> bool:
        """작업 승인"""
        try:
            # Kubernetes Job의 suspend 상태 해제
            patch_body = {"spec": {"suspend": False}}
            
            result = self.k8s.batch_v1.patch_namespaced_job(
                name=job_info.name,
                namespace=job_info.namespace,
                body=patch_body
            )
            
            self.logger.info(f"Successfully admitted job: {job_info.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to admit job {job_info.name}: {e}")
            # Workload 승인 시도
            return self._admit_workload(job_info)
    
    def _is_job_pending(self, job: Any) -> bool:
        """작업이 대기 상태인지 확인"""
        return hasattr(job.spec, 'suspend') and job.spec.suspend == True
    
    def _create_job_info(self, job: Any, namespace: str) -> Optional[JobInfo]:
        """Job 객체에서 JobInfo 생성"""
        try:
            metadata = job.metadata
            spec = job.spec
            
            # 리소스 추출
            cpu_req, mem_req, _, _ = self.parser.extract_resources(spec)
            
            # 메타데이터 추출
            labels = metadata.labels or {}
            annotations = metadata.annotations or {}
            
            return JobInfo(
                job_id=metadata.uid,
                name=metadata.name,
                namespace=namespace,
                cpu_request=cpu_req,
                memory_request=mem_req,
                priority=int(labels.get('priority', '0')),
                arrival_time=metadata.creation_timestamp or datetime.now(),
                estimated_duration=float(annotations.get('skrueue.ai/estimated-duration', '30')),
                job_type=annotations.get('skrueue.ai/job-type', 'generic'),
                labels=labels,
                annotations=annotations
            )
        except Exception as e:
            self.logger.error(f"Failed to create JobInfo: {e}")
            return None
    
    def _get_pending_workloads(self, namespace: str) -> List[JobInfo]:
        """Kueue Workload 조회"""
        workloads = []
        try:
            result = self.k8s.custom_api.list_namespaced_custom_object(
                group="kueue.x-k8s.io",
                version="v1beta1",
                namespace=namespace,
                plural="workloads"
            )
            
            for workload in result.get('items', []):
                if self._is_workload_pending(workload):
                    job_info = self._create_job_info_from_workload(workload, namespace)
                    if job_info:
                        workloads.append(job_info)
                        
        except Exception as e:
            self.logger.debug(f"Failed to get workloads: {e}")
            
        return workloads
    
    def _is_workload_pending(self, workload: Dict) -> bool:
        """Workload가 대기 상태인지 확인"""
        spec = workload.get('spec', {})
        status = workload.get('status', {})
        return spec.get('suspend', False) or not status.get('admitted', False)
    
    def _create_job_info_from_workload(self, workload: Dict, namespace: str) -> Optional[JobInfo]:
        """Workload에서 JobInfo 생성"""
        try:
            metadata = workload.get('metadata', {})
            spec = workload.get('spec', {})
            
            # 리소스 추출 (간소화)
            cpu_req = mem_req = 1.0  # 기본값
            
            pod_sets = spec.get('podSets', [])
            if pod_sets:
                pod_set = pod_sets[0]
                resources = pod_set.get('template', {}).get('spec', {}).get('containers', [{}])[0].get('resources', {})
                requests = resources.get('requests', {})
                
                cpu_req = self.parser.parse_cpu(requests.get('cpu', '1'))
                mem_req = self.parser.parse_memory(requests.get('memory', '1Gi'))
            
            return JobInfo(
                job_id=metadata.get('uid', ''),
                name=metadata.get('name', ''),
                namespace=namespace,
                cpu_request=cpu_req,
                memory_request=mem_req,
                priority=spec.get('priority', 0),
                arrival_time=datetime.now(),
                estimated_duration=30,
                job_type='workload',
                labels=metadata.get('labels', {}),
                annotations=metadata.get('annotations', {})
            )
        except Exception as e:
            self.logger.error(f"Failed to create JobInfo from workload: {e}")
            return None
    
    def _is_node_schedulable(self, node: Any) -> bool:
        """노드 스케줄 가능 여부"""
        # Taint 확인
        if hasattr(node.spec, 'taints') and node.spec.taints:
            for taint in node.spec.taints:
                if taint.effect in ['NoSchedule', 'NoExecute']:
                    return False
        
        # Ready 상태 확인
        if hasattr(node.status, 'conditions'):
            for condition in node.status.conditions:
                if condition.type == 'Ready' and condition.status == 'True':
                    return True
                    
        return False
    
    def _count_running_jobs(self) -> int:
        """실행 중인 작업 수"""
        count = 0
        for namespace in self.namespaces:
            try:
                jobs = self.k8s.batch_v1.list_namespaced_job(namespace=namespace)
                for job in jobs.items:
                    if hasattr(job.status, 'active') and job.status.active:
                        count += job.status.active
            except Exception:
                pass
        return count
    
    def _calculate_oom_rate(self) -> float:
        """최근 OOM 비율 계산"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=1)
            oom_count = total_events = 0
            
            for namespace in self.namespaces:
                try:
                    events = self.k8s.core_v1.list_namespaced_event(namespace=namespace)
                    
                    for event in events.items:
                        event_time = event.first_timestamp or event.event_time
                        if event_time and event_time.replace(tzinfo=None) > cutoff_time.replace(tzinfo=None):
                            total_events += 1
                            if 'OOM' in str(event.reason) or 'OutOfMemory' in str(event.message):
                                oom_count += 1
                except Exception:
                    continue
                    
            return oom_count / max(total_events, 1)
            
        except Exception:
            return 0.0
    
    def _admit_workload(self, job_info: JobInfo) -> bool:
        """Workload 승인 (폴백)"""
        try:
            patch_body = {"spec": {"suspend": False}}
            
            result = self.k8s.custom_api.patch_namespaced_custom_object(
                group="kueue.x-k8s.io",
                version="v1beta1",
                namespace=job_info.namespace,
                plural="workloads",
                name=job_info.name,
                body=patch_body
            )
            
            self.logger.info(f"Successfully admitted workload: {job_info.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to admit workload {job_info.name}: {e}")
            return False
    
    def _get_default_cluster_resource(self) -> ClusterResource:
        """기본 클러스터 리소스 (오류 시)"""
        return ClusterResource(
            cpu_total=1.0,
            cpu_available=1.0,
            memory_total=1.0,
            memory_available=1.0,
            running_jobs=0,
            avg_cpu_utilization=0.0,
            avg_memory_utilization=0.0,
            recent_oom_rate=0.0
        )