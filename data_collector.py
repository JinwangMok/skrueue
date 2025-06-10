# data_collector.py
# SKRueue RL 학습용 데이터 수집 시스템
# 클러스터에서 실시간으로 작업 및 리소스 데이터를 수집하여 RL 훈련 데이터셋을 구축
# 버그 수정 및 안정성 개선 버전

import os
import json
import time
import logging
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import subprocess
import yaml

# Kubernetes Python Client
try:
    from kubernetes import client
    import kubernetes.config as k8s_config
    from kubernetes.client.rest import ApiException
except ImportError:
    print("kubernetes 패키지를 설치하세요: pip install kubernetes")
    exit(1)

# 데이터 수집 설정
@dataclass
class CollectionConfig:
    collection_interval: int = 10  # 초
    retention_days: int = 30
    db_path: str = "skrueue_training_data.db"
    prometheus_url: Optional[str] = None
    namespaces: List[str] = None  # None이면 모든 네임스페이스
    
    def __post_init__(self):
        if self.namespaces is None:
            self.namespaces = ["default", "spark", "kueue-system"]

@dataclass
class JobData:
    """작업 특성 데이터"""
    job_id: str
    name: str
    namespace: str
    submission_time: datetime
    start_time: Optional[datetime]
    completion_time: Optional[datetime]
    cpu_request: float
    memory_request: float  # GB
    cpu_limit: float
    memory_limit: float  # GB
    priority: int
    user: str
    queue_name: str
    status: str  # Pending, Running, Succeeded, Failed
    restart_count: int
    oom_killed: bool
    spark_config: Optional[Dict] = None

@dataclass 
class ClusterState:
    """클러스터 상태 데이터"""
    timestamp: datetime
    total_cpu_capacity: float
    total_memory_capacity: float  # GB
    available_cpu: float
    available_memory: float  # GB
    pending_jobs_count: int
    running_jobs_count: int
    queue_length: int
    node_count: int
    node_states: Dict[str, Dict] = None

@dataclass
class SchedulingEvent:
    """스케줄링 이벤트 데이터"""
    timestamp: datetime
    job_id: str
    event_type: str  # submitted, scheduled, started, completed, failed
    scheduler_decision: str  # 어떤 스케줄러가 결정했는지
    queue_wait_time: Optional[float] = None  # 분
    resource_efficiency: Optional[float] = None

class DataCollector:
    def __init__(self, config: CollectionConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.db_conn = self._setup_database()
        
        # Kubernetes 클라이언트 초기화 (in-cluster or local)
        from kubernetes.config.config_exception import ConfigException
        try:
            k8s_config.load_incluster_config()
            self.logger.info("✔️ In-cluster kubeconfig 로드 성공")
        except ConfigException:
            try:
                k8s_config.load_kube_config()
                self.logger.info("✔️ 로컬 kubeconfig(~/.kube/config) 로드 성공")
            except Exception as e:
                self.logger.error(f"❌ kubeconfig 로드 실패: {e}")
                raise
            
        self.k8s_core = client.CoreV1Api()
        self.k8s_batch = client.BatchV1Api()
        self.k8s_custom = client.CustomObjectsApi()
        
        # 데이터 캐시
        self.job_cache: Dict[str, JobData] = {}
        self.last_cluster_state: Optional[ClusterState] = None
        
    def _setup_logging(self) -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('skrueue_collector.log')
            ]
        )
        return logging.getLogger('SKRueueDataCollector')
        
    def _setup_database(self) -> sqlite3.Connection:
        """SQLite 데이터베이스 초기화"""
        conn = sqlite3.connect(self.config.db_path, check_same_thread=False)
        
        # 테이블 생성
        conn.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                name TEXT,
                namespace TEXT,
                submission_time TEXT,
                start_time TEXT,
                completion_time TEXT,
                cpu_request REAL,
                memory_request REAL,
                cpu_limit REAL,
                memory_limit REAL,
                priority INTEGER,
                user_name TEXT,
                queue_name TEXT,
                status TEXT,
                restart_count INTEGER,
                oom_killed BOOLEAN,
                spark_config TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS cluster_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                total_cpu_capacity REAL,
                total_memory_capacity REAL,
                available_cpu REAL,
                available_memory REAL,
                pending_jobs_count INTEGER,
                running_jobs_count INTEGER,
                queue_length INTEGER,
                node_count INTEGER,
                node_states TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS scheduling_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                job_id TEXT,
                event_type TEXT,
                scheduler_decision TEXT,
                queue_wait_time REAL,
                resource_efficiency REAL,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )
        ''')
        
        conn.commit()
        return conn
        
    def collect_job_data(self) -> List[JobData]:
        """현재 클러스터의 모든 작업 데이터 수집"""
        jobs = []
        
        for namespace in self.config.namespaces:
            try:
                # Kubernetes Jobs 수집
                k8s_jobs = self.k8s_batch.list_namespaced_job(namespace=namespace)
                
                for job in k8s_jobs.items:
                    job_data = self._extract_job_data(job, namespace)
                    if job_data:
                        jobs.append(job_data)
                        
                # Kueue Workloads 수집 (있는 경우)
                try:
                    workloads = self.k8s_custom.list_namespaced_custom_object(
                        group="kueue.x-k8s.io",
                        version="v1beta1", 
                        namespace=namespace,
                        plural="workloads"
                    )
                    
                    for workload in workloads.get('items', []):
                        job_data = self._extract_workload_data(workload, namespace)
                        if job_data:
                            jobs.append(job_data)
                            
                except ApiException as e:
                    if e.status != 404:  # Kueue가 설치되지 않은 경우는 무시
                        self.logger.warning(f"Workload 수집 실패: {e}")
                        
                # SparkApplications 수집 (있는 경우)
                try:
                    spark_apps = self.k8s_custom.list_namespaced_custom_object(
                        group="sparkoperator.k8s.io",
                        version="v1beta2",
                        namespace=namespace,
                        plural="sparkapplications"
                    )
                    
                    for spark_app in spark_apps.get('items', []):
                        job_data = self._extract_spark_app_data(spark_app, namespace)
                        if job_data:
                            jobs.append(job_data)
                            
                except ApiException as e:
                    if e.status != 404:  # Spark Operator가 설치되지 않은 경우는 무시
                        self.logger.warning(f"SparkApplication 수집 실패: {e}")
                        
            except Exception as e:
                self.logger.error(f"네임스페이스 {namespace} 작업 수집 실패: {e}")
                
        return jobs
        
    def _extract_job_data(self, job, namespace: str) -> Optional[JobData]:
        """Kubernetes Job에서 JobData 추출"""
        try:
            metadata = job.metadata
            spec = job.spec
            status = job.status
            
            # 기본 정보
            job_id = metadata.uid
            name = metadata.name
            submission_time = metadata.creation_timestamp
            
            # 리소스 요구사항 추출
            cpu_request, memory_request, cpu_limit, memory_limit = self._extract_resources(spec)
            
            # 상태 및 시간 정보
            start_time = status.start_time if status else None
            completion_time = status.completion_time if status else None
            
            # 상태 결정
            job_status = "Pending"
            if status:
                if status.succeeded:
                    job_status = "Succeeded"
                elif status.failed:
                    job_status = "Failed"
                elif status.active:
                    job_status = "Running"
                    
            # OOM 확인 (안전한 방식으로)
            oom_killed = self._check_oom_status_safe(job, namespace)
            
            # Spark 설정 추출 (있는 경우)
            spark_config = self._extract_spark_config(job)
            
            return JobData(
                job_id=job_id,
                name=name,
                namespace=namespace,
                submission_time=submission_time,
                start_time=start_time,
                completion_time=completion_time,
                cpu_request=cpu_request,
                memory_request=memory_request,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                priority=self._extract_priority(job),
                user=metadata.labels.get('user', 'unknown') if metadata.labels else 'unknown',
                queue_name=metadata.labels.get('kueue.x-k8s.io/queue-name', 'default') if metadata.labels else 'default',
                status=job_status,
                restart_count=status.failed or 0 if status else 0,
                oom_killed=oom_killed,
                spark_config=spark_config
            )
            
        except Exception as e:
            self.logger.error(f"Job 데이터 추출 실패 {job.metadata.name}: {e}")
            return None

    def _extract_spark_app_data(self, spark_app, namespace: str) -> Optional[JobData]:
        """SparkApplication에서 JobData 추출"""
        try:
            metadata = spark_app.get('metadata', {})
            spec = spark_app.get('spec', {})
            status = spark_app.get('status', {})
            
            # 기본 정보
            job_id = metadata.get('uid', 'unknown')
            name = metadata.get('name', 'unknown')
            submission_time = metadata.get('creationTimestamp')
            if submission_time:
                submission_time = datetime.fromisoformat(submission_time.replace('Z', '+00:00'))
            
            # 리소스 추출 (SparkApplication 형식)
            driver_spec = spec.get('driver', {})
            executor_spec = spec.get('executor', {})
            
            # CPU 및 메모리 계산
            driver_cores = driver_spec.get('cores', 1)
            driver_memory = self._parse_memory(driver_spec.get('memory', '1g'))
            
            executor_cores = executor_spec.get('cores', 1)
            executor_memory = self._parse_memory(executor_spec.get('memory', '1g'))
            executor_instances = executor_spec.get('instances', 1)
            
            total_cpu = driver_cores + (executor_cores * executor_instances)
            total_memory = driver_memory + (executor_memory * executor_instances)
            
            # 상태 결정
            app_state = status.get('applicationState', {}).get('state', 'UNKNOWN')
            job_status = self._map_spark_status(app_state)
            
            # 시간 정보
            start_time = None
            completion_time = None
            if 'executionAttempts' in status:
                attempts = status['executionAttempts']
                if attempts:
                    latest_attempt = attempts[-1]
                    if 'executionStart' in latest_attempt:
                        start_time = datetime.fromisoformat(latest_attempt['executionStart'].replace('Z', '+00:00'))
                    if 'executionEnd' in latest_attempt:
                        completion_time = datetime.fromisoformat(latest_attempt['executionEnd'].replace('Z', '+00:00'))
            
            return JobData(
                job_id=job_id,
                name=name,
                namespace=namespace,
                submission_time=submission_time or datetime.now(),
                start_time=start_time,
                completion_time=completion_time,
                cpu_request=total_cpu,
                memory_request=total_memory,
                cpu_limit=total_cpu * 1.2,  # 약간의 여유
                memory_limit=total_memory * 1.2,
                priority=self._extract_priority_from_labels(metadata.get('labels', {})),
                user=metadata.get('labels', {}).get('user', 'spark-user'),
                queue_name=metadata.get('labels', {}).get('queue', 'default'),
                status=job_status,
                restart_count=len(status.get('executionAttempts', [])) - 1,
                oom_killed=False,  # SparkApplication에서는 별도 확인 필요
                spark_config=spec.get('sparkConf', {})
            )
            
        except Exception as e:
            self.logger.error(f"SparkApplication 데이터 추출 실패 {spark_app.get('metadata', {}).get('name', 'unknown')}: {e}")
            return None

    def _map_spark_status(self, spark_state: str) -> str:
        """Spark 상태를 표준 Job 상태로 매핑"""
        state_mapping = {
            'SUBMITTED': 'Pending',
            'RUNNING': 'Running', 
            'COMPLETED': 'Succeeded',
            'FAILED': 'Failed',
            'UNKNOWN': 'Pending',
            'INVALIDATING': 'Failed',
            'SUCCEEDING': 'Running',
            'FAILING': 'Running'
        }
        return state_mapping.get(spark_state, 'Pending')
        
    def _extract_resources(self, spec) -> Tuple[float, float, float, float]:
        """리소스 요구사항 추출"""
        cpu_request = cpu_limit = 0.0
        memory_request = memory_limit = 0.0
        
        if spec.template and spec.template.spec and spec.template.spec.containers:
            container = spec.template.spec.containers[0]  # 첫 번째 컨테이너
            
            if container.resources:
                if container.resources.requests:
                    cpu_request = self._parse_cpu(container.resources.requests.get('cpu', '0'))
                    memory_request = self._parse_memory(container.resources.requests.get('memory', '0'))
                    
                if container.resources.limits:
                    cpu_limit = self._parse_cpu(container.resources.limits.get('cpu', '0'))
                    memory_limit = self._parse_memory(container.resources.limits.get('memory', '0'))
                    
        return cpu_request, memory_request, cpu_limit, memory_limit
        
    def _parse_cpu(self, cpu_str: str) -> float:
        """CPU 문자열을 float로 변환 (예: '500m' -> 0.5)"""
        if not cpu_str:
            return 0.0
            
        cpu_str = str(cpu_str)
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000.0
        return float(cpu_str)
        
    def _parse_memory(self, memory_str: str) -> float:
        """메모리 문자열을 GB로 변환 (예: '2Gi' -> 2.0)"""
        if not memory_str:
            return 0.0
            
        memory_str = str(memory_str)
        units = {
            'Ki': 1024,
            'Mi': 1024**2,
            'Gi': 1024**3,
            'Ti': 1024**4,
            'K': 1000,
            'M': 1000**2,
            'G': 1000**3,
            'T': 1000**4
        }
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                value = float(memory_str[:-len(unit)])
                return (value * multiplier) / (1024**3)  # GB로 변환
                
        try:
            return float(memory_str) / (1024**3)  # 바이트를 GB로
        except ValueError:
            return 0.0
        
    def collect_cluster_state(self) -> ClusterState:
        """현재 클러스터 상태 수집"""
        try:
            nodes = self.k8s_core.list_node()
            
            total_cpu = total_memory = 0.0
            available_cpu = available_memory = 0.0
            node_states = {}
            
            for node in nodes.items:
                # 노드 용량
                capacity = node.status.capacity
                node_cpu = self._parse_cpu(capacity.get('cpu', '0'))
                node_memory = self._parse_memory(capacity.get('memory', '0'))
                
                total_cpu += node_cpu
                total_memory += node_memory
                
                # 노드 할당 가능 리소스
                allocatable = node.status.allocatable
                alloc_cpu = self._parse_cpu(allocatable.get('cpu', '0'))
                alloc_memory = self._parse_memory(allocatable.get('memory', '0'))
                
                available_cpu += alloc_cpu
                available_memory += alloc_memory
                
                # 노드별 상태 저장
                node_states[node.metadata.name] = {
                    'cpu_capacity': node_cpu,
                    'memory_capacity': node_memory,
                    'cpu_allocatable': alloc_cpu,
                    'memory_allocatable': alloc_memory,
                    'ready': self._is_node_ready(node)
                }
                
            # 실행 중인 작업 수 계산
            running_jobs = 0
            pending_jobs = 0
            
            for namespace in self.config.namespaces:
                try:
                    jobs = self.k8s_batch.list_namespaced_job(namespace=namespace)
                    for job in jobs.items:
                        if job.status:
                            if job.status.active:
                                running_jobs += 1
                            elif not job.status.succeeded and not job.status.failed:
                                pending_jobs += 1
                except Exception as e:
                    self.logger.warning(f"네임스페이스 {namespace}의 작업 상태 확인 실패: {e}")
                            
            return ClusterState(
                timestamp=datetime.now(),
                total_cpu_capacity=total_cpu,
                total_memory_capacity=total_memory,
                available_cpu=available_cpu,
                available_memory=available_memory,
                pending_jobs_count=pending_jobs,
                running_jobs_count=running_jobs,
                queue_length=pending_jobs,  # 단순화
                node_count=len(nodes.items),
                node_states=node_states
            )
            
        except Exception as e:
            self.logger.error(f"클러스터 상태 수집 실패: {e}")
            return None
            
    def _is_node_ready(self, node) -> bool:
        """노드가 Ready 상태인지 확인"""
        if not node.status.conditions:
            return False
            
        for condition in node.status.conditions:
            if condition.type == 'Ready':
                return condition.status == 'True'
        return False
        
    def save_data(self, jobs: List[JobData], cluster_state: ClusterState):
        """수집된 데이터를 데이터베이스에 저장"""
        try:
            # 작업 데이터 저장
            for job in jobs:
                self.db_conn.execute('''
                    INSERT OR REPLACE INTO jobs VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job.job_id, job.name, job.namespace,
                    job.submission_time.isoformat() if job.submission_time else None,
                    job.start_time.isoformat() if job.start_time else None,
                    job.completion_time.isoformat() if job.completion_time else None,
                    job.cpu_request, job.memory_request, job.cpu_limit, job.memory_limit,
                    job.priority, job.user, job.queue_name, job.status,
                    job.restart_count, job.oom_killed,
                    json.dumps(job.spark_config) if job.spark_config else None
                ))
                
            # 클러스터 상태 저장
            if cluster_state:
                self.db_conn.execute('''
                    INSERT INTO cluster_states VALUES 
                    (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    cluster_state.timestamp.isoformat(),
                    cluster_state.total_cpu_capacity,
                    cluster_state.total_memory_capacity,
                    cluster_state.available_cpu,
                    cluster_state.available_memory,
                    cluster_state.pending_jobs_count,
                    cluster_state.running_jobs_count,
                    cluster_state.queue_length,
                    cluster_state.node_count,
                    json.dumps(cluster_state.node_states) if cluster_state.node_states else None
                ))
                
            self.db_conn.commit()
            self.logger.info(f"데이터 저장 완료: 작업 {len(jobs)}개, 클러스터 상태 1개")
            
        except Exception as e:
            self.logger.error(f"데이터 저장 실패: {e}")
            
    def _check_oom_status_safe(self, job, namespace: str) -> bool:
        """안전한 방식으로 작업의 OOM 발생 여부 확인"""
        try:
            # 작업과 연관된 팟들 확인
            pods = self.k8s_core.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job.metadata.name}"
            )
            
            for pod in pods.items:
                if pod.status.container_statuses:
                    for container_status in pod.status.container_statuses:
                        # 안전한 속성 접근
                        try:
                            # 현재 상태 확인
                            if (container_status.state and 
                                container_status.state.terminated and 
                                container_status.state.terminated.reason == 'OOMKilled'):
                                return True
                                
                            # 이전 상태 확인 (속성이 있는 경우에만)
                            if (container_status.last_state and
                                container_status.last_state.terminated and
                                container_status.last_state.terminated.reason == 'OOMKilled'):
                                return True
                                
                        except AttributeError:
                            # 속성이 없는 경우 무시하고 계속
                            continue
                            
            return False
            
        except Exception as e:
            self.logger.warning(f"OOM 상태 확인 실패: {e}")
            return False
            
    def _extract_spark_config(self, job) -> Optional[Dict]:
        """Spark 작업의 설정 정보 추출"""
        try:
            if job.metadata.labels and 'spark' in str(job.metadata.labels).lower():
                # Spark 관련 환경 변수나 레이블에서 설정 추출
                spark_config = {}
                
                if job.spec.template.spec.containers:
                    container = job.spec.template.spec.containers[0]
                    if container.env:
                        for env_var in container.env:
                            if env_var.name.startswith('SPARK_'):
                                spark_config[env_var.name] = env_var.value
                                
                return spark_config if spark_config else None
            return None
            
        except Exception:
            return None
            
    def _extract_priority(self, job) -> int:
        """작업 우선순위 추출"""
        try:
            if (job.spec.template.spec.priority_class_name or 
                (job.metadata.labels and 'priority' in job.metadata.labels)):
                # 우선순위 클래스나 레이블에서 추출
                return int(job.metadata.labels.get('priority', 0)) if job.metadata.labels else 0
            return 0
        except (ValueError, TypeError):
            return 0

    def _extract_priority_from_labels(self, labels: Dict) -> int:
        """레이블에서 우선순위 추출"""
        try:
            return int(labels.get('priority', 0))
        except (ValueError, TypeError):
            return 0

    def _extract_workload_data(self, workload, namespace: str) -> Optional[JobData]:
        """Kueue Workload에서 JobData 추출"""
        try:
            metadata = workload.get('metadata', {})
            spec = workload.get('spec', {})
            status = workload.get('status', {})
            
            # 기본 정보
            job_id = metadata.get('uid', 'unknown')
            name = metadata.get('name', 'unknown')
            submission_time = metadata.get('creationTimestamp')
            if submission_time:
                submission_time = datetime.fromisoformat(submission_time.replace('Z', '+00:00'))
            
            # 기본값으로 설정 (Workload에서는 정확한 리소스 추출이 복잡함)
            return JobData(
                job_id=job_id,
                name=name,
                namespace=namespace,
                submission_time=submission_time or datetime.now(),
                start_time=None,
                completion_time=None,
                cpu_request=1.0,  # 기본값
                memory_request=1.0,  # 기본값
                cpu_limit=2.0,
                memory_limit=2.0,
                priority=0,
                user='workload-user',
                queue_name=spec.get('queueName', 'default'),
                status='Pending',  # Workload는 보통 Pending 상태
                restart_count=0,
                oom_killed=False,
                spark_config=None
            )
            
        except Exception as e:
            self.logger.error(f"Workload 데이터 추출 실패: {e}")
            return None
            
    def export_training_data(self, output_file: str = "skrueue_training_data.csv"):
        """학습용 데이터셋을 CSV로 내보내기"""
        try:
            # 모든 데이터를 DataFrame으로 로드
            jobs_df = pd.read_sql_query("SELECT * FROM jobs", self.db_conn)
            cluster_df = pd.read_sql_query("SELECT * FROM cluster_states", self.db_conn)
            events_df = pd.read_sql_query("SELECT * FROM scheduling_events", self.db_conn)
            
            # 데이터 유무 확인
            if len(jobs_df) == 0:
                self.logger.warning("내보낼 작업 데이터가 없습니다.")
                return None
                
            if len(cluster_df) == 0:
                self.logger.warning("내보낼 클러스터 상태 데이터가 없습니다.")
                return None
            
            # 시계열 데이터 결합을 위한 전처리
            jobs_df['submission_time'] = pd.to_datetime(jobs_df['submission_time'])
            cluster_df['timestamp'] = pd.to_datetime(cluster_df['timestamp'])
            
            # 각 작업에 대해 제출 시점의 클러스터 상태를 매칭
            training_data = []
            
            for _, job in jobs_df.iterrows():
                # 작업 제출 시점과 가장 가까운 클러스터 상태 찾기
                time_diff = abs(cluster_df['timestamp'] - job['submission_time'])
                closest_state_idx = time_diff.idxmin()
                closest_state = cluster_df.loc[closest_state_idx]
                
                # 피처 구성
                features = {
                    # 작업 특성
                    'job_cpu_request': job['cpu_request'],
                    'job_memory_request': job['memory_request'],
                    'job_priority': job['priority'],
                    
                    # 클러스터 상태
                    'cluster_cpu_available': closest_state['available_cpu'],
                    'cluster_memory_available': closest_state['available_memory'],
                    'cluster_queue_length': closest_state['queue_length'],
                    'cluster_running_jobs': closest_state['running_jobs_count'],
                    
                    # 타겟 (레이블)
                    'completion_time': job['completion_time'],
                    'oom_occurred': job['oom_killed'],
                    'execution_success': 1 if job['status'] == 'Succeeded' else 0,
                    
                    # 시간 정보
                    'submission_timestamp': job['submission_time'],
                    'job_id': job['job_id']
                }
                
                training_data.append(features)
                
            # DataFrame 생성 및 저장
            training_df = pd.DataFrame(training_data)
            training_df.to_csv(output_file, index=False)
            
            self.logger.info(f"학습 데이터 내보내기 완료: {output_file} ({len(training_df)} 샘플)")
            return output_file
            
        except Exception as e:
            self.logger.error(f"학습 데이터 내보내기 실패: {e}")
            return None
            
    def run_collection_loop(self):
        """데이터 수집 메인 루프"""
        self.logger.info("SKRueue 데이터 수집 시작")
        
        try:
            while True:
                start_time = time.time()
                
                # 데이터 수집
                jobs = self.collect_job_data()
                cluster_state = self.collect_cluster_state()
                
                # 데이터 저장
                self.save_data(jobs, cluster_state)
                
                # 수집 간격 유지
                elapsed = time.time() - start_time
                sleep_time = max(0, self.config.collection_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 중단됨")
        except Exception as e:
            self.logger.error(f"수집 루프 오류: {e}")
        finally:
            self.db_conn.close()

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SKRueue RL 학습용 데이터 수집기')
    parser.add_argument('--interval', type=int, default=10, help='수집 간격 (초)')
    parser.add_argument('--export-only', action='store_true', help='기존 데이터만 내보내기')
    parser.add_argument('--output', type=str, default='skrueue_training_data.csv', help='출력 파일명')
    parser.add_argument('--namespaces', nargs='*', help='모니터링할 네임스페이스 목록')
    
    args = parser.parse_args()
    
    config = CollectionConfig(
        collection_interval=args.interval,
        namespaces=args.namespaces
    )
    
    collector = DataCollector(config)
    
    if args.export_only:
        # 기존 데이터만 내보내기
        output_file = collector.export_training_data(args.output)
        if output_file:
            print(f"✅ 학습 데이터 내보내기 완료: {output_file}")
        else:
            print("❌ 데이터 내보내기 실패")
    else:
        # 실시간 데이터 수집
        collector.run_collection_loop()

if __name__ == "__main__":
    main()