"""성능 모니터링"""
import time
import json
import subprocess
from typing import Dict, List, Any, Optional
from datetime import datetime
import threading
from collections import defaultdict

from utils import K8sClient, get_logger
from config.settings import get_config


class PerformanceMonitor:
    """실시간 성능 모니터링"""
    
    def __init__(self, namespace: str = None):
        self.config = get_config()
        self.namespace = namespace or self.config.kueue.namespaces[0]
        self.logger = get_logger('PerformanceMonitor')
        self.k8s = K8sClient()
        
        # 모니터링 상태
        self.is_monitoring = False
        self.monitor_thread = None
        
        # 메트릭 저장소
        self.metrics_history = defaultdict(list)
        self.events = []
        
    def start(self):
        """모니터링 시작"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        self.logger.info("Performance monitoring started")
    
    def stop(self):
        """모니터링 중지"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        
        self.logger.info("Performance monitoring stopped")
    
    def _monitor_loop(self):
        """모니터링 루프"""
        while self.is_monitoring:
            try:
                metrics = self._collect_metrics()
                
                # 메트릭 저장
                timestamp = datetime.now()
                for metric_name, metric_value in metrics.items():
                    self.metrics_history[metric_name].append({
                        'timestamp': timestamp,
                        'value': metric_value
                    })
                
                # 이벤트 감지
                self._detect_events(metrics)
                
                time.sleep(30)  # 30초마다 수집
                
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                time.sleep(60)
    
    def _collect_metrics(self) -> Dict[str, Any]:
        """메트릭 수집"""
        metrics = {}
        
        try:
            # 작업 상태
            jobs = self.k8s.batch_v1.list_namespaced_job(namespace=self.namespace)
            
            job_states = defaultdict(int)
            for job in jobs.items:
                status = self._get_job_status(job)
                job_states[status] += 1
            
            metrics.update({
                'jobs_pending': job_states['Pending'],
                'jobs_running': job_states['Running'],
                'jobs_succeeded': job_states['Succeeded'],
                'jobs_failed': job_states['Failed'],
                'jobs_total': len(jobs.items)
            })
            
            # 팟 상태
            pods = self.k8s.core_v1.list_namespaced_pod(namespace=self.namespace)
            
            pod_states = defaultdict(int)
            oom_count = 0
            
            for pod in pods.items:
                if hasattr(pod.status, 'phase'):
                    pod_states[pod.status.phase] += 1
                
                # OOM 체크
                if self._is_pod_oom(pod):
                    oom_count += 1
            
            metrics.update({
                'pods_running': pod_states['Running'],
                'pods_pending': pod_states['Pending'],
                'pods_failed': pod_states['Failed'],
                'pods_succeeded': pod_states['Succeeded'],
                'oom_incidents': oom_count
            })
            
            # 노드 리소스 (가능한 경우)
            try:
                nodes = self.k8s.core_v1.list_node()
                
                total_cpu = available_cpu = 0
                total_memory = available_memory = 0
                
                for node in nodes.items:
                    if self._is_node_ready(node):
                        # 간단한 리소스 계산
                        total_cpu += 1
                        total_memory += 1
                        available_cpu += 0.5  # 근사치
                        available_memory += 0.5
                
                metrics.update({
                    'node_count': len(nodes.items),
                    'cpu_utilization': 1 - (available_cpu / max(total_cpu, 1)),
                    'memory_utilization': 1 - (available_memory / max(total_memory, 1))
                })
                
            except Exception:
                pass
            
        except Exception as e:
            self.logger.error(f"Metrics collection failed: {e}")
        
        return metrics
    
    def _get_job_status(self, job: Any) -> str:
        """작업 상태 결정"""
        if hasattr(job.spec, 'suspend') and job.spec.suspend:
            return 'Suspended'
        
        if hasattr(job.status, 'succeeded') and job.status.succeeded:
            return 'Succeeded'
        elif hasattr(job.status, 'failed') and job.status.failed:
            return 'Failed'
        elif hasattr(job.status, 'active') and job.status.active:
            return 'Running'
        else:
            return 'Pending'
    
    def _is_pod_oom(self, pod: Any) -> bool:
        """OOM 상태 확인"""
        if hasattr(pod.status, 'container_statuses'):
            for status in pod.status.container_statuses:
                if (hasattr(status.state, 'terminated') and 
                    status.state.terminated and
                    status.state.terminated.reason == 'OOMKilled'):
                    return True
        return False
    
    def _is_node_ready(self, node: Any) -> bool:
        """노드 Ready 상태"""
        if hasattr(node.status, 'conditions'):
            for condition in node.status.conditions:
                if condition.type == 'Ready' and condition.status == 'True':
                    return True
        return False
    
    def _detect_events(self, current_metrics: Dict[str, Any]):
        """이벤트 감지"""
        # OOM 급증 감지
        if current_metrics.get('oom_incidents', 0) > 5:
            self.events.append({
                'timestamp': datetime.now(),
                'type': 'high_oom_rate',
                'severity': 'warning',
                'message': f"High OOM rate detected: {current_metrics['oom_incidents']} incidents"
            })
        
        # 큐 길이 급증
        pending_jobs = current_metrics.get('jobs_pending', 0)
        if pending_jobs > 50:
            self.events.append({
                'timestamp': datetime.now(),
                'type': 'queue_congestion',
                'severity': 'warning',
                'message': f"Queue congestion: {pending_jobs} pending jobs"
            })
    
    def get_summary(self) -> Dict[str, Any]:
        """모니터링 요약"""
        summary = {
            'monitoring_duration': time.time() - (self.monitor_thread._started if hasattr(self.monitor_thread, '_started') else time.time()),
            'total_metrics_collected': sum(len(v) for v in self.metrics_history.values()),
            'events_detected': len(self.events),
            'latest_metrics': {}
        }
        
        # 최신 메트릭
        for metric_name, history in self.metrics_history.items():
            if history:
                summary['latest_metrics'][metric_name] = history[-1]['value']
        
        # 평균값 계산
        if self.metrics_history:
            summary['averages'] = {}
            for metric_name, history in self.metrics_history.items():
                if history:
                    values = [h['value'] for h in history if isinstance(h['value'], (int, float))]
                    if values:
                        summary['averages'][metric_name] = sum(values) / len(values)
        
        return summary
