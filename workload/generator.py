"""통합 워크로드 생성기"""
import time
import yaml
from typing import Dict, List, Optional, Union
from datetime import datetime
import subprocess

from utils import K8sClient, get_logger
from config.settings import get_config
from .templates import JobTemplate, JobTemplateFactory
from .strategies import WorkloadStrategy, UniformStrategy, RealisticStrategy, BurstStrategy


class WorkloadGenerator:
    """통합 워크로드 생성기"""
    
    def __init__(self, namespace: str = None, strategy: WorkloadStrategy = None):
        self.config = get_config()
        self.namespace = namespace or self.config.kueue.namespaces[0]
        self.logger = get_logger('WorkloadGenerator')
        self.k8s = K8sClient()
        
        # 전략 설정
        self.strategy = strategy or RealisticStrategy()
        
        # 템플릿
        self.templates = JobTemplateFactory.get_default_templates()
        
        # 카운터
        self.job_counter = 0
        self.submitted_count = 0
        self.failed_count = 0
    
    def generate_job_yaml(self, template: JobTemplate, suspend: bool = True) -> str:
        """Job YAML 생성"""
        self.job_counter += 1
        job_name = f"{template.name}-{self.job_counter:05d}"
        
        job_spec = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': job_name,
                'namespace': self.namespace,
                'labels': {
                    'app': 'skrueue',
                    'category': template.category,
                    'priority': str(template.priority),
                    'generator': 'workload-generator'
                },
                'annotations': {
                    'skrueue.ai/estimated-duration': str(template.estimated_duration),
                    'skrueue.ai/job-type': template.category,
                    'skrueue.ai/created-at': datetime.now().isoformat()
                }
            },
            'spec': {
                'suspend': suspend,
                'backoffLimit': 2,
                'activeDeadlineSeconds': template.estimated_duration * 60 * 3,
                'template': {
                    'metadata': {
                        'labels': {
                            'app': 'skrueue',
                            'job-name': job_name
                        }
                    },
                    'spec': {
                        'restartPolicy': 'Never',
                        'containers': [{
                            'name': 'worker',
                            'image': template.image,
                            'command': template.command or ["sleep", str(template.estimated_duration * 60)],
                            'resources': {
                                'requests': {
                                    'cpu': template.cpu_request,
                                    'memory': template.memory_request
                                },
                                'limits': {
                                    'cpu': template.cpu_limit,
                                    'memory': template.memory_limit
                                }
                            },
                            'env': self._build_env_vars(template, job_name)
                        }]
                    }
                }
            }
        }
        
        return yaml.dump(job_spec, default_flow_style=False)
    
    def generate_spark_yaml(self, template: JobTemplate) -> str:
        """SparkApplication YAML 생성"""
        if not template.spark_config:
            raise ValueError(f"Template {template.name} has no Spark configuration")
        
        self.job_counter += 1
        job_name = f"spark-{template.name}-{self.job_counter:05d}"
        
        spark_app = {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {
                'name': job_name,
                'namespace': self.namespace,
                'labels': {
                    'app': 'skrueue',
                    'category': template.category,
                    'priority': str(template.priority)
                }
            },
            'spec': {
                'type': 'Python',
                'pythonVersion': '3',
                'mode': 'cluster',
                'image': 'apache/spark:3.4.0',
                'imagePullPolicy': 'IfNotPresent',
                'mainApplicationFile': 'local:///tmp/spark-job.py',
                'sparkVersion': '3.4.0',
                'sparkConf': template.spark_config,
                'driver': {
                    'cores': 1,
                    'memory': template.spark_config.get('spark.driver.memory', '2g'),
                    'serviceAccount': 'spark-sa'
                },
                'executor': {
                    'cores': int(template.spark_config.get('spark.executor.cores', '2')),
                    'instances': int(template.spark_config.get('spark.executor.instances', '2')),
                    'memory': template.spark_config.get('spark.executor.memory', '4g')
                },
                'restartPolicy': {
                    'type': 'Never'
                }
            }
        }
        
        return yaml.dump(spark_app, default_flow_style=False)
    
    def submit_job(self, template: JobTemplate, suspend: bool = True, 
                   use_spark: bool = False) -> bool:
        """작업 제출"""
        try:
            if use_spark and template.spark_config:
                yaml_content = self.generate_spark_yaml(template)
                job_type = "SparkApplication"
            else:
                yaml_content = self.generate_job_yaml(template, suspend)
                job_type = "Job"
            
            # 제출
            success = self.k8s.apply_yaml(yaml_content)
            
            if success:
                self.submitted_count += 1
                self.logger.info(f"Submitted {job_type}: {template.name}-{self.job_counter:05d}")
            else:
                self.failed_count += 1
                self.logger.error(f"Failed to submit {job_type}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Job submission error: {e}")
            self.failed_count += 1
            return False
    
    def run_simulation(self, duration_hours: int, realtime: bool = True):
        """워크로드 시뮬레이션 실행"""
        self.logger.info(f"Starting workload simulation for {duration_hours} hours")
        
        # 스케줄 생성
        schedule = self.strategy.generate_schedule(duration_hours)
        
        self.logger.info(f"Generated schedule with {len(schedule)} jobs")
        
        # 실행
        start_time = time.time()
        
        for job_info in schedule:
            # 제출 시간까지 대기 (실시간 모드)
            if realtime:
                now = datetime.now()
                wait_time = (job_info['submit_time'] - now).total_seconds()
                
                if wait_time > 0:
                    self.logger.info(f"Waiting {wait_time:.1f}s until next job")
                    time.sleep(min(wait_time, 300))  # 최대 5분 대기
            
            # 작업 제출
            template = job_info['template']
            success = self.submit_job(template)
            
            # 진행 상황 로깅
            if self.submitted_count % 10 == 0:
                self.logger.info(
                    f"Progress: {self.submitted_count} submitted, "
                    f"{self.failed_count} failed"
                )
        
        # 최종 통계
        elapsed = time.time() - start_time
        self.logger.info(
            f"Simulation completed in {elapsed/60:.1f} minutes: "
            f"{self.submitted_count} submitted, {self.failed_count} failed"
        )
    
    def _build_env_vars(self, template: JobTemplate, job_name: str) -> List[Dict[str, str]]:
        """환경 변수 구성"""
        env_vars = [
            {'name': 'JOB_NAME', 'value': job_name},
            {'name': 'JOB_CATEGORY', 'value': template.category},
            {'name': 'ESTIMATED_DURATION', 'value': str(template.estimated_duration)},
            {'name': 'PRIORITY', 'value': str(template.priority)}
        ]
        
        if template.env_vars:
            for key, value in template.env_vars.items():
                env_vars.append({'name': key, 'value': value})
        
        return env_vars


def create_generator(strategy_name: str = "realistic", namespace: str = None) -> WorkloadGenerator:
    """워크로드 생성기 팩토리 함수"""
    strategies = {
        "uniform": UniformStrategy(),
        "realistic": RealisticStrategy(),
        "burst": BurstStrategy()
    }
    
    strategy = strategies.get(strategy_name, RealisticStrategy())
    return WorkloadGenerator(namespace=namespace, strategy=strategy)