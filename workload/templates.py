"""작업 템플릿 정의"""
from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class JobTemplate:
    """작업 템플릿"""
    name: str
    category: str
    description: str
    cpu_request: str
    memory_request: str
    cpu_limit: str
    memory_limit: str
    estimated_duration: int  # 분
    priority: int
    image: str = "busybox:1.35"
    command: Optional[List[str]] = None
    spark_config: Optional[Dict[str, str]] = None
    env_vars: Optional[Dict[str, str]] = None
    failure_rate: float = 0.0


class JobTemplateFactory:
    """작업 템플릿 팩토리"""
    
    @staticmethod
    def get_default_templates() -> Dict[str, JobTemplate]:
        """기본 템플릿 세트"""
        return {
            "cpu-intensive": JobTemplate(
                name="cpu-task",
                category="cpu-intensive",
                description="CPU intensive computation task",
                cpu_request="1000m",
                memory_request="2Gi",
                cpu_limit="2000m",
                memory_limit="4Gi",
                estimated_duration=30,
                priority=5,
                command=[
                    "sh", "-c",
                    "echo 'Starting CPU intensive task'; "
                    "for i in $(seq 1 60); do "
                    "  awk 'BEGIN {for(j=1;j<=10000;j++) sum+=sqrt(j*j)}'; "
                    "  echo \"Progress: $i/60\"; "
                    "  sleep 30; "
                    "done; "
                    "echo 'Task completed'"
                ]
            ),
            
            "memory-intensive": JobTemplate(
                name="memory-task",
                category="memory-intensive",
                description="Memory intensive data processing",
                cpu_request="500m",
                memory_request="4Gi",
                cpu_limit="1000m",
                memory_limit="8Gi",
                estimated_duration=45,
                priority=4,
                command=[
                    "sh", "-c",
                    "echo 'Starting memory intensive task'; "
                    "for i in $(seq 1 45); do "
                    "  dd if=/dev/zero of=/tmp/data_$i bs=100M count=10 2>/dev/null; "
                    "  echo \"Memory allocated: $i/45\"; "
                    "  sleep 60; "
                    "  rm -f /tmp/data_$i; "
                    "done; "
                    "echo 'Task completed'"
                ]
            ),
            
            "io-intensive": JobTemplate(
                name="io-task",
                category="io-intensive",
                description="I/O intensive file operations",
                cpu_request="300m",
                memory_request="1Gi",
                cpu_limit="600m",
                memory_limit="2Gi",
                estimated_duration=25,
                priority=3,
                command=[
                    "sh", "-c",
                    "echo 'Starting I/O intensive task'; "
                    "for i in $(seq 1 50); do "
                    "  for j in $(seq 1 100); do "
                    "    echo \"Data line $j\" >> /tmp/output_$i.txt; "
                    "  done; "
                    "  sort /tmp/output_$i.txt > /tmp/sorted_$i.txt; "
                    "  rm -f /tmp/output_$i.txt /tmp/sorted_$i.txt; "
                    "  echo \"I/O operation $i/50 completed\"; "
                    "  sleep 30; "
                    "done; "
                    "echo 'Task completed'"
                ]
            ),
            
            "ml-training": JobTemplate(
                name="ml-training",
                category="ml-training",
                description="Machine learning model training",
                cpu_request="2000m",
                memory_request="8Gi",
                cpu_limit="4000m",
                memory_limit="16Gi",
                estimated_duration=60,
                priority=8,
                spark_config={
                    "spark.executor.instances": "3",
                    "spark.executor.cores": "4",
                    "spark.executor.memory": "4g",
                    "spark.driver.memory": "2g",
                    "spark.sql.adaptive.enabled": "true"
                }
            ),
            
            "etl-pipeline": JobTemplate(
                name="etl-pipeline",
                category="etl",
                description="ETL data pipeline processing",
                cpu_request="1500m",
                memory_request="6Gi",
                cpu_limit="3000m",
                memory_limit="12Gi",
                estimated_duration=40,
                priority=6,
                spark_config={
                    "spark.executor.instances": "4",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "3g",
                    "spark.driver.memory": "2g",
                    "spark.sql.shuffle.partitions": "200"
                }
            ),
            
            "quick-task": JobTemplate(
                name="quick-task",
                category="lightweight",
                description="Quick lightweight task",
                cpu_request="100m",
                memory_request="256Mi",
                cpu_limit="200m",
                memory_limit="512Mi",
                estimated_duration=5,
                priority=2,
                command=[
                    "sh", "-c",
                    "echo 'Quick task started'; "
                    "sleep 300; "
                    "echo 'Quick task completed'"
                ]
            )
        }