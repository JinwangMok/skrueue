# sample_workload_generator.py
# SKRueue 샘플 워크로드 생성기
# 다양한 유형의 Spark + Iceberg 배치 작업을 자동으로 생성하여 실험 환경 구축
# Spark Operator 문제 대응 및 안정성 개선 버전

import os
import time
import yaml
import random
import logging
import argparse
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import json

@dataclass
class WorkloadTemplate:
    """워크로드 템플릿"""
    name: str
    category: str  # 'cpu-intensive', 'memory-intensive', 'io-intensive', 'mixed'
    description: str
    spark_config: Dict
    estimated_duration_minutes: int
    resource_requirements: Dict
    priority: int
    failure_rate: float = 0.0  # 의도적 실패율 (테스트용)

class SparkJobGenerator:
    """Spark 작업 생성기 - 안정성 개선 버전"""
    
    def __init__(self, namespace: str = "skrueue-test", 
                 image_repository: str = "apache/spark:3.4.0",
                 fallback_to_k8s_jobs: bool = True):
        self.namespace = namespace
        self.image_repository = image_repository
        self.fallback_to_k8s_jobs = fallback_to_k8s_jobs
        self.logger = logging.getLogger('SparkJobGenerator')
        self.job_counter = 0
        
        # 환경 상태 확인
        self.spark_operator_available = self._check_spark_operator()
        self.service_account_ready = self._check_service_account()
        
        # 워크로드 템플릿 정의
        self.templates = self._define_templates()
        
        # 상태 로깅
        self.logger.info(f"환경 상태 - Spark Operator: {self.spark_operator_available}, ServiceAccount: {self.service_account_ready}")
        
    def _check_spark_operator(self) -> bool:
        """Spark Operator 상태 확인"""
        try:
            result = subprocess.run(['kubectl', 'get', 'pods', '-n', 'spark-operator'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                # 웹훅 상태 확인
                webhook_result = subprocess.run(
                    ['kubectl', 'get', 'pods', '-n', 'spark-operator', '-l', 'app.kubernetes.io/name=spark-operator-webhook'],
                    capture_output=True, text=True
                )
                if 'CrashLoopBackOff' in webhook_result.stdout:
                    self.logger.warning("Spark Operator 웹훅이 CrashLoopBackOff 상태입니다.")
                    return False
                return True
            return False
        except Exception as e:
            self.logger.warning(f"Spark Operator 상태 확인 실패: {e}")
            return False
    
    def _check_service_account(self) -> bool:
        """ServiceAccount 존재 확인"""
        try:
            result = subprocess.run([
                'kubectl', 'get', 'serviceaccount', 'skrueue-agent', '-n', self.namespace
            ], capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            self.logger.warning(f"ServiceAccount 확인 실패: {e}")
            return False
    
    def _setup_service_account(self):
        """ServiceAccount 및 권한 설정"""
        try:
            self.logger.info("ServiceAccount 및 권한 설정 중...")
            
            # ServiceAccount 생성
            sa_yaml = {
                'apiVersion': 'v1',
                'kind': 'ServiceAccount',
                'metadata': {
                    'name': 'skrueue-agent',
                    'namespace': self.namespace
                }
            }
            
            # Role 생성
            role_yaml = {
                'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': 'Role',
                'metadata': {
                    'namespace': self.namespace,
                    'name': 'spark-role'
                },
                'rules': [
                    {
                        'apiGroups': [''],
                        'resources': ['pods', 'services', 'configmaps'],
                        'verbs': ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete']
                    },
                    {
                        'apiGroups': [''],
                        'resources': ['secrets'],
                        'verbs': ['get', 'list', 'watch']
                    }
                ]
            }
            
            # RoleBinding 생성
            binding_yaml = {
                'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': 'RoleBinding',
                'metadata': {
                    'name': 'spark-rolebinding',
                    'namespace': self.namespace
                },
                'subjects': [{
                    'kind': 'ServiceAccount',
                    'name': 'skrueue-agent',
                    'namespace': self.namespace
                }],
                'roleRef': {
                    'kind': 'Role',
                    'name': 'spark-role',
                    'apiGroup': 'rbac.authorization.k8s.io'
                }
            }
            
            # 리소스들 적용
            for yaml_content in [sa_yaml, role_yaml, binding_yaml]:
                self._apply_yaml(yaml_content)
            
            self.service_account_ready = True
            self.logger.info("ServiceAccount 설정 완료")
            
        except Exception as e:
            self.logger.error(f"ServiceAccount 설정 실패: {e}")
            
    def _apply_yaml(self, yaml_content: dict):
        """YAML 리소스 적용"""
        try:
            process = subprocess.Popen(
                ['kubectl', 'apply', '-f', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=yaml.dump(yaml_content))
            
            if process.returncode != 0:
                self.logger.warning(f"YAML 적용 경고: {stderr}")
                
        except Exception as e:
            self.logger.error(f"YAML 적용 실패: {e}")
        
    def _define_templates(self) -> List[WorkloadTemplate]:
        """다양한 워크로드 템플릿 정의"""
        return [
            # 1. CPU 집약적 작업 (머신러닝 훈련)
            WorkloadTemplate(
                name="ml-training",
                category="cpu-intensive",
                description="Machine Learning Training with Spark MLlib",
                spark_config={
                    "spark.executor.instances": "3",
                    "spark.executor.cores": "4", 
                    "spark.executor.memory": "4g",
                    "spark.driver.memory": "2g",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                },
                estimated_duration_minutes=45,
                resource_requirements={
                    "cpu": "4000m",
                    "memory": "6Gi"
                },
                priority=8
            ),
            
            # 2. 메모리 집약적 작업 (대용량 데이터 집계)
            WorkloadTemplate(
                name="big-data-aggregation",
                category="memory-intensive", 
                description="Large Dataset Aggregation with Window Functions",
                spark_config={
                    "spark.executor.instances": "4",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "8g",
                    "spark.driver.memory": "4g",
                    "spark.sql.shuffle.partitions": "400",
                    "spark.executor.memoryFraction": "0.8"
                },
                estimated_duration_minutes=60,
                resource_requirements={
                    "cpu": "2000m", 
                    "memory": "12Gi"
                },
                priority=6
            ),
            
            # 3. I/O 집약적 작업 (ETL 파이프라인)
            WorkloadTemplate(
                name="etl-pipeline",
                category="io-intensive",
                description="ETL Pipeline with Multiple Data Sources",
                spark_config={
                    "spark.executor.instances": "5",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "6g", 
                    "spark.driver.memory": "3g",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.sql.adaptive.skewJoin.enabled": "true"
                },
                estimated_duration_minutes=35,
                resource_requirements={
                    "cpu": "2000m",
                    "memory": "8Gi"
                },
                priority=5
            ),
            
            # 4. 혼합 워크로드 (실시간 분석)
            WorkloadTemplate(
                name="realtime-analytics",
                category="mixed",
                description="Real-time Analytics with Complex Joins",
                spark_config={
                    "spark.executor.instances": "3",
                    "spark.executor.cores": "3",
                    "spark.executor.memory": "5g",
                    "spark.driver.memory": "2g",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.localShuffleReader.enabled": "true"
                },
                estimated_duration_minutes=25,
                resource_requirements={
                    "cpu": "3000m",
                    "memory": "7Gi"
                },
                priority=7
            ),
            
            # 5. 경량 작업 (데이터 검증)
            WorkloadTemplate(
                name="data-validation",
                category="lightweight",
                description="Data Quality Validation and Profiling",
                spark_config={
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "1",
                    "spark.executor.memory": "2g",
                    "spark.driver.memory": "1g"
                },
                estimated_duration_minutes=15,
                resource_requirements={
                    "cpu": "1000m",
                    "memory": "3Gi"
                },
                priority=3
            ),
            
            # 6. 메모리 부족 유발 작업 (테스트용)
            WorkloadTemplate(
                name="memory-stress-test",
                category="memory-intensive",
                description="Memory Stress Test for OOM Simulation",
                spark_config={
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "6g",  # 실제로는 더 많이 사용
                    "spark.driver.memory": "2g",
                    "spark.sql.shuffle.partitions": "50"  # 적은 파티션으로 메모리 압박
                },
                estimated_duration_minutes=30,
                resource_requirements={
                    "cpu": "2000m",
                    "memory": "8Gi"  # 요청보다 실제 사용량이 많음
                },
                priority=4,
                failure_rate=0.3  # 30% 실패율
            )
        ]
        
    def generate_spark_application_yaml(self, template: WorkloadTemplate, 
                                      additional_config: Dict = None) -> str:
        """SparkApplication YAML 생성"""
        self.job_counter += 1
        job_name = f"{template.name}-{self.job_counter:04d}"
        
        # 추가 설정 병합
        spark_config = template.spark_config.copy()
        if additional_config:
            spark_config.update(additional_config)
            
        # Python 코드 생성 (템플릿별로 다른 작업 수행)
        python_code = self._generate_python_code(template)
        
        spark_app = {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {
                'name': job_name,
                'namespace': self.namespace,
                'labels': {
                    'app': 'skrueue-test',
                    'workload-type': template.category,
                    'scheduler': 'skrueue'
                },
                'annotations': {
                    'skrueue.ai/estimated-duration': str(template.estimated_duration_minutes),
                    'skrueue.ai/workload-category': template.category,
                    'skrueue.ai/description': template.description
                }
            },
            'spec': {
                'sparkVersion': self.image_repository.split(':')[-1],
                'type': 'Python',
                'pythonVersion': '3',
                'mode': 'cluster',
                'image': self.image_repository,
                'imagePullPolicy': 'IfNotPresent',
                'mainApplicationFile': 'local:///tmp/spark-job.py',
                'sparkConf': spark_config,
                'driver': {
                    'cores': 1,
                    'memory': spark_config.get('spark.driver.memory', '2g'),
                    'serviceAccount': 'skrueue-agent',
                    'labels': {
                        'version': '3.4.0',
                        'job-name': job_name
                    }
                },
                'executor': {
                    'cores': int(spark_config.get('spark.executor.cores', '2')),
                    'instances': int(spark_config.get('spark.executor.instances', '2')),
                    'memory': spark_config.get('spark.executor.memory', '4g'),
                    'labels': {
                        'version': '3.4.0',
                        'job-name': job_name
                    }
                },
                'restartPolicy': {
                    'type': 'Never'
                }
            }
        }
        
        # ConfigMap으로 Python 코드 주입
        configmap = {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': f"{job_name}-config",
                'namespace': self.namespace
            },
            'data': {
                'spark-job.py': python_code
            }
        }
        
        # Volume Mount 추가
        spark_app['spec']['driver']['volumeMounts'] = [{
            'name': 'job-config',
            'mountPath': '/tmp'
        }]
        spark_app['spec']['executor']['volumeMounts'] = [{
            'name': 'job-config', 
            'mountPath': '/tmp'
        }]
        
        spark_app['spec']['volumes'] = [{
            'name': 'job-config',
            'configMap': {
                'name': f"{job_name}-config"
            }
        }]
        
        # 두 개의 YAML 문서를 결합
        combined_yaml = yaml.dump(configmap) + "\n---\n" + yaml.dump(spark_app)
        
        return combined_yaml

    def generate_k8s_job_yaml(self, template: WorkloadTemplate) -> str:
        """대안으로 Kubernetes Job YAML 생성"""
        self.job_counter += 1
        job_name = f"k8s-{template.name}-{self.job_counter:04d}"
        
        # 리소스 요구사항 매핑
        resource_map = {
            "cpu-intensive": {"cpu": "1000m", "memory": "2Gi"},
            "memory-intensive": {"cpu": "500m", "memory": "4Gi"},
            "io-intensive": {"cpu": "800m", "memory": "1Gi"},
            "mixed": {"cpu": "800m", "memory": "2Gi"},
            "lightweight": {"cpu": "200m", "memory": "512Mi"}
        }
        
        resources = resource_map.get(template.category, {"cpu": "500m", "memory": "1Gi"})
        
        # 작업 스크립트 생성
        script = self._generate_k8s_job_script(template)
        
        job_yaml = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': job_name,
                'namespace': self.namespace,
                'labels': {
                    'app': 'skrueue-test',
                    'workload-type': template.category,
                    'scheduler': 'default-scheduler',
                    'priority': str(template.priority),
                    'template': template.name
                },
                'annotations': {
                    'skrueue.ai/estimated-duration': str(template.estimated_duration_minutes),
                    'skrueue.ai/workload-category': template.category,
                    'skrueue.ai/description': template.description,
                    'skrueue.ai/fallback-job': 'true'
                }
            },
            'spec': {
                'backoffLimit': 2,
                'activeDeadlineSeconds': template.estimated_duration_minutes * 60 * 2,
                'template': {
                    'metadata': {
                        'labels': {
                            'job-name': job_name,
                            'workload-type': template.category
                        }
                    },
                    'spec': {
                        'restartPolicy': 'Never',
                        'containers': [{
                            'name': 'worker',
                            'image': 'busybox:1.35',
                            'command': ['sh', '-c', script],
                            'resources': {
                                'requests': resources,
                                'limits': {
                                    'cpu': str(int(resources['cpu'][:-1]) * 2) + 'm',
                                    'memory': resources['memory']
                                }
                            },
                            'env': [
                                {'name': 'JOB_NAME', 'value': job_name},
                                {'name': 'JOB_TYPE', 'value': template.category},
                                {'name': 'DURATION', 'value': str(template.estimated_duration_minutes * 60)},
                                {'name': 'TEMPLATE', 'value': template.name}
                            ]
                        }]
                    }
                }
            }
        }
        
        return yaml.dump(job_yaml)

    def _generate_k8s_job_script(self, template: WorkloadTemplate) -> str:
        """Kubernetes Job용 스크립트 생성"""
        duration = template.estimated_duration_minutes * 60
        
        base_script = f'''echo "🚀 {template.description}"
echo "카테고리: {template.category}"
echo "예상 실행 시간: {template.estimated_duration_minutes}분"
echo "시작 시간: $(date)"

start_time=$(date +%s)
iteration=0
'''

        if template.category == "cpu-intensive":
            work_script = '''
# CPU 집약적 작업 시뮬레이션
while [ $(($(date +%s) - start_time)) -lt ''' + str(duration) + ''' ]; do
    for i in $(seq 1 1000); do
        result=$(awk 'BEGIN {for(i=1;i<=100;i++) sum+=sqrt(i*i); print sum}')
    done
    iteration=$((iteration + 1))
    if [ $((iteration % 10)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "CPU 작업 진행률: $((elapsed * 100 / ''' + str(duration) + '''))% (반복: $iteration)"
    fi
done'''

        elif template.category == "memory-intensive":
            work_script = '''
# 메모리 집약적 작업 시뮬레이션
while [ $(($(date +%s) - start_time)) -lt ''' + str(duration) + ''' ]; do
    temp_file="/tmp/mem_test_$(date +%s%N)"
    dd if=/dev/zero of="$temp_file" bs=1M count=50 2>/dev/null
    iteration=$((iteration + 1))
    if [ $((iteration % 5)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "메모리 작업 진행률: $((elapsed * 100 / ''' + str(duration) + '''))% (파일: $iteration개)"
        ls /tmp/mem_test_* 2>/dev/null | wc -l
    fi
    sleep 3
done
rm -f /tmp/mem_test_* 2>/dev/null'''

        elif template.category == "io-intensive":
            work_script = '''
# I/O 집약적 작업 시뮬레이션  
while [ $(($(date +%s) - start_time)) -lt ''' + str(duration) + ''' ]; do
    test_file="/tmp/io_test_$iteration"
    for i in $(seq 1 50); do
        echo "데이터 라인 $i: $(date +%s%N)" >> "$test_file"
    done
    wc -l "$test_file" >/dev/null
    rm -f "$test_file"
    iteration=$((iteration + 1))
    if [ $((iteration % 20)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "I/O 작업 진행률: $((elapsed * 100 / ''' + str(duration) + '''))% (파일 처리: $iteration개)"
    fi
    sleep 1
done'''

        else:  # mixed, lightweight
            work_script = '''
# 혼합/경량 작업 시뮬레이션
while [ $(($(date +%s) - start_time)) -lt ''' + str(duration) + ''' ]; do
    # 간단한 계산
    result=$(awk 'BEGIN {print sqrt(''' + str(random.randint(1, 1000)) + ''')}')
    
    # 간단한 파일 작업
    echo "작업 $iteration: $result" > "/tmp/work_$iteration"
    cat "/tmp/work_$iteration" >/dev/null
    rm -f "/tmp/work_$iteration"
    
    iteration=$((iteration + 1))
    if [ $((iteration % 30)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "작업 진행률: $((elapsed * 100 / ''' + str(duration) + '''))% (처리: $iteration회)"
    fi
    sleep 2
done'''

        end_script = '''
elapsed=$(($(date +%s) - start_time))
echo "✅ 작업 완료: $(date)"
echo "실제 실행 시간: ${elapsed}초"
echo "총 반복 횟수: $iteration"
'''

        return base_script + work_script + end_script
        
    def _generate_python_code(self, template: WorkloadTemplate) -> str:
        """템플릿별 Python 코드 생성 (기존 코드 유지)"""
        
        base_imports = f'''
import time
import random
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("{template.name}").getOrCreate()
print(f"Starting job: {template.description}")
start_time = time.time()

try:
'''

        # 템플릿별 특화 코드 (기존과 동일)
        if template.category == "cpu-intensive":
            job_code = '''
    # CPU 집약적 작업: 복잡한 수학 연산
    data = [(i, random.random(), random.random()) for i in range(1000000)]
    df = spark.createDataFrame(data, ["id", "x", "y"])
    
    # 복잡한 수학 연산 수행
    result = df.select(
        col("id"),
        sin(col("x")).alias("sin_x"),
        cos(col("y")).alias("cos_y"),
        sqrt(col("x") * col("x") + col("y") * col("y")).alias("distance"),
        when(col("x") > 0.5, exp(col("x"))).otherwise(log(col("x") + 1)).alias("complex_calc")
    ).groupBy((col("id") % 100).alias("bucket")).agg(
        avg("sin_x").alias("avg_sin"),
        max("cos_y").alias("max_cos"),
        sum("distance").alias("total_distance"),
        count("*").alias("count")
    )
    
    print(f"CPU 집약적 작업 완료. 결과 레코드 수: {result.count()}")
'''

        elif template.category == "memory-intensive":
            job_code = '''
    # 메모리 집약적 작업: 대용량 데이터 처리
    # 큰 데이터셋 생성
    large_data = [(i, f"user_{i}", random.randint(1, 1000), random.random() * 10000) 
                  for i in range(5000000)]  # 5M 레코드
    df1 = spark.createDataFrame(large_data, ["id", "user", "category", "amount"])
    
    # 메모리 집약적 윈도우 함수
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("category").orderBy("amount")
    
    result = df1.withColumn("rank", row_number().over(window_spec)) \\
               .withColumn("cumsum", sum("amount").over(window_spec)) \\
               .cache()  # 메모리에 캐시
    
    # 여러 번 접근하여 메모리 사용량 증가
    for i in range(3):
        count = result.filter(col("rank") <= 100).count()
        print(f"Iteration {i+1}: Top 100 per category count: {count}")
        time.sleep(10)  # 메모리 압박 지속
'''

        elif template.category == "io-intensive":
            job_code = '''
    # I/O 집약적 작업: 다중 조인 및 파일 처리
    # 여러 데이터셋 생성
    users = [(i, f"user_{i}", random.choice(["A", "B", "C"])) for i in range(100000)]
    orders = [(i, random.randint(1, 100000), random.random() * 1000) for i in range(500000)]
    products = [(i, f"product_{i}", random.random() * 100) for i in range(10000)]
    
    users_df = spark.createDataFrame(users, ["user_id", "user_name", "segment"])
    orders_df = spark.createDataFrame(orders, ["order_id", "user_id", "amount"])
    products_df = spark.createDataFrame(products, ["product_id", "product_name", "price"])
    
    # 복잡한 조인 연산
    result = orders_df.join(users_df, "user_id") \\
                     .join(products_df, orders_df.order_id % 10000 == products_df.product_id) \\
                     .groupBy("segment", "product_name") \\
                     .agg(
                         sum("amount").alias("total_sales"),
                         count("*").alias("order_count"),
                         avg("price").alias("avg_price")
                     )
    
    print(f"I/O 집약적 작업 완료. 결과 레코드 수: {result.count()}")
'''

        elif template.category == "mixed":
            job_code = '''
    # 혼합 워크로드: CPU + 메모리 + I/O
    # 시계열 데이터 생성
    import datetime
    
    time_series_data = []
    for i in range(1000000):
        timestamp = datetime.datetime.now() - datetime.timedelta(seconds=i)
        time_series_data.append((
            timestamp,
            random.choice(["sensor_1", "sensor_2", "sensor_3"]),
            random.random() * 100,
            random.randint(0, 1)
        ))
    
    df = spark.createDataFrame(time_series_data, 
                              ["timestamp", "sensor_id", "value", "status"])
    
    # 복잡한 시계열 분석
    result = df.withColumn("hour", hour("timestamp")) \\
               .groupBy("sensor_id", "hour") \\
               .agg(
                   avg("value").alias("avg_value"),
                   stddev("value").alias("stddev_value"),
                   max("value").alias("max_value"),
                   sum(when(col("status") == 1, 1).otherwise(0)).alias("active_count")
               ) \\
               .withColumn("anomaly_score", 
                          when(col("stddev_value") > 20, 1).otherwise(0))
    
    print(f"혼합 워크로드 완료. 결과 레코드 수: {result.count()}")
'''

        elif template.category == "lightweight":
            job_code = '''
    # 경량 작업: 간단한 데이터 검증
    sample_data = [(i, f"value_{i}", random.choice([None, "valid", "invalid"])) 
                   for i in range(10000)]
    df = spark.createDataFrame(sample_data, ["id", "value", "status"])
    
    # 데이터 품질 검사
    total_count = df.count()
    null_count = df.filter(col("status").isNull()).count()
    valid_count = df.filter(col("status") == "valid").count()
    
    print(f"데이터 검증 완료 - 전체: {total_count}, NULL: {null_count}, 유효: {valid_count}")
'''

        else:  # memory-stress-test
            job_code = f'''
    # 메모리 스트레스 테스트: 의도적으로 OOM 유발
    print("메모리 스트레스 테스트 시작...")
    
    # 점진적으로 메모리 사용량 증가
    data_sizes = [1000000, 2000000, 5000000]  # 점점 큰 데이터셋
    
    for size in data_sizes:
        print(f"Processing dataset of size: {{size}}")
        large_data = [(i, f"data_{{i}}" * 100, random.random()) for i in range(size)]
        df = spark.createDataFrame(large_data, ["id", "large_text", "value"])
        
        # 메모리 집약적 연산
        df.cache()
        result = df.groupBy((col("id") % 10).alias("group")).agg(
            collect_list("large_text").alias("text_list"),  # 메모리 많이 사용
            sum("value").alias("total_value")
        )
        
        count = result.count()
        print(f"Processed {{count}} groups for size {{size}}")
        
        # 실패 확률 적용
        if random.random() < {template.failure_rate}:
            print("Simulating failure...")
            raise Exception("Simulated OOM or processing failure")
            
        time.sleep(15)  # 메모리 압박 지속
'''

        cleanup_code = '''
except Exception as e:
    print(f"Job failed with error: {e}")
    sys.exit(1)
finally:
    elapsed_time = time.time() - start_time
    print(f"Job completed in {elapsed_time:.2f} seconds")
    spark.stop()
'''

        return base_imports + job_code + cleanup_code

    def submit_job(self, template: WorkloadTemplate, 
                   additional_config: Dict = None, force_k8s_job: bool = False) -> bool:
        """작업을 클러스터에 제출 - 안정성 개선"""
        
        # ServiceAccount 확인 및 설정
        if not self.service_account_ready:
            self._setup_service_account()
        
        # SparkApplication 또는 Kubernetes Job 결정
        use_spark = (self.spark_operator_available and 
                    self.service_account_ready and 
                    not force_k8s_job)
        
        try:
            if use_spark:
                yaml_content = self.generate_spark_application_yaml(template, additional_config)
                job_type = "SparkApplication"
            else:
                yaml_content = self.generate_k8s_job_yaml(template)
                job_type = "Kubernetes Job"
                
            self.logger.info(f"제출할 작업 유형: {job_type}")
            
            # kubectl apply로 제출
            process = subprocess.Popen(
                ['kubectl', 'apply', '-f', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=yaml_content)
            
            if process.returncode == 0:
                if use_spark:
                    job_name = f"{template.name}-{self.job_counter:04d}"
                else:
                    job_name = f"k8s-{template.name}-{self.job_counter:04d}"
                    
                self.logger.info(f"작업 제출 성공: {job_name} ({job_type})")
                
                # 작업 상태 확인 (5초 후)
                time.sleep(5)
                self._verify_job_status(job_name, use_spark)
                
                return True
            else:
                self.logger.error(f"작업 제출 실패: {stderr}")
                
                # SparkApplication 실패 시 Kubernetes Job으로 재시도
                if use_spark and self.fallback_to_k8s_jobs:
                    self.logger.info("SparkApplication 실패, Kubernetes Job으로 재시도...")
                    return self.submit_job(template, additional_config, force_k8s_job=True)
                    
                return False
                
        except Exception as e:
            self.logger.error(f"작업 제출 중 오류: {e}")
            
            # 예외 발생 시도 Kubernetes Job으로 재시도
            if use_spark and self.fallback_to_k8s_jobs:
                self.logger.info("예외 발생, Kubernetes Job으로 재시도...")
                return self.submit_job(template, additional_config, force_k8s_job=True)
                
            return False

    def _verify_job_status(self, job_name: str, is_spark_app: bool):
        """작업 상태 확인"""
        try:
            if is_spark_app:
                result = subprocess.run([
                    'kubectl', 'get', 'sparkapplication', job_name, '-n', self.namespace
                ], capture_output=True, text=True)
            else:
                result = subprocess.run([
                    'kubectl', 'get', 'job', job_name, '-n', self.namespace
                ], capture_output=True, text=True)
                
            if result.returncode == 0:
                self.logger.info(f"작업 상태 확인 성공: {job_name}")
            else:
                self.logger.warning(f"작업 상태 확인 실패: {job_name}")
                
        except Exception as e:
            self.logger.warning(f"작업 상태 확인 중 오류: {e}")

class WorkloadGenerator:
    """워크로드 생성 메인 클래스 - 안정성 개선 버전"""
    
    def __init__(self, namespace: str = "skrueue-test"):
        self.namespace = namespace
        self.logger = logging.getLogger('WorkloadGenerator')
        self.spark_generator = SparkJobGenerator(namespace)
        
    def generate_realistic_workload_pattern(self, duration_hours: int = 24) -> List[Dict]:
        """현실적인 워크로드 패턴 생성 (기존과 동일)"""
        workload_schedule = []
        
        # 시간대별 워크로드 패턴 정의
        hourly_patterns = {
            # 새벽 시간 (0-6시): 배치 작업 중심
            range(0, 6): {
                'job_rate': 2,  # 시간당 작업 수
                'template_weights': {
                    'etl-pipeline': 0.4,
                    'big-data-aggregation': 0.3,
                    'data-validation': 0.2,
                    'memory-stress-test': 0.1
                }
            },
            # 업무 시간 (9-18시): 다양한 분석 작업
            range(9, 18): {
                'job_rate': 5,
                'template_weights': {
                    'realtime-analytics': 0.3,
                    'ml-training': 0.25,
                    'etl-pipeline': 0.2,
                    'big-data-aggregation': 0.15,
                    'data-validation': 0.1
                }
            },
            # 저녁 시간 (19-23시): 보고서 생성 및 분석
            range(19, 24): {
                'job_rate': 3,
                'template_weights': {
                    'big-data-aggregation': 0.35,
                    'ml-training': 0.25,
                    'realtime-analytics': 0.2,
                    'etl-pipeline': 0.15,
                    'memory-stress-test': 0.05
                }
            }
        }
        
        # 기본 패턴 (다른 시간대)
        default_pattern = {
            'job_rate': 1,
            'template_weights': {
                'data-validation': 0.4,
                'etl-pipeline': 0.3,
                'realtime-analytics': 0.2,
                'memory-stress-test': 0.1
            }
        }
        
        # 스케줄 생성
        start_time = datetime.now()
        
        for hour in range(duration_hours):
            current_hour = (start_time.hour + hour) % 24
            
            # 해당 시간대 패턴 찾기
            pattern = default_pattern
            for hour_range, hour_pattern in hourly_patterns.items():
                if current_hour in hour_range:
                    pattern = hour_pattern
                    break
                    
            # 해당 시간 동안 제출할 작업들 생성
            num_jobs = max(1, int(random.gauss(pattern['job_rate'], 1)))
            
            for job_idx in range(num_jobs):
                # 템플릿 선택 (가중치 기반)
                templates = list(pattern['template_weights'].keys())
                weights = list(pattern['template_weights'].values())
                selected_template = random.choices(templates, weights=weights)[0]
                
                # 제출 시간 (해당 시간 내 랜덤)
                job_time = start_time + timedelta(
                    hours=hour,
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                workload_schedule.append({
                    'template_name': selected_template,
                    'submit_time': job_time,
                    'hour': current_hour
                })
                
        # 시간순 정렬
        workload_schedule.sort(key=lambda x: x['submit_time'])
        
        return workload_schedule
        
    def run_workload_simulation(self, schedule: List[Dict]):
        """워크로드 시뮬레이션 실행 - 안정성 개선"""
        self.logger.info(f"워크로드 시뮬레이션 시작: {len(schedule)}개 작업 예정")
        
        templates_map = {t.name: t for t in self.spark_generator.templates}
        submitted_count = 0
        failed_count = 0
        
        for job_info in schedule:
            # 제출 시간까지 대기
            now = datetime.now()
            wait_time = (job_info['submit_time'] - now).total_seconds()
            
            if wait_time > 0:
                self.logger.info(f"다음 작업까지 {wait_time:.1f}초 대기...")
                time.sleep(min(wait_time, 300))  # 최대 5분까지만 대기
                
            # 작업 제출
            template_name = job_info['template_name']
            if template_name in templates_map:
                template = templates_map[template_name]
                
                # 약간의 변동성 추가
                additional_config = self._add_job_variance(template)
                
                success = self.spark_generator.submit_job(template, additional_config)
                if success:
                    submitted_count += 1
                else:
                    failed_count += 1
                    
                self.logger.info(f"진행률: {submitted_count + failed_count}/{len(schedule)} "
                               f"(성공: {submitted_count}, 실패: {failed_count})")
            else:
                self.logger.error(f"알 수 없는 템플릿: {template_name}")
                failed_count += 1
                
        self.logger.info(f"워크로드 시뮬레이션 완료: {submitted_count}개 작업 제출 성공, {failed_count}개 실패")
        
    def _add_job_variance(self, template: WorkloadTemplate) -> Dict:
        """작업에 변동성 추가 (기존과 동일)"""
        variance_config = {}
        
        # 실행자 수 변동 (±1)
        base_instances = int(template.spark_config.get('spark.executor.instances', '2'))
        variance_config['spark.executor.instances'] = str(max(1, 
            base_instances + random.randint(-1, 1)))
            
        # 메모리 변동 (±20%)
        base_memory = template.spark_config.get('spark.executor.memory', '4g')
        memory_value = int(base_memory.replace('g', ''))
        variance_factor = random.uniform(0.8, 1.2)
        new_memory = max(1, int(memory_value * variance_factor))
        variance_config['spark.executor.memory'] = f"{new_memory}g"
        
        return variance_config

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='SKRueue 샘플 워크로드 생성기 - 안정성 개선 버전')
    parser.add_argument('--mode', choices=['single', 'batch', 'simulation'], 
                       default='simulation', help='실행 모드')
    parser.add_argument('--template', type=str, help='단일 템플릿 이름')
    parser.add_argument('--count', type=int, default=10, help='배치 모드 작업 수')
    parser.add_argument('--interval', type=int, default=60, help='배치 모드 간격(초)')
    parser.add_argument('--duration', type=int, default=24, help='시뮬레이션 시간(시간)')
    parser.add_argument('--namespace', type=str, default='skrueue-test', 
                       help='대상 네임스페이스')
    parser.add_argument('--dry-run', action='store_true', help='실제 제출 없이 YAML만 출력')
    parser.add_argument('--force-k8s-jobs', action='store_true', 
                       help='SparkApplication 대신 Kubernetes Job 강제 사용')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    generator = WorkloadGenerator(args.namespace)
    
    # Kubernetes Job 강제 사용 설정
    if args.force_k8s_jobs:
        generator.spark_generator.fallback_to_k8s_jobs = True
        generator.spark_generator.spark_operator_available = False
    
    if args.mode == 'single':
        # 단일 작업 제출
        if not args.template:
            print("사용 가능한 템플릿:")
            for template in generator.spark_generator.templates:
                print(f"  - {template.name}: {template.description}")
            return
            
        template = next((t for t in generator.spark_generator.templates 
                        if t.name == args.template), None)
        if not template:
            print(f"템플릿을 찾을 수 없음: {args.template}")
            return
            
        if args.dry_run:
            if args.force_k8s_jobs:
                yaml_content = generator.spark_generator.generate_k8s_job_yaml(template)
            else:
                yaml_content = generator.spark_generator.generate_spark_application_yaml(template)
            print(yaml_content)
        else:
            success = generator.spark_generator.submit_job(template, force_k8s_job=args.force_k8s_jobs)
            print(f"작업 제출 {'성공' if success else '실패'}")
            
    elif args.mode == 'batch':
        # 배치 모드: 랜덤 작업들을 일정 간격으로 제출
        success_count = 0
        for i in range(args.count):
            template = random.choice(generator.spark_generator.templates)
            print(f"[{i+1}/{args.count}] 제출 중: {template.name}")
            
            if not args.dry_run:
                if generator.spark_generator.submit_job(template, force_k8s_job=args.force_k8s_jobs):
                    success_count += 1
                    
            if i < args.count - 1:  # 마지막 작업이 아니면 대기
                time.sleep(args.interval)
                
        if not args.dry_run:
            print(f"배치 작업 완료: {success_count}/{args.count} 성공")
                
    elif args.mode == 'simulation':
        # 시뮬레이션 모드: 현실적인 워크로드 패턴
        schedule = generator.generate_realistic_workload_pattern(args.duration)
        
        if args.dry_run:
            print(f"생성된 스케줄 ({len(schedule)}개 작업):")
            for job_info in schedule[:10]:  # 처음 10개만 출력
                print(f"  {job_info['submit_time']}: {job_info['template_name']}")
            print("...")
        else:
            generator.run_workload_simulation(schedule)

if __name__ == "__main__":
    main()