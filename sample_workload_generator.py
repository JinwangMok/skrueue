# """
# SKRueue 샘플 워크로드 생성기
# 다양한 유형의 Spark + Iceberg 배치 작업을 자동으로 생성하여 실험 환경 구축
# """

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
    """Spark 작업 생성기"""
    
    def __init__(self, namespace: str = "skrueue-test", 
                 image_repository: str = "apache/spark:3.4.0"):
        self.namespace = namespace
        self.image_repository = image_repository
        self.logger = logging.getLogger('SparkJobGenerator')
        self.job_counter = 0
        
        # 워크로드 템플릿 정의
        self.templates = self._define_templates()
        
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
        
    def _generate_python_code(self, template: WorkloadTemplate) -> str:
        """템플릿별 Python 코드 생성"""
        
        base_imports = """
import time
import random
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("{}").getOrCreate()
print(f"Starting job: {}")
start_time = time.time()

try:
""".format(template.name, template.description)

        # 템플릿별 특화 코드
        if template.category == "cpu-intensive":
            job_code = """
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
"""

        elif template.category == "memory-intensive":
            job_code = """
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
"""

        elif template.category == "io-intensive":
            job_code = """
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
"""

        elif template.category == "mixed":
            job_code = """
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
"""

        elif template.category == "lightweight":
            job_code = """
    # 경량 작업: 간단한 데이터 검증
    sample_data = [(i, f"value_{i}", random.choice([None, "valid", "invalid"])) 
                   for i in range(10000)]
    df = spark.createDataFrame(sample_data, ["id", "value", "status"])
    
    # 데이터 품질 검사
    total_count = df.count()
    null_count = df.filter(col("status").isNull()).count()
    valid_count = df.filter(col("status") == "valid").count()
    
    print(f"데이터 검증 완료 - 전체: {total_count}, NULL: {null_count}, 유효: {valid_count}")
"""

        else:  # memory-stress-test
            job_code = """
    # 메모리 스트레스 테스트: 의도적으로 OOM 유발
    print("메모리 스트레스 테스트 시작...")
    
    # 점진적으로 메모리 사용량 증가
    data_sizes = [1000000, 2000000, 5000000]  # 점점 큰 데이터셋
    
    for size in data_sizes:
        print(f"Processing dataset of size: {size}")
        large_data = [(i, f"data_{i}" * 100, random.random()) for i in range(size)]
        df = spark.createDataFrame(large_data, ["id", "large_text", "value"])
        
        # 메모리 집약적 연산
        df.cache()
        result = df.groupBy((col("id") % 10).alias("group")).agg(
            collect_list("large_text").alias("text_list"),  # 메모리 많이 사용
            sum("value").alias("total_value")
        )
        
        count = result.count()
        print(f"Processed {count} groups for size {size}")
        
        # 실패 확률 적용
        if random.random() < {}:
            print("Simulating failure...")
            raise Exception("Simulated OOM or processing failure")
            
        time.sleep(15)  # 메모리 압박 지속
""".format(template.failure_rate)

        cleanup_code = """
except Exception as e:
    print(f"Job failed with error: {e}")
    sys.exit(1)
finally:
    elapsed_time = time.time() - start_time
    print(f"Job completed in {elapsed_time:.2f} seconds")
    spark.stop()
"""

        return base_imports + job_code + cleanup_code

    def submit_job(self, template: WorkloadTemplate, 
                   additional_config: Dict = None) -> bool:
        """작업을 클러스터에 제출"""
        try:
            yaml_content = self.generate_spark_application_yaml(template, additional_config)
            
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
                job_name = f"{template.name}-{self.job_counter:04d}"
                self.logger.info(f"작업 제출 성공: {job_name}")
                return True
            else:
                self.logger.error(f"작업 제출 실패: {stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"작업 제출 중 오류: {e}")
            return False

class WorkloadGenerator:
    """워크로드 생성 메인 클래스"""
    
    def __init__(self, namespace: str = "skrueue-test"):
        self.namespace = namespace
        self.logger = logging.getLogger('WorkloadGenerator')
        self.spark_generator = SparkJobGenerator(namespace)
        
    def generate_realistic_workload_pattern(self, duration_hours: int = 24) -> List[Dict]:
        """현실적인 워크로드 패턴 생성"""
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
        """워크로드 시뮬레이션 실행"""
        self.logger.info(f"워크로드 시뮬레이션 시작: {len(schedule)}개 작업 예정")
        
        templates_map = {t.name: t for t in self.spark_generator.templates}
        submitted_count = 0
        
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
                    self.logger.info(f"진행률: {submitted_count}/{len(schedule)} "
                                   f"({submitted_count/len(schedule)*100:.1f}%)")
                else:
                    self.logger.error(f"작업 제출 실패: {template_name}")
            else:
                self.logger.error(f"알 수 없는 템플릿: {template_name}")
                
        self.logger.info(f"워크로드 시뮬레이션 완료: {submitted_count}개 작업 제출")
        
    def _add_job_variance(self, template: WorkloadTemplate) -> Dict:
        """작업에 변동성 추가"""
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
    parser = argparse.ArgumentParser(description='SKRueue 샘플 워크로드 생성기')
    parser.add_argument('--mode', choices=['single', 'batch', 'simulation'], 
                       default='simulation', help='실행 모드')
    parser.add_argument('--template', type=str, help='단일 템플릿 이름')
    parser.add_argument('--count', type=int, default=10, help='배치 모드 작업 수')
    parser.add_argument('--interval', type=int, default=60, help='배치 모드 간격(초)')
    parser.add_argument('--duration', type=int, default=24, help='시뮬레이션 시간(시간)')
    parser.add_argument('--namespace', type=str, default='skrueue-test', 
                       help='대상 네임스페이스')
    parser.add_argument('--dry-run', action='store_true', help='실제 제출 없이 YAML만 출력')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    generator = WorkloadGenerator(args.namespace)
    
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
            yaml_content = generator.spark_generator.generate_spark_application_yaml(template)
            print(yaml_content)
        else:
            success = generator.spark_generator.submit_job(template)
            print(f"작업 제출 {'성공' if success else '실패'}")
            
    elif args.mode == 'batch':
        # 배치 모드: 랜덤 작업들을 일정 간격으로 제출
        for i in range(args.count):
            template = random.choice(generator.spark_generator.templates)
            print(f"[{i+1}/{args.count}] 제출 중: {template.name}")
            
            if not args.dry_run:
                generator.spark_generator.submit_job(template)
                
            if i < args.count - 1:  # 마지막 작업이 아니면 대기
                time.sleep(args.interval)
                
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