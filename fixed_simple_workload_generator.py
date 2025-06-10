#!/usr/bin/env python3
"""
수정된 간단한 Kubernetes Job 워크로드 생성기
YAML 파싱 오류 수정 및 안정화
"""

import os
import time
import yaml
import random
import logging
import argparse
import subprocess
from datetime import datetime
from typing import Dict, List

class FixedSimpleJobGenerator:
    def __init__(self, namespace: str = "skrueue-test"):
        self.namespace = namespace
        self.job_counter = 0
        self.logger = logging.getLogger('FixedSimpleJobGenerator')
        
    def create_job_yaml(self, job_type: str = "cpu", duration: int = 60) -> str:
        """안전한 Kubernetes Job YAML 생성"""
        self.job_counter += 1
        job_name = f"test-{job_type}-{self.job_counter:04d}"
        timestamp = int(time.time())
        
        # 작업 유형별 설정
        job_configs = {
            "cpu": {
                "cpu_request": "500m",
                "memory_request": "256Mi",
                "cpu_limit": "1000m", 
                "memory_limit": "512Mi",
                "command": self._get_safe_cpu_command(duration)
            },
            "memory": {
                "cpu_request": "200m",
                "memory_request": "512Mi",
                "cpu_limit": "400m",
                "memory_limit": "1Gi", 
                "command": self._get_safe_memory_command(duration)
            },
            "io": {
                "cpu_request": "200m",
                "memory_request": "256Mi",
                "cpu_limit": "400m",
                "memory_limit": "512Mi",
                "command": self._get_safe_io_command(duration)
            },
            "short": {
                "cpu_request": "100m",
                "memory_request": "128Mi",
                "cpu_limit": "200m",
                "memory_limit": "256Mi",
                "command": self._get_safe_short_command(duration // 4)
            }
        }
        
        config = job_configs.get(job_type, job_configs["cpu"])
        
        job_yaml = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': job_name,
                'namespace': self.namespace,
                'labels': {
                    'app': 'skrueue-test',
                    'workload-type': job_type,
                    'scheduler': 'default-scheduler',
                    'priority': str(random.randint(1, 10)),
                    'created-by': 'fixed-workload-generator'
                },
                'annotations': {
                    'skrueue.ai/estimated-duration': str(duration),
                    'skrueue.ai/workload-category': job_type,
                    'skrueue.ai/created-at': str(timestamp)
                }
            },
            'spec': {
                'backoffLimit': 2,
                'activeDeadlineSeconds': duration * 3,  # 넉넉한 타임아웃
                'template': {
                    'metadata': {
                        'labels': {
                            'job-name': job_name,
                            'workload-type': job_type
                        }
                    },
                    'spec': {
                        'restartPolicy': 'Never',
                        'containers': [{
                            'name': 'worker',
                            'image': 'busybox:1.35',
                            'command': ['sh', '-c', config['command']],
                            'resources': {
                                'requests': {
                                    'cpu': config['cpu_request'],
                                    'memory': config['memory_request']
                                },
                                'limits': {
                                    'cpu': config['cpu_limit'],
                                    'memory': config['memory_limit']
                                }
                            },
                            'env': [
                                {'name': 'JOB_NAME', 'value': job_name},
                                {'name': 'JOB_TYPE', 'value': job_type},
                                {'name': 'DURATION', 'value': str(duration)},
                                {'name': 'TIMESTAMP', 'value': str(timestamp)}
                            ]
                        }]
                    }
                }
            }
        }
        
        return yaml.dump(job_yaml, default_flow_style=False)
    
    def _get_safe_cpu_command(self, duration: int) -> str:
        """안전한 CPU 집약적 명령어"""
        return f'''echo "CPU 집약적 작업 시작: $(date)"
echo "예상 실행 시간: {duration}초"
start_time=$(date +%s)
counter=0
while [ $(($(date +%s) - start_time)) -lt {duration} ]; do
    result=$(awk 'BEGIN {{for(i=1;i<=100;i++) sum+=sqrt(i*i); print sum}}')
    counter=$((counter + 1))
    if [ $((counter % 50)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "진행률: $((elapsed * 100 / {duration}))% (계산: $counter회)"
    fi
done
echo "CPU 작업 완료: $(date)"'''

    def _get_safe_memory_command(self, duration: int) -> str:
        """안전한 메모리 집약적 명령어"""
        return f'''echo "메모리 집약적 작업 시작: $(date)"
echo "예상 실행 시간: {duration}초"
start_time=$(date +%s)
file_count=0
while [ $(($(date +%s) - start_time)) -lt {duration} ]; do
    temp_file="/tmp/mem_$file_count"
    dd if=/dev/zero of="$temp_file" bs=1k count=100 2>/dev/null
    file_count=$((file_count + 1))
    if [ $((file_count % 10)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "진행률: $((elapsed * 100 / {duration}))% (파일: $file_count개)"
        ls -la /tmp/mem_* 2>/dev/null | wc -l
    fi
    sleep 2
done
rm -f /tmp/mem_* 2>/dev/null
echo "메모리 작업 완료: $(date)"'''

    def _get_safe_io_command(self, duration: int) -> str:
        """안전한 I/O 집약적 명령어"""
        return f'''echo "I/O 집약적 작업 시작: $(date)"
echo "예상 실행 시간: {duration}초"
start_time=$(date +%s)
operation_count=0
while [ $(($(date +%s) - start_time)) -lt {duration} ]; do
    test_file="/tmp/io_test_$operation_count"
    echo "테스트 데이터 $operation_count: $(date)" > "$test_file"
    for i in 1 2 3 4 5; do
        echo "라인 $i" >> "$test_file"
    done
    wc -l "$test_file" >/dev/null
    rm -f "$test_file"
    operation_count=$((operation_count + 1))
    if [ $((operation_count % 20)) -eq 0 ]; then
        elapsed=$(($(date +%s) - start_time))
        echo "진행률: $((elapsed * 100 / {duration}))% (I/O: $operation_count회)"
    fi
    sleep 1
done
echo "I/O 작업 완료: $(date)"'''

    def _get_safe_short_command(self, duration: int) -> str:
        """안전한 단시간 작업 명령어"""
        actual_duration = max(10, duration)  # 최소 10초
        return f'''echo "단시간 작업 시작: $(date)"
echo "실행 시간: {actual_duration}초"
for i in $(seq 1 {actual_duration}); do
    echo "작업 진행: $i/{actual_duration}"
    sleep 1
done
echo "단시간 작업 완료: $(date)"'''

    def submit_job(self, job_type: str = "cpu", duration: int = 60) -> bool:
        """작업을 클러스터에 제출"""
        try:
            yaml_content = self.create_job_yaml(job_type, duration)
            
            # 디버그용 YAML 저장
            debug_file = f"/tmp/debug_job_{job_type}_{self.job_counter}.yaml"
            with open(debug_file, 'w') as f:
                f.write(yaml_content)
            
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
                job_name = f"test-{job_type}-{self.job_counter:04d}"
                self.logger.info(f"작업 제출 성공: {job_name}")
                print(f"✅ 작업 제출 성공: {job_name}")
                print(f"   디버그 YAML: {debug_file}")
                return True
            else:
                self.logger.error(f"작업 제출 실패: {stderr}")
                print(f"❌ 작업 제출 실패: {stderr}")
                print(f"   디버그 YAML: {debug_file}")
                return False
                
        except Exception as e:
            self.logger.error(f"작업 제출 중 오류: {e}")
            print(f"❌ 작업 제출 중 오류: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='수정된 간단한 Kubernetes Job 워크로드 생성기')
    parser.add_argument('--count', type=int, default=5, help='생성할 작업 수')
    parser.add_argument('--interval', type=int, default=30, help='작업 간 간격(초)')
    parser.add_argument('--duration', type=int, default=60, help='각 작업 실행 시간(초)')
    parser.add_argument('--namespace', type=str, default='skrueue-test', help='대상 네임스페이스')
    parser.add_argument('--types', nargs='*', default=['cpu', 'memory', 'io', 'short'], 
                       help='작업 유형들')
    parser.add_argument('--dry-run', action='store_true', help='YAML만 출력하고 실제 제출하지 않음')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    generator = FixedSimpleJobGenerator(args.namespace)
    
    print(f"🚀 수정된 워크로드 생성기 시작")
    print(f"   - 네임스페이스: {args.namespace}")
    print(f"   - 작업 수: {args.count}")
    print(f"   - 간격: {args.interval}초")
    print(f"   - 작업 시간: {args.duration}초")
    print(f"   - 작업 유형: {args.types}")
    print(f"   - Dry Run: {args.dry_run}")
    print()
    
    success_count = 0
    
    for i in range(args.count):
        job_type = random.choice(args.types)
        duration = args.duration + random.randint(-10, 20)  # 변동성
        duration = max(15, duration)  # 최소 15초
        
        print(f"[{i+1}/{args.count}] {job_type} 작업 생성 중 (시간: {duration}초)...")
        
        if args.dry_run:
            yaml_content = generator.create_job_yaml(job_type, duration)
            print(f"--- Job {i+1} YAML ---")
            print(yaml_content[:500] + "..." if len(yaml_content) > 500 else yaml_content)
            print()
            success_count += 1
        else:
            if generator.submit_job(job_type, duration):
                success_count += 1
                
        if i < args.count - 1:  # 마지막이 아니면 대기
            time.sleep(args.interval)
    
    print(f"\n📊 작업 생성 완료: {success_count}/{args.count} 성공")
    if not args.dry_run:
        print(f"📋 생성된 작업 확인:")
        print(f"   kubectl get jobs -n {args.namespace}")
        print(f"   kubectl get pods -n {args.namespace}")
        print(f"   kubectl logs -n {args.namespace} job/test-cpu-0001")  # 예시

if __name__ == "__main__":
    main()