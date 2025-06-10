import subprocess
import json
import sqlite3
from datetime import datetime

print("🔄 강제 작업 수집 시작...")

# kubectl로 모든 작업 정보 가져오기
result = subprocess.run([
    'kubectl', 'get', 'jobs', '-n', 'skrueue-test', '-o', 'json'
], capture_output=True, text=True)

if result.returncode != 0:
    print("❌ kubectl 실행 실패")
    exit(1)

jobs_data = json.loads(result.stdout)
conn = sqlite3.connect('skrueue_training_data.db')
cursor = conn.cursor()

new_jobs = 0
for job in jobs_data['items']:
    try:
        metadata = job['metadata']
        spec = job['spec']
        status = job.get('status', {})
        
        # 작업 정보 추출
        job_id = metadata['uid']
        name = metadata['name']
        namespace = metadata['namespace']
        creation_time = metadata['creationTimestamp']
        
        # 이미 존재하는지 확인
        cursor.execute('SELECT 1 FROM jobs WHERE job_id = ?', (job_id,))
        if cursor.fetchone():
            continue
            
        # 리소스 정보 추출
        container = spec['template']['spec']['containers'][0]
        resources = container.get('resources', {}).get('requests', {})
        cpu_request = resources.get('cpu', '100m')
        memory_request = resources.get('memory', '128Mi')
        
        # CPU 파싱 (m 단위 처리)
        if cpu_request.endswith('m'):
            cpu_val = float(cpu_request[:-1]) / 1000
        else:
            cpu_val = float(cpu_request)
            
        # 메모리 파싱 (Mi/Gi 단위 처리)
        if memory_request.endswith('Gi'):
            mem_val = float(memory_request[:-2])
        elif memory_request.endswith('Mi'):
            mem_val = float(memory_request[:-2]) / 1024
        else:
            mem_val = 0.1
            
        # 상태 결정
        if status.get('succeeded'):
            job_status = 'Succeeded'
        elif status.get('failed'):
            job_status = 'Failed'
        elif status.get('active'):
            job_status = 'Running'
        elif spec.get('suspend', False):
            job_status = 'Suspended'
        else:
            job_status = 'Pending'
            
        # 데이터베이스에 삽입
        cursor.execute('''
            INSERT OR IGNORE INTO jobs (
                job_id, name, namespace, submission_time, start_time,
                completion_time, cpu_request, memory_request, cpu_limit,
                memory_limit, priority, user_name, queue_name, status,
                restart_count, oom_killed, spark_config
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            job_id, name, namespace, creation_time,
            status.get('startTime'), status.get('completionTime'),
            cpu_val, mem_val, cpu_val, mem_val,
            int(metadata.get('labels', {}).get('priority', '0')),
            'test-user', 'default', job_status,
            0, False, None
        ))
        
        if cursor.rowcount > 0:
            new_jobs += 1
            print(f"  ✅ 추가: {name} ({job_status})")
            
    except Exception as e:
        print(f"  ❌ 오류 ({name}): {e}")
        
conn.commit()
print(f"\n✅ 총 {new_jobs}개의 새 작업을 데이터베이스에 추가했습니다.")

# 최종 통계
cursor.execute('SELECT COUNT(*) FROM jobs')
total_jobs = cursor.fetchone()[0]
print(f"📊 전체 작업 수: {total_jobs}")

conn.close()
