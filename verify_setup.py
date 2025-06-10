# verify_setup.py
#!/usr/bin/env python3
import subprocess
import json

print("🔍 SKRueue 환경 검증")
print("=" * 50)

# 1. Suspended 작업 확인
result = subprocess.run([
    'kubectl', 'get', 'jobs', '-n', 'skrueue-test', 
    '-o', 'json'
], capture_output=True, text=True)

if result.returncode == 0:
    jobs_data = json.loads(result.stdout)
    suspended_count = 0
    running_count = 0
    
    for job in jobs_data.get('items', []):
        if job['spec'].get('suspend', False):
            suspended_count += 1
            print(f"✅ Suspended: {job['metadata']['name']}")
        elif job['status'].get('active', 0) > 0:
            running_count += 1
    
    print(f"\n📊 작업 상태:")
    print(f"  - Suspended (대기 중): {suspended_count}")
    print(f"  - Running (실행 중): {running_count}")
    
    if suspended_count == 0:
        print("⚠️  경고: Suspended 상태의 작업이 없습니다!")
        print("   RL 스케줄러가 관리할 작업이 없습니다.")
else:
    print("❌ kubectl 명령 실행 실패")

# 2. Python 환경에서 확인
print("\n🐍 Python API 테스트:")
try:
    from skrueue import KueueInterface
    kueue = KueueInterface(['skrueue-test'])
    pending_jobs = kueue.get_pending_jobs()
    print(f"✅ KueueInterface가 찾은 대기 작업: {len(pending_jobs)}")
    
    if len(pending_jobs) > 0:
        print("  첫 번째 작업:")
        job = pending_jobs[0]
        print(f"    - 이름: {job.name}")
        print(f"    - CPU: {job.cpu_request}")
        print(f"    - 메모리: {job.memory_request}GB")
except Exception as e:
    print(f"❌ Python API 오류: {e}")