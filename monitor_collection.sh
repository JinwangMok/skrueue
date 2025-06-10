#!/bin/bash
while true; do
    echo "=== 데이터 수집 현황 $(date) ==="
    
    # 작업 수 확인
    job_count=$(kubectl get jobs -n skrueue-test --no-headers 2>/dev/null | wc -l)
    running_pods=$(kubectl get pods -n skrueue-test --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    
    echo "생성된 작업 수: $job_count"
    echo "실행 중인 Pod 수: $running_pods"
    
    # 데이터베이스 상태
    if [ -f "skrueue_training_data.db" ]; then
        python -c "
import sqlite3
conn = sqlite3.connect('skrueue_training_data.db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM jobs')
job_count = cursor.fetchone()[0]
cursor.execute('SELECT COUNT(*) FROM cluster_states')
cluster_count = cursor.fetchone()[0]
print(f'수집된 작업: {job_count}, 클러스터 상태: {cluster_count}')
conn.close()
"
    else
        echo "데이터베이스 파일 없음"
    fi
    
    echo "---"
    sleep 3600  # 1시간마다 확인
done
