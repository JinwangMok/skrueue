import sqlite3
import pandas as pd
from datetime import datetime

try:
    conn = sqlite3.connect('skrueue_training_data.db')
    
    # 간단한 쿼리로 데이터 추출
    query = """
    SELECT 
        j.name as job_name,
        j.cpu_request as job_cpu_request,
        j.memory_request as job_memory_request,
        j.priority as job_priority,
        j.status,
        j.submission_time,
        j.job_id
    FROM jobs j
    WHERE j.submission_time IS NOT NULL
    LIMIT 10000
    """
    
    df = pd.read_sql_query(query, conn)
    
    # 클러스터 상태 추가 (최신 값 사용)
    cluster_query = """
    SELECT 
        available_cpu as cluster_cpu_available,
        available_memory as cluster_memory_available,
        queue_length as cluster_queue_length,
        running_jobs_count as cluster_running_jobs
    FROM cluster_states
    ORDER BY timestamp DESC
    LIMIT 1
    """
    
    cluster_state = pd.read_sql_query(cluster_query, conn).iloc[0]
    
    # 클러스터 상태를 모든 레코드에 추가
    for col, val in cluster_state.items():
        df[col] = val
    
    # 추가 컬럼
    df['execution_success'] = df['status'].apply(lambda x: 1 if x == 'Succeeded' else 0)
    df['oom_occurred'] = 0  # 기본값
    
    # CSV 저장
    df.to_csv('balanced_training_data.csv', index=False)
    print(f"✅ {len(df)} 레코드를 balanced_training_data.csv로 저장")
    
    conn.close()
    
except Exception as e:
    print(f"❌ 오류: {e}")
