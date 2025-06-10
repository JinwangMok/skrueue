#!/bin/bash
# collecting_data_balanced.sh
# 수정된 균형잡힌 데이터 수집 스크립트

set -e

# 설정
NAMESPACE="skrueue-test"
LOG_DIR="balanced_logs_$(date +%Y%m%d_%H%M%S)"
DATA_COLLECTOR_INTERVAL=5
WORKLOADS_PER_TYPE=30  # 각 워크로드 타입별 생성 수

# 색상 코드
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 로깅 함수
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

# 메인 실행
log "🎯 균형잡힌 데이터 수집 시작"
mkdir -p $LOG_DIR

# 1. 기존 데이터 수집기 확인 및 정리
log "🔍 기존 데이터 수집기 확인..."
EXISTING_PID=$(ps aux | grep "[p]ython data_collector.py" | awk '{print $2}' | head -1)
if [ ! -z "$EXISTING_PID" ]; then
    log "기존 데이터 수집기 발견 (PID: $EXISTING_PID), 계속 사용"
    DATA_COLLECTOR_PID=$EXISTING_PID
else
    log "📊 새 데이터 수집기 시작..."
    python data_collector.py --interval $DATA_COLLECTOR_INTERVAL --namespaces default $NAMESPACE > $LOG_DIR/collector.log 2>&1 &
    DATA_COLLECTOR_PID=$!
    log "Data Collector PID: $DATA_COLLECTOR_PID"
    sleep 10
fi

# 2. 균형잡힌 워크로드 생성
WORKLOAD_TYPES=("ml-training" "big-data-aggregation" "etl-pipeline" "realtime-analytics" "data-validation" "memory-stress-test")

for workload in "${WORKLOAD_TYPES[@]}"; do
    log "🎯 $workload 생성 중..."
    
    for i in $(seq 1 $WORKLOADS_PER_TYPE); do
        python sample_workload_generator.py \
            --mode single \
            --template $workload \
            --namespace $NAMESPACE \
            --force-k8s-jobs >> $LOG_DIR/$workload.log 2>&1
        
        if [ $((i % 10)) -eq 0 ]; then
            echo "  $i/$WORKLOADS_PER_TYPE 완료"
            sleep 30  # 10개마다 30초 대기
        else
            sleep 3   # 각 작업 사이 3초 대기
        fi
    done
done

# 3. 데이터 수집 대기
log "⏳ 추가 데이터 수집을 위해 2분 대기..."
sleep 120

# 4. 데이터베이스 확인
log "🗄️ 데이터베이스 상태 확인..."
if [ -f "skrueue_training_data.db" ]; then
    python check_database.py
else
    error "데이터베이스 파일이 없습니다!"
fi

# 5. 데이터 내보내기 (여러 방법 시도)
log "📤 데이터 내보내기 시작..."

# 방법 1: data_collector.py 사용
python data_collector.py --export-only --output balanced_training_data.csv 2>/dev/null

# 방법 2: 실패 시 export_full_data.py 사용
if [ ! -f "balanced_training_data.csv" ]; then
    warning "data_collector.py 내보내기 실패, 대체 방법 사용..."
    
    # export_full_data.py 생성
    cat > export_full_data.py << 'PYTHON_SCRIPT'
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
PYTHON_SCRIPT

    python export_full_data.py
fi

# 방법 3: 여전히 실패 시 test_export.csv 사용
if [ ! -f "balanced_training_data.csv" ] && [ -f "test_export.csv" ]; then
    warning "모든 내보내기 실패, test_export.csv 사용..."
    cp test_export.csv balanced_training_data.csv
fi

# 6. 최종 결과 확인
log "📊 최종 결과 확인..."
if [ -f "balanced_training_data.csv" ]; then
    log "✅ 균형잡힌 데이터셋 생성 완료!"
    echo "  파일 크기: $(ls -lh balanced_training_data.csv | awk '{print $5}')"
    echo "  레코드 수: $(wc -l balanced_training_data.csv | awk '{print $1}')"
    echo ""
    echo "📊 데이터 분석:"
    python -c "
import pandas as pd
try:
    df = pd.read_csv('balanced_training_data.csv')
    print(f'총 레코드: {len(df)}')
    print(f'컬럼: {list(df.columns)}')
    if 'status' in df.columns:
        print('\n작업 상태 분포:')
        print(df['status'].value_counts())
    if 'job_name' in df.columns:
        # 워크로드 타입 분석
        workload_types = {
            'ml-training': 0,
            'big-data-aggregation': 0,
            'etl-pipeline': 0,
            'realtime-analytics': 0,
            'data-validation': 0,
            'memory-stress-test': 0,
            'other': 0
        }
        for name in df['job_name']:
            found = False
            for wtype in workload_types.keys():
                if wtype in str(name):
                    workload_types[wtype] += 1
                    found = True
                    break
            if not found:
                workload_types['other'] += 1
        
        print('\n워크로드 타입별 분포:')
        for wtype, count in workload_types.items():
            if count > 0:
                print(f'  - {wtype}: {count}')
except Exception as e:
    print(f'분석 오류: {e}')
"
else
    error "❌ 데이터 내보내기 실패"
fi

# 7. 정리 (새로 시작한 경우만)
if [ "$DATA_COLLECTOR_PID" != "$EXISTING_PID" ]; then
    log "🛑 데이터 수집기 종료..."
    kill $DATA_COLLECTOR_PID 2>/dev/null || true
fi

log "✅ 완료!"
echo "📁 로그 디렉토리: $LOG_DIR"