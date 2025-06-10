#!/bin/bash
# collecting_data_large.sh
# SKRueue 24시간 대용량 데이터 수집 시스템
# 현실적인 워크로드 패턴으로 다양한 작업 생성 및 장기간 데이터 수집

# 본 스크립트 파일 소개
# 1. 현실적인 24시간 워크로드 패턴
## 시간대별 다른 패턴
# - 새벽 (0-6시): 배치 작업 중심 (적은 작업량)
# - 업무 시간 (9-18시): 다양한 분석 작업 (많은 작업량)  
# - 저녁 (19-23시): 보고서 생성 (중간 작업량)
# 2. 다중 워크로드 생성
# # 동시 실행되는 워크로드 생성기들
# - 메인 시뮬레이션 (24시간 지속)
# - 시간당 추가 배치 작업 (10-19개)
# - 6가지 다양한 작업 타입
# 3. 완전한 모니터링 시스템
## 실시간 모니터링
# - 5분마다 상태 확인
# - 1시간마다 상세 리포트
# - 자동 백업 (1시간마다)
# - 프로세스 장애 복구
# 4. 안전한 실행 관리
## 장애 대응
# - 프로세스 자동 재시작
# - 신호 처리 (Ctrl+C 안전 종료)
# - 자동 백업 및 복구
# - 상세한 로깅
# 📊 예상 데이터 생성량
## 24시간 실행 시 예상:
# ├── 작업 수: 1,000-3,000개 (시간대별 변동)
# ├── 클러스터 상태: 8,640개 (10초마다)
# ├── 데이터베이스: 50-200MB
# └── CSV 훈련 데이터: 10,000-50,000 레코드


set -e  # 오류 발생 시 스크립트 중단

# 설정 변수
DURATION_HOURS=24
DATA_COLLECTION_INTERVAL=10
NAMESPACE="skrueue-test"
LOG_DIR="logs_$(date +%Y%m%d_%H%M%S)"
DATA_OUTPUT_DIR="data_$(date +%Y%m%d_%H%M%S)"
BACKUP_INTERVAL=3600  # 1시간마다 백업

# 색상 코드
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 로깅 함수
log() {
    echo -e "${CYAN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "${LOG_DIR}/main.log"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "${LOG_DIR}/main.log"
}

success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS:${NC} $1" | tee -a "${LOG_DIR}/main.log"
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "${LOG_DIR}/main.log"
}

# 디렉토리 및 환경 설정
setup_environment() {
    log "🔧 24시간 대용량 데이터 수집 환경 설정 시작"
    
    # 로그 및 데이터 디렉토리 생성
    mkdir -p "${LOG_DIR}"
    mkdir -p "${DATA_OUTPUT_DIR}"
    mkdir -p "backups"
    
    # 네임스페이스 확인/생성
    if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
        log "📋 네임스페이스 생성 중: ${NAMESPACE}"
        kubectl create namespace "${NAMESPACE}"
    else
        log "✅ 네임스페이스 확인: ${NAMESPACE}"
    fi
    
    # ServiceAccount 및 권한 설정
    log "🔐 ServiceAccount 및 권한 설정 중..."
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: skrueue-agent
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: skrueue-agent-role
rules:
- apiGroups: [""]
  resources: ["nodes", "pods", "events", "services", "configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["apps"]
  resources: ["deployments", "replicasets"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["kueue.x-k8s.io"]
  resources: ["workloads", "localqueues", "clusterqueues"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: skrueue-agent-binding
subjects:
- kind: ServiceAccount
  name: skrueue-agent
  namespace: ${NAMESPACE}
roleRef:
  kind: ClusterRole
  name: skrueue-agent-role
  apiGroup: rbac.authorization.k8s.io
EOF
    
    success "✅ 환경 설정 완료"
}

# 기존 프로세스 정리
cleanup_processes() {
    log "🧹 기존 프로세스 정리 중..."
    
    # 이전 실행 중인 프로세스들 종료
    pkill -f "data_collector.py" 2>/dev/null || true
    pkill -f "sample_workload_generator.py" 2>/dev/null || true
    
    # 기존 테스트 작업들 정리 (선택적)
    read -p "기존 테스트 작업들을 정리하시겠습니까? (y/N): " cleanup_jobs
    if [[ $cleanup_jobs =~ ^[Yy]$ ]]; then
        log "🗑️ 기존 작업 정리 중..."
        kubectl delete jobs --all -n "${NAMESPACE}" --wait=false 2>/dev/null || true
        kubectl delete pods --all -n "${NAMESPACE}" --wait=false 2>/dev/null || true
        sleep 10
    fi
    
    success "✅ 프로세스 정리 완료"
}

# 클러스터 상태 확인
check_cluster_status() {
    log "🔍 클러스터 상태 확인 중..."
    
    # 노드 상태 확인
    echo "📊 클러스터 노드 상태:" | tee -a "${LOG_DIR}/cluster_status.log"
    kubectl get nodes -o wide | tee -a "${LOG_DIR}/cluster_status.log"
    
    # 리소스 사용량 확인
    echo "💾 리소스 사용량:" | tee -a "${LOG_DIR}/cluster_status.log"
    kubectl top nodes 2>/dev/null | tee -a "${LOG_DIR}/cluster_status.log" || log "⚠️ kubectl top 사용 불가 (metrics-server 필요)"
    
    # 네임스페이스 현재 상태
    echo "📋 네임스페이스 ${NAMESPACE} 상태:" | tee -a "${LOG_DIR}/cluster_status.log"
    kubectl get all -n "${NAMESPACE}" | tee -a "${LOG_DIR}/cluster_status.log"
    
    success "✅ 클러스터 상태 확인 완료"
}

# 데이터 수집기 시작
start_data_collector() {
    log "📊 데이터 수집기 시작 중..."
    
    # 데이터 수집기 백그라운드 실행
    nohup python data_collector.py \
        --interval ${DATA_COLLECTION_INTERVAL} \
        --namespaces default ${NAMESPACE} monitoring kube-system \
        --retention-days 7 \
        --db-path "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" \
        > "${LOG_DIR}/data_collector.log" 2>&1 &
    
    DATA_COLLECTOR_PID=$!
    echo $DATA_COLLECTOR_PID > "${LOG_DIR}/data_collector.pid"
    
    log "📊 Data Collector PID: $DATA_COLLECTOR_PID"
    
    # 데이터 수집기 시작 확인
    sleep 10
    if ps -p $DATA_COLLECTOR_PID > /dev/null; then
        success "✅ 데이터 수집기 정상 시작"
    else
        error "❌ 데이터 수집기 시작 실패"
        cat "${LOG_DIR}/data_collector.log"
        exit 1
    fi
}

# 워크로드 생성기 시작 (24시간 시뮬레이션)
start_workload_generator() {
    log "🚀 24시간 워크로드 생성기 시작 중..."
    
    # 워크로드 생성기 백그라운드 실행 (simulation 모드)
    nohup python sample_workload_generator.py \
        --mode simulation \
        --duration ${DURATION_HOURS} \
        --namespace ${NAMESPACE} \
        --force-k8s-jobs \
        > "${LOG_DIR}/workload_generator.log" 2>&1 &
    
    WORKLOAD_GENERATOR_PID=$!
    echo $WORKLOAD_GENERATOR_PID > "${LOG_DIR}/workload_generator.pid"
    
    log "🚀 Workload Generator PID: $WORKLOAD_GENERATOR_PID"
    
    # 워크로드 생성기 시작 확인
    sleep 15
    if ps -p $WORKLOAD_GENERATOR_PID > /dev/null; then
        success "✅ 워크로드 생성기 정상 시작"
        
        # 첫 작업 생성 확인
        log "⏳ 첫 작업 생성 대기 중... (최대 5분)"
        for i in {1..30}; do
            job_count=$(kubectl get jobs -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l)
            if [ "$job_count" -gt 0 ]; then
                success "✅ 첫 작업 생성 확인: ${job_count}개"
                break
            fi
            sleep 10
        done
    else
        error "❌ 워크로드 생성기 시작 실패"
        cat "${LOG_DIR}/workload_generator.log"
        exit 1
    fi
}

# 추가적인 다양한 워크로드 패턴 생성
generate_additional_workloads() {
    log "🎯 추가 다양한 워크로드 패턴 생성 중..."
    
    # 1시간마다 추가 워크로드 생성 (백그라운드)
    (
        for hour in $(seq 1 ${DURATION_HOURS}); do
            sleep 3600  # 1시간 대기
            
            log "⏰ ${hour}시간 경과 - 추가 워크로드 생성"
            
            # 시간대에 따른 다른 패턴 적용
            current_hour=$(date +%H)
            
            if [ $current_hour -ge 9 ] && [ $current_hour -le 18 ]; then
                # 업무 시간: 더 많은 작업
                batch_size=$((RANDOM % 10 + 10))  # 10-19개
                interval=30
            elif [ $current_hour -ge 19 ] && [ $current_hour -le 23 ]; then
                # 저녁 시간: 중간 정도
                batch_size=$((RANDOM % 8 + 5))   # 5-12개
                interval=45
            else
                # 새벽/밤: 적은 작업
                batch_size=$((RANDOM % 5 + 3))   # 3-7개
                interval=60
            fi
            
            log "📊 시간대별 배치 작업 생성: ${batch_size}개 (간격: ${interval}초)"
            
            # 배치 작업 생성
            timeout 1800 python sample_workload_generator.py \
                --mode batch \
                --count ${batch_size} \
                --interval ${interval} \
                --namespace ${NAMESPACE} \
                --force-k8s-jobs \
                >> "${LOG_DIR}/additional_workloads.log" 2>&1 &
                
        done
    ) &
    
    ADDITIONAL_WORKLOAD_PID=$!
    echo $ADDITIONAL_WORKLOAD_PID > "${LOG_DIR}/additional_workload.pid"
    
    log "🎯 Additional Workload Generator PID: $ADDITIONAL_WORKLOAD_PID"
}

# 모니터링 시스템 시작
start_monitoring() {
    log "📈 실시간 모니터링 시스템 시작 중..."
    
    # 모니터링 스크립트 생성
    cat > "${LOG_DIR}/monitor.sh" << 'EOF'
#!/bin/bash
LOG_DIR="$1"
NAMESPACE="$2"
DATA_OUTPUT_DIR="$3"

while true; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # 작업 상태 수집
    job_stats=$(kubectl get jobs -n "$NAMESPACE" --no-headers 2>/dev/null | \
        awk 'BEGIN{pending=0; running=0; succeeded=0; failed=0} 
             {if($2=="0/1") pending++; 
              else if($3=="0") running++; 
              else if($2=="1/1") succeeded++; 
              else failed++} 
             END{print pending","running","succeeded","failed}')
    
    # 팟 상태 수집
    pod_stats=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | \
        awk 'BEGIN{running=0; pending=0; succeeded=0; failed=0; unknown=0}
             {if($3=="Running") running++;
              else if($3=="Pending") pending++;
              else if($3=="Succeeded") succeeded++;
              else if($3=="Failed" || $3=="Error") failed++;
              else unknown++}
             END{print running","pending","succeeded","failed","unknown}')
    
    # 데이터베이스 크기
    if [ -f "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" ]; then
        db_size=$(ls -lh "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" | awk '{print $5}')
    else
        db_size="N/A"
    fi
    
    # 로그 출력
    echo "${timestamp} | Jobs: ${job_stats} | Pods: ${pod_stats} | DB: ${db_size}" >> "${LOG_DIR}/monitoring.log"
    
    sleep 300  # 5분마다 수집
done
EOF
    
    chmod +x "${LOG_DIR}/monitor.sh"
    
    # 모니터링 백그라운드 실행
    nohup "${LOG_DIR}/monitor.sh" "${LOG_DIR}" "${NAMESPACE}" "${DATA_OUTPUT_DIR}" > /dev/null 2>&1 &
    MONITOR_PID=$!
    echo $MONITOR_PID > "${LOG_DIR}/monitor.pid"
    
    log "📈 Monitor PID: $MONITOR_PID"
    success "✅ 모니터링 시스템 시작 완료"
}

# 백업 시스템
setup_backup_system() {
    log "💾 자동 백업 시스템 설정 중..."
    
    # 백업 스크립트 생성
    cat > "${LOG_DIR}/backup.sh" << EOF
#!/bin/bash
BACKUP_DIR="backups"
DATA_OUTPUT_DIR="$1"
LOG_DIR="$2"

while true; do
    sleep ${BACKUP_INTERVAL}  # 1시간마다 백업
    
    timestamp=\$(date '+%Y%m%d_%H%M%S')
    backup_name="skrueue_backup_\${timestamp}"
    
    echo "\$(date): 백업 시작 - \${backup_name}" >> "\${LOG_DIR}/backup.log"
    
    # 데이터베이스 백업
    if [ -f "\${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" ]; then
        cp "\${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" "\${BACKUP_DIR}/\${backup_name}.db"
        echo "\$(date): DB 백업 완료 - \${backup_name}.db" >> "\${LOG_DIR}/backup.log"
    fi
    
    # 로그 백업
    tar -czf "\${BACKUP_DIR}/\${backup_name}_logs.tar.gz" "\${LOG_DIR}/" 2>/dev/null
    echo "\$(date): 로그 백업 완료 - \${backup_name}_logs.tar.gz" >> "\${LOG_DIR}/backup.log"
    
    # 오래된 백업 정리 (72시간 이상)
    find "\${BACKUP_DIR}" -name "skrueue_backup_*" -mtime +3 -delete 2>/dev/null
    
done
EOF
    
    chmod +x "${LOG_DIR}/backup.sh"
    
    # 백업 시스템 백그라운드 실행
    nohup "${LOG_DIR}/backup.sh" "${DATA_OUTPUT_DIR}" "${LOG_DIR}" > /dev/null 2>&1 &
    BACKUP_PID=$!
    echo $BACKUP_PID > "${LOG_DIR}/backup.pid"
    
    log "💾 Backup System PID: $BACKUP_PID"
    success "✅ 자동 백업 시스템 설정 완료"
}

# 상태 리포트 생성
generate_status_report() {
    local hour=$1
    log "📊 ${hour}시간 경과 상태 리포트 생성 중..."
    
    report_file="${LOG_DIR}/status_report_${hour}h.txt"
    
    cat > "$report_file" << EOF
================================
SKRueue 데이터 수집 상태 리포트
시간: $(date)
경과: ${hour}시간 / ${DURATION_HOURS}시간
================================

=== 클러스터 상태 ===
$(kubectl get nodes --no-headers | wc -l) 노드 활성

=== 작업 상태 (네임스페이스: ${NAMESPACE}) ===
$(kubectl get jobs -n "${NAMESPACE}" --no-headers 2>/dev/null | awk 'BEGIN{pending=0; running=0; succeeded=0; failed=0} {if($2=="0/1") pending++; else if($3=="0") running++; else if($2=="1/1") succeeded++; else failed++} END{printf "대기: %d, 실행중: %d, 성공: %d, 실패: %d\n", pending, running, succeeded, failed}')

총 작업 수: $(kubectl get jobs -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l)

=== 팟 상태 ===
$(kubectl get pods -n "${NAMESPACE}" --no-headers 2>/dev/null | awk 'BEGIN{running=0; pending=0; succeeded=0; failed=0} {if($3=="Running") running++; else if($3=="Pending") pending++; else if($3=="Succeeded") succeeded++; else if($3=="Failed") failed++} END{printf "실행중: %d, 대기: %d, 성공: %d, 실패: %d\n", running, pending, succeeded, failed}')

=== 데이터베이스 상태 ===
EOF
    
    # 데이터베이스 상태 확인
    if [ -f "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" ]; then
        echo "파일 크기: $(ls -lh "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" | awk '{print $5}')" >> "$report_file"
        
        # 데이터베이스 레코드 수 확인
        python3 -c "
import sqlite3
try:
    conn = sqlite3.connect('${DATA_OUTPUT_DIR}/skrueue_large_dataset.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM jobs')
    job_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM cluster_states')
    cluster_count = cursor.fetchone()[0]
    print(f'작업 레코드: {job_count:,}개')
    print(f'클러스터 상태 레코드: {cluster_count:,}개')
    conn.close()
except Exception as e:
    print(f'DB 조회 오류: {e}')
" >> "$report_file" 2>/dev/null || echo "DB 레코드 수 조회 실패" >> "$report_file"
    else
        echo "데이터베이스 파일 없음" >> "$report_file"
    fi
    
    echo "" >> "$report_file"
    echo "=== 프로세스 상태 ===" >> "$report_file"
    
    # 프로세스 상태 확인
    for pid_file in "${LOG_DIR}"/*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            process_name=$(basename "$pid_file" .pid)
            if ps -p "$pid" > /dev/null; then
                echo "${process_name}: 실행중 (PID: $pid)" >> "$report_file"
            else
                echo "${process_name}: 중단됨 (PID: $pid)" >> "$report_file"
            fi
        fi
    done
    
    log "📊 상태 리포트 생성 완료: $report_file"
    
    # 중요 정보 로그에도 출력
    cat "$report_file" | head -20 | tee -a "${LOG_DIR}/main.log"
}

# 정리 함수
cleanup_on_exit() {
    log "🛑 종료 신호 감지 - 정리 작업 시작"
    
    # 모든 백그라운드 프로세스 종료
    for pid_file in "${LOG_DIR}"/*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            process_name=$(basename "$pid_file" .pid)
            if ps -p "$pid" > /dev/null; then
                log "🛑 ${process_name} 프로세스 종료 중 (PID: $pid)"
                kill "$pid" 2>/dev/null || true
            fi
        fi
    done
    
    # 최종 데이터 내보내기
    log "📊 최종 데이터 내보내기 중..."
    if [ -f "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" ]; then
        python data_collector.py \
            --export-only \
            --db-path "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" \
            --output "${DATA_OUTPUT_DIR}/final_large_training_data.csv" \
            >> "${LOG_DIR}/final_export.log" 2>&1
        
        if [ -f "${DATA_OUTPUT_DIR}/final_large_training_data.csv" ]; then
            success "✅ 최종 CSV 생성 완료: ${DATA_OUTPUT_DIR}/final_large_training_data.csv"
        fi
    fi
    
    # 최종 상태 리포트
    generate_status_report "FINAL"
    
    success "✅ 정리 작업 완료"
    exit 0
}

# 메인 실행 함수
main() {
    echo ""
    echo "🚀 SKRueue 24시간 대용량 데이터 수집 시작"
    echo "============================================"
    echo "📅 시작 시간: $(date)"
    echo "⏰ 지속 시간: ${DURATION_HOURS}시간"
    echo "📊 수집 간격: ${DATA_COLLECTION_INTERVAL}초"
    echo "📂 로그 디렉토리: ${LOG_DIR}"
    echo "💾 데이터 디렉토리: ${DATA_OUTPUT_DIR}"
    echo "🔄 백업 간격: ${BACKUP_INTERVAL}초"
    echo "============================================"
    echo ""
    
    # 신호 처리기 설정
    trap cleanup_on_exit SIGINT SIGTERM
    
    # 1. 환경 설정
    setup_environment
    
    # 2. 기존 프로세스 정리
    cleanup_processes
    
    # 3. 클러스터 상태 확인
    check_cluster_status
    
    # 4. 데이터 수집기 시작
    start_data_collector
    
    # 5. 워크로드 생성기 시작
    start_workload_generator
    
    # 6. 추가 워크로드 패턴 생성
    generate_additional_workloads
    
    # 7. 모니터링 시스템 시작
    start_monitoring
    
    # 8. 백업 시스템 설정
    setup_backup_system
    
    # 9. 메인 루프 (시간별 상태 확인)
    log "🔄 메인 루프 시작 - ${DURATION_HOURS}시간 실행"
    
    for hour in $(seq 1 ${DURATION_HOURS}); do
        log "⏰ ${hour}/${DURATION_HOURS}시간 대기 시작"
        
        # 1시간 대기 (12번의 5분 체크)
        for minute_check in {1..12}; do
            sleep 300  # 5분
            
            # 프로세스 상태 확인
            if [ -f "${LOG_DIR}/data_collector.pid" ]; then
                pid=$(cat "${LOG_DIR}/data_collector.pid")
                if ! ps -p "$pid" > /dev/null; then
                    error "❌ 데이터 수집기 중단 감지 - 재시작 시도"
                    start_data_collector
                fi
            fi
            
            if [ -f "${LOG_DIR}/workload_generator.pid" ]; then
                pid=$(cat "${LOG_DIR}/workload_generator.pid")
                if ! ps -p "$pid" > /dev/null; then
                    warning "⚠️ 워크로드 생성기 중단 감지"
                fi
            fi
            
            # 5분마다 간단한 상태 출력
            job_count=$(kubectl get jobs -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l)
            running_pods=$(kubectl get pods -n "${NAMESPACE}" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
            log "📊 ${hour}시간 ${minute_check}번째 체크 - 작업: ${job_count}개, 실행중 팟: ${running_pods}개"
        done
        
        # 시간별 상태 리포트 생성
        generate_status_report "$hour"
        
        log "✅ ${hour}시간 완료"
    done
    
    success "🎉 24시간 데이터 수집 완료!"
    
    # 최종 데이터 처리
    log "📊 최종 데이터 내보내기 및 정리 중..."
    
    # 데이터 수집기 종료
    if [ -f "${LOG_DIR}/data_collector.pid" ]; then
        pid=$(cat "${LOG_DIR}/data_collector.pid")
        if ps -p "$pid" > /dev/null; then
            kill "$pid"
            sleep 10
        fi
    fi
    
    # 최종 CSV 내보내기
    if [ -f "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" ]; then
        log "📊 최종 대용량 훈련 데이터 생성 중..."
        python data_collector.py \
            --export-only \
            --db-path "${DATA_OUTPUT_DIR}/skrueue_large_dataset.db" \
            --output "${DATA_OUTPUT_DIR}/large_training_dataset.csv"
        
        if [ -f "${DATA_OUTPUT_DIR}/large_training_dataset.csv" ]; then
            success "✅ 대용량 훈련 데이터셋 생성 완료!"
            log "📁 파일 위치: ${DATA_OUTPUT_DIR}/large_training_dataset.csv"
            log "📊 파일 크기: $(ls -lh "${DATA_OUTPUT_DIR}/large_training_dataset.csv" | awk '{print $5}')"
            log "📝 레코드 수: $(wc -l "${DATA_OUTPUT_DIR}/large_training_dataset.csv" | awk '{print $1}')"
        else
            error "❌ 최종 CSV 생성 실패"
        fi
    else
        error "❌ 데이터베이스 파일이 존재하지 않음"
    fi
    
    # 모든 프로세스 정리
    cleanup_on_exit
}

# 사용법 출력
usage() {
    echo "사용법: $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  -h, --help           이 도움말 출력"
    echo "  -d, --duration HOURS 실행 시간 (기본값: 24시간)"
    echo "  -i, --interval SEC   데이터 수집 간격 (기본값: 10초)"
    echo "  -n, --namespace NS   대상 네임스페이스 (기본값: skrueue-test)"
    echo ""
    echo "예시:"
    echo "  $0                           # 기본 설정으로 24시간 실행"
    echo "  $0 -d 12                     # 12시간만 실행"
    echo "  $0 -d 48 -i 5               # 48시간, 5초 간격으로 실행"
    echo ""
}

# 명령행 인수 처리
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -d|--duration)
            DURATION_HOURS="$2"
            shift 2
            ;;
        -i|--interval)
            DATA_COLLECTION_INTERVAL="$2"
            shift 2
            ;;
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        *)
            error "알 수 없는 옵션: $1"
            usage
            exit 1
            ;;
    esac
done

# 인수 검증
if ! [[ "$DURATION_HOURS" =~ ^[0-9]+$ ]] || [ "$DURATION_HOURS" -lt 1 ]; then
    error "유효하지 않은 지속 시간: $DURATION_HOURS"
    exit 1
fi

if ! [[ "$DATA_COLLECTION_INTERVAL" =~ ^[0-9]+$ ]] || [ "$DATA_COLLECTION_INTERVAL" -lt 1 ]; then
    error "유효하지 않은 수집 간격: $DATA_COLLECTION_INTERVAL"
    exit 1
fi

# 메인 실행
main