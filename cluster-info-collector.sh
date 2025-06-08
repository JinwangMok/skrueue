#!/bin/bash

# SKRueue 프로젝트를 위한 클러스터 환경 정보 수집 스크립트
# 작성자: SKRueue Research Team
# 목적: Kubernetes 클러스터의 현재 상태와 Kueue 설정 정보 수집

set -e

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
OUTPUT_DIR="skrueue_cluster_info_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

echo "=== SKRueue 클러스터 환경 정보 수집 시작 ==="
echo "출력 디렉토리: ${OUTPUT_DIR}"

# 1. 기본 클러스터 정보
echo "1. 기본 클러스터 정보 수집 중..."
kubectl cluster-info > "${OUTPUT_DIR}/cluster_info.txt" 2>&1 || echo "cluster-info 실패" >> "${OUTPUT_DIR}/errors.log"
kubectl version --output=yaml > "${OUTPUT_DIR}/kubectl_version.yaml" 2>&1 || echo "kubectl version 실패" >> "${OUTPUT_DIR}/errors.log"

# 2. 노드 정보 (리소스 용량 및 할당량)
echo "2. 노드 정보 수집 중..."
kubectl get nodes -o wide > "${OUTPUT_DIR}/nodes_wide.txt" 2>&1
kubectl describe nodes > "${OUTPUT_DIR}/nodes_describe.txt" 2>&1
kubectl top nodes > "${OUTPUT_DIR}/nodes_top.txt" 2>&1 || echo "top nodes 실패 (metrics-server 필요)" >> "${OUTPUT_DIR}/errors.log"

# 노드별 상세 리소스 정보 (JSON 형태)
kubectl get nodes -o json > "${OUTPUT_DIR}/nodes_full.json" 2>&1

# 3. 네임스페이스 정보
echo "3. 네임스페이스 정보 수집 중..."
kubectl get namespaces -o wide > "${OUTPUT_DIR}/namespaces.txt" 2>&1

# 4. Kueue 관련 정보 확인
echo "4. Kueue 설치 및 설정 확인 중..."
# Kueue CRD 확인
kubectl get crd | grep kueue > "${OUTPUT_DIR}/kueue_crds.txt" 2>&1 || echo "Kueue CRD 없음" >> "${OUTPUT_DIR}/kueue_status.txt"

# Kueue 컨트롤러 확인
kubectl get pods -A | grep kueue > "${OUTPUT_DIR}/kueue_pods.txt" 2>&1 || echo "Kueue 팟 없음" >> "${OUTPUT_DIR}/kueue_status.txt"

# ClusterQueue 및 LocalQueue 확인
kubectl get clusterqueue -A -o yaml > "${OUTPUT_DIR}/clusterqueues.yaml" 2>&1 || echo "ClusterQueue 없음" >> "${OUTPUT_DIR}/kueue_status.txt"
kubectl get localqueue -A -o yaml > "${OUTPUT_DIR}/localqueues.yaml" 2>&1 || echo "LocalQueue 없음" >> "${OUTPUT_DIR}/kueue_status.txt"

# 5. Spark Operator 확인
echo "5. Spark Operator 확인 중..."
kubectl get crd | grep spark > "${OUTPUT_DIR}/spark_crds.txt" 2>&1 || echo "Spark CRD 없음" >> "${OUTPUT_DIR}/spark_status.txt"
kubectl get pods -A | grep spark > "${OUTPUT_DIR}/spark_pods.txt" 2>&1 || echo "Spark 팟 없음" >> "${OUTPUT_DIR}/spark_status.txt"

# 6. 스토리지 클래스 및 PV 정보
echo "6. 스토리지 정보 수집 중..."
kubectl get storageclass > "${OUTPUT_DIR}/storageclasses.txt" 2>&1
kubectl get pv > "${OUTPUT_DIR}/persistent_volumes.txt" 2>&1
kubectl get pvc -A > "${OUTPUT_DIR}/persistent_volume_claims.txt" 2>&1

# 7. 네트워킹 정보
echo "7. 네트워킹 정보 수집 중..."
kubectl get services -A > "${OUTPUT_DIR}/services.txt" 2>&1
kubectl get ingress -A > "${OUTPUT_DIR}/ingress.txt" 2>&1 || echo "인그레스 없음" >> "${OUTPUT_DIR}/networking_status.txt"

# 8. RBAC 정보 (Kueue 작업에 필요)
echo "8. RBAC 정보 수집 중..."
kubectl get clusterroles | grep -E "(kueue|spark)" > "${OUTPUT_DIR}/kueue_spark_clusterroles.txt" 2>&1 || echo "관련 ClusterRole 없음" >> "${OUTPUT_DIR}/rbac_status.txt"
kubectl get clusterrolebindings | grep -E "(kueue|spark)" > "${OUTPUT_DIR}/kueue_spark_bindings.txt" 2>&1 || echo "관련 ClusterRoleBinding 없음" >> "${OUTPUT_DIR}/rbac_status.txt"

# 9. 리소스 할당량 및 제한
echo "9. 리소스 정책 수집 중..."
kubectl get resourcequota -A > "${OUTPUT_DIR}/resource_quotas.txt" 2>&1 || echo "ResourceQuota 없음" >> "${OUTPUT_DIR}/resource_status.txt"
kubectl get limitrange -A > "${OUTPUT_DIR}/limit_ranges.txt" 2>&1 || echo "LimitRange 없음" >> "${OUTPUT_DIR}/resource_status.txt"

# 10. 현재 실행 중인 작업들
echo "10. 현재 워크로드 수집 중..."
kubectl get jobs -A > "${OUTPUT_DIR}/current_jobs.txt" 2>&1
kubectl get pods -A --field-selector=status.phase=Running > "${OUTPUT_DIR}/running_pods.txt" 2>&1
kubectl get workloads -A > "${OUTPUT_DIR}/kueue_workloads.txt" 2>&1 || echo "Kueue Workload CRD 없음" >> "${OUTPUT_DIR}/workload_status.txt"

# 11. 메트릭 서버 및 모니터링 확인
echo "11. 모니터링 시스템 확인 중..."
kubectl get pods -A | grep -E "(metrics|prometheus|grafana)" > "${OUTPUT_DIR}/monitoring_pods.txt" 2>&1 || echo "모니터링 시스템 없음" >> "${OUTPUT_DIR}/monitoring_status.txt"

# 12. 컨테이너 런타임 정보
echo "12. 컨테이너 런타임 정보 수집 중..."
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.nodeInfo.containerRuntimeVersion}{"\n"}{end}' > "${OUTPUT_DIR}/container_runtime.txt" 2>&1

# 13. 클러스터 이벤트 (최근 문제 확인용)
echo "13. 최근 클러스터 이벤트 수집 중..."
kubectl get events -A --sort-by='.metadata.creationTimestamp' | tail -100 > "${OUTPUT_DIR}/recent_events.txt" 2>&1

# 14. 사용 가능한 API 리소스
echo "14. API 리소스 정보 수집 중..."
kubectl api-resources > "${OUTPUT_DIR}/api_resources.txt" 2>&1

# 15. 특정 설정 및 권한 확인
echo "15. SKRueue 실행 권한 확인 중..."
{
    echo "=== Current Context ==="
    kubectl config current-context
    echo ""
    echo "=== Can I Create Jobs? ==="
    kubectl auth can-i create jobs
    echo ""
    echo "=== Can I Patch Jobs? ==="
    kubectl auth can-i patch jobs
    echo ""
    echo "=== Can I List Workloads? ==="
    kubectl auth can-i list workloads
    echo ""
    echo "=== Can I Create ClusterQueues? ==="
    kubectl auth can-i create clusterqueues
} > "${OUTPUT_DIR}/permissions_check.txt" 2>&1

# 16. 환경 요약 생성
echo "16. 환경 요약 생성 중..."
cat > "${OUTPUT_DIR}/environment_summary.md" << EOF
# SKRueue 클러스터 환경 요약

## 수집 일시
- 수집 시간: $(date)
- kubectl 버전: $(kubectl version --client --short 2>/dev/null || echo "버전 정보 없음")

## 클러스터 기본 정보
\`\`\`
$(kubectl cluster-info 2>/dev/null | head -5 || echo "클러스터 정보 수집 실패")
\`\`\`

## 노드 정보
\`\`\`
$(kubectl get nodes 2>/dev/null || echo "노드 정보 수집 실패")
\`\`\`

## Kueue 상태
- CRD 설치: $(kubectl get crd 2>/dev/null | grep -c kueue || echo "0")개
- 실행 중인 Kueue 팟: $(kubectl get pods -A 2>/dev/null | grep -c kueue || echo "0")개

## Spark 상태  
- CRD 설치: $(kubectl get crd 2>/dev/null | grep -c spark || echo "0")개
- 실행 중인 Spark 팟: $(kubectl get pods -A 2>/dev/null | grep -c spark || echo "0")개

## 다음 단계
1. 위 정보를 바탕으로 Kueue 및 Spark Operator 설치 필요 여부 확인
2. 클러스터 리소스 용량 기반 실험 규모 결정  
3. RBAC 권한 설정 검토
4. 모니터링 시스템 구축 계획

EOF

# 17. 압축 파일 생성
echo "17. 결과 압축 중..."
tar -czf "${OUTPUT_DIR}.tar.gz" "${OUTPUT_DIR}"

echo ""
echo "=== 수집 완료 ==="
echo "결과 디렉토리: ${OUTPUT_DIR}"
echo "압축 파일: ${OUTPUT_DIR}.tar.gz"
echo ""
echo "주요 확인 항목:"
echo "1. Kueue 설치 상태: cat ${OUTPUT_DIR}/kueue_status.txt"
echo "2. 노드 리소스: cat ${OUTPUT_DIR}/nodes_wide.txt"
echo "3. 권한 확인: cat ${OUTPUT_DIR}/permissions_check.txt"
echo "4. 환경 요약: cat ${OUTPUT_DIR}/environment_summary.md"
echo ""

# 18. 즉시 확인 가능한 핵심 정보 출력
echo "=== 즉시 확인 - 핵심 정보 ==="
echo ""
echo "📊 클러스터 노드 상태:"
kubectl get nodes 2>/dev/null || echo "노드 정보 조회 실패"
echo ""
echo "🔧 Kueue 설치 상태:"
kubectl get crd 2>/dev/null | grep kueue || echo "❌ Kueue CRD 없음 - 설치 필요"
echo ""
echo "⚡ Spark 설치 상태:"  
kubectl get crd 2>/dev/null | grep spark || echo "❌ Spark CRD 없음 - 설치 필요"
echo ""
echo "🔐 기본 권한 확인:"
echo "Jobs 생성 권한: $(kubectl auth can-i create jobs 2>/dev/null || echo "확인 불가")"
echo "Jobs 수정 권한: $(kubectl auth can-i patch jobs 2>/dev/null || echo "확인 불가")"
echo ""

if [ -f "${OUTPUT_DIR}/errors.log" ]; then
    echo "⚠️ 수집 중 발생한 오류들:"
    cat "${OUTPUT_DIR}/errors.log"
    echo ""
fi

echo "✅ 전체 정보 수집 완료. 상세 분석을 위해 생성된 파일들을 확인하세요."