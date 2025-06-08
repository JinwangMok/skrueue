#!/bin/bash

# SKRueue 전체 환경 구축 자동화 스크립트
# 목적: Kubernetes 클러스터에 Kueue, Spark Operator, 모니터링 시스템 설치

set -e

# 색상 코드 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로깅 함수
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 설정 변수
KUEUE_VERSION="v0.6.2"
SPARK_OPERATOR_VERSION="v1beta2-1.3.8-3.1.1"
NAMESPACE_KUEUE="kueue-system"
NAMESPACE_SPARK="spark-operator"
NAMESPACE_MONITORING="monitoring"
NAMESPACE_TEST="skrueue-test"

# 클러스터 상태 확인
check_cluster_status() {
    log_step "클러스터 연결 상태 확인"
    
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Kubernetes 클러스터에 연결할 수 없습니다."
        log_error "kubectl 설정을 확인하세요."
        exit 1
    fi
    
    NODES=$(kubectl get nodes --no-headers | wc -l)
    log_info "연결된 노드 수: $NODES"
    
    # 노드 리소스 확인
    log_info "클러스터 리소스 요약:"
    kubectl top nodes 2>/dev/null || log_warn "metrics-server가 설치되지 않음 (나중에 설치됨)"
    
    # kubectl 버전 확인
    KUBECTL_VERSION=$(kubectl version --client --short 2>/dev/null | cut -d':' -f2 | tr -d ' ')
    log_info "kubectl 버전: $KUBECTL_VERSION"
}

# 필요한 네임스페이스 생성
create_namespaces() {
    log_step "네임스페이스 생성"
    
    NAMESPACES=($NAMESPACE_KUEUE $NAMESPACE_SPARK $NAMESPACE_MONITORING $NAMESPACE_TEST)
    
    for NS in "${NAMESPACES[@]}"; do
        if kubectl get namespace "$NS" &> /dev/null; then
            log_warn "네임스페이스 $NS 이미 존재"
        else
            kubectl create namespace "$NS"
            log_info "네임스페이스 $NS 생성 완료"
        fi
    done
}

# Helm 설치 확인
check_helm() {
    log_step "Helm 설치 확인"
    
    if ! command -v helm &> /dev/null; then
        log_warn "Helm이 설치되지 않음. 설치 중..."
        
        curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
        chmod 700 get_helm.sh
        ./get_helm.sh
        rm get_helm.sh
        
        log_info "Helm 설치 완료"
    else
        HELM_VERSION=$(helm version --short 2>/dev/null)
        log_info "Helm 버전: $HELM_VERSION"
    fi
}

# Kueue 설치
install_kueue() {
    log_step "Kueue 설치 시작"
    
    # Kueue CRD 확인
    if kubectl get crd clusterqueues.kueue.x-k8s.io &> /dev/null; then
        log_warn "Kueue가 이미 설치되어 있습니다."
        return 0
    fi
    
    # Kueue 설치
    kubectl apply --server-side -f "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
    
    # 설치 완료 대기
    log_info "Kueue 컨트롤러 시작 대기 중..."
    kubectl wait --for=condition=available --timeout=300s deployment/kueue-controller-manager -n $NAMESPACE_KUEUE
    
    log_info "Kueue 설치 완료"
    
    # 기본 ClusterQueue 및 LocalQueue 생성
    create_default_queues
}

# 기본 큐 설정 생성
create_default_queues() {
    log_step "기본 큐 설정 생성"
    
    # ClusterQueue 생성
    cat <<EOF | kubectl apply -f -
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor
spec:
  nodeLabels:
    node-type: default
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: cluster-queue
spec:
  namespaceSelector: {}
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: default-flavor
      resources:
      - name: "cpu"
        nominalQuota: 100
      - name: "memory"
        nominalQuota: 200Gi
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  namespace: $NAMESPACE_TEST
  name: test-queue
spec:
  clusterQueue: cluster-queue
EOF

    log_info "기본 큐 설정 생성 완료"
}

# Spark Operator 설치
install_spark_operator() {
    log_step "Spark Operator 설치 시작"
    
    # Spark Operator CRD 확인
    if kubectl get crd sparkapplications.sparkoperator.k8s.io &> /dev/null; then
        log_warn "Spark Operator가 이미 설치되어 있습니다."
        return 0
    fi
    
    # Helm 레포지토리 추가
    helm repo add spark-operator https://kubeflow.github.io/spark-operator/
    helm repo update
    
    # Spark Operator 설치
    helm install spark-operator spark-operator/spark-operator \
        --namespace $NAMESPACE_SPARK \
        --create-namespace \
        --set webhook.enable=true \
        --set webhook.port=8080 \
        --set metrics.enable=true \
        --set metrics.port=10254
    
    # 설치 완료 대기
    log_info "Spark Operator 시작 대기 중..."
    kubectl wait --for=condition=available --timeout=300s deployment/spark-operator -n $NAMESPACE_SPARK --selector app.kubernetes.io/instance=spark-operator
    
    log_info "Spark Operator 설치 완료"
}

# 모니터링 시스템 설치 (Prometheus + Grafana)
install_monitoring() {
    log_step "모니터링 시스템 설치 시작"
    
    # Prometheus Operator 확인
    if kubectl get crd prometheuses.monitoring.coreos.com &> /dev/null; then
        log_warn "Prometheus Operator가 이미 설치되어 있습니다."
    else
        # kube-prometheus-stack 설치
        helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
        helm repo update
        
        helm install monitoring prometheus-community/kube-prometheus-stack \
            --namespace $NAMESPACE_MONITORING \
            --create-namespace \
            --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
            --set prometheus.prometheusSpec.retention=7d \
            --set grafana.adminPassword=admin123 \
            --set grafana.service.type=NodePort \
            --set prometheus.service.type=NodePort
            
        log_info "모니터링 시스템 설치 완료"
    fi
    
    # metrics-server 설치 (없는 경우)
    if ! kubectl get deployment metrics-server -n kube-system &> /dev/null; then
        log_info "metrics-server 설치 중..."
        kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
        
        # metrics-server 설정 패치 (insecure 모드)
        kubectl patch deployment metrics-server -n kube-system --type='json' \
            -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls"}]'
            
        log_info "metrics-server 설치 완료"
    fi
}

# SKRueue 전용 RBAC 설정
setup_rbac() {
    log_step "SKRueue RBAC 설정"
    
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: skrueue-agent
  namespace: $NAMESPACE_TEST
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: skrueue-agent
rules:
- apiGroups: [""]
  resources: ["nodes", "pods", "events"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "watch", "patch", "update"]
- apiGroups: ["kueue.x-k8s.io"]
  resources: ["workloads", "localqueues", "clusterqueues"]
  verbs: ["get", "list", "watch", "patch", "update"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["get", "list", "watch", "patch", "update"]
- apiGroups: ["metrics.k8s.io"]
  resources: ["nodes", "pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: skrueue-agent
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: skrueue-agent
subjects:
- kind: ServiceAccount
  name: skrueue-agent
  namespace: $NAMESPACE_TEST
EOF

    log_info "RBAC 설정 완료"
}

# Grafana 대시보드 설정
setup_grafana_dashboard() {
    log_step "Grafana 대시보드 설정"
    
    # ConfigMap으로 대시보드 정의 생성
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: skrueue-dashboard
  namespace: $NAMESPACE_MONITORING
  labels:
    grafana_dashboard: "1"
data:
  skrueue-dashboard.json: |
    {
      "dashboard": {
        "id": null,
        "title": "SKRueue Performance Dashboard",
        "tags": ["skrueue", "kubernetes", "scheduling"],
        "timezone": "browser",
        "panels": [
          {
            "id": 1,
            "title": "Job Queue Length",
            "type": "stat",
            "targets": [
              {
                "expr": "kueue_pending_workloads",
                "legendFormat": "Pending Jobs"
              }
            ],
            "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0}
          },
          {
            "id": 2,
            "title": "Cluster Resource Utilization",
            "type": "graph",
            "targets": [
              {
                "expr": "100 - (avg(rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)",
                "legendFormat": "CPU Usage %"
              },
              {
                "expr": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
                "legendFormat": "Memory Usage %"
              }
            ],
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4}
          }
        ],
        "time": {"from": "now-1h", "to": "now"},
        "refresh": "30s"
      }
    }
EOF

    log_info "Grafana 대시보드 설정 완료"
}

# 설치 상태 확인
verify_installation() {
    log_step "설치 상태 확인"
    
    echo ""
    log_info "=== 설치 확인 결과 ==="
    
    # Kueue 확인
    if kubectl get pods -n $NAMESPACE_KUEUE | grep -q "Running"; then
        log_info "✅ Kueue: 정상 동작"
    else
        log_error "❌ Kueue: 설치 확인 필요"
    fi
    
    # Spark Operator 확인
    if kubectl get pods -n $NAMESPACE_SPARK | grep -q "Running"; then
        log_info "✅ Spark Operator: 정상 동작"
    else
        log_error "❌ Spark Operator: 설치 확인 필요"
    fi
    
    # 모니터링 확인
    if kubectl get pods -n $NAMESPACE_MONITORING | grep -q "Running"; then
        log_info "✅ 모니터링 시스템: 정상 동작"
    else
        log_error "❌ 모니터링 시스템: 설치 확인 필요"
    fi
    
    # metrics-server 확인
    if kubectl get pods -n kube-system | grep metrics-server | grep -q "Running"; then
        log_info "✅ metrics-server: 정상 동작"
    else
        log_error "❌ metrics-server: 설치 확인 필요"
    fi
    
    echo ""
    log_info "=== 접속 정보 ==="
    
    # Grafana 접속 정보
    GRAFANA_PORT=$(kubectl get svc -n $NAMESPACE_MONITORING monitoring-grafana -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "미확인")
    if [ "$GRAFANA_PORT" != "미확인" ]; then
        log_info "📊 Grafana 대시보드: http://<NODE_IP>:$GRAFANA_PORT (admin/admin123)"
    fi
    
    # Prometheus 접속 정보
    PROMETHEUS_PORT=$(kubectl get svc -n $NAMESPACE_MONITORING monitoring-kube-prometheus-prometheus -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "미확인")
    if [ "$PROMETHEUS_PORT" != "미확인" ]; then
        log_info "📈 Prometheus: http://<NODE_IP>:$PROMETHEUS_PORT"
    fi
    
    echo ""
    log_info "=== 다음 단계 ==="
    log_info "1. Python 의존성 설치: pip install -r requirements.txt"
    log_info "2. 클러스터 정보 수집: ./cluster_info_collector.sh"
    log_info "3. 데이터 수집 시작: python data_collector.py"
    log_info "4. RL 모델 훈련: python kueue_rl_interface.py --mode train"
    log_info "5. 테스트 실행: python skrueue_integration_test.py"
}

# 정리 함수
cleanup() {
    log_step "정리 작업"
    
    read -p "설치된 구성 요소를 모두 제거하시겠습니까? (y/N): " -r
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        
        log_info "SKRueue 구성 요소 제거 중..."
        
        # 네임스페이스 삭제 (내부 리소스도 함께 삭제됨)
        kubectl delete namespace $NAMESPACE_TEST --ignore-not-found=true
        kubectl delete namespace $NAMESPACE_MONITORING --ignore-not-found=true
        kubectl delete namespace $NAMESPACE_SPARK --ignore-not-found=true
        kubectl delete namespace $NAMESPACE_KUEUE --ignore-not-found=true
        
        # Helm 릴리스 제거
        helm uninstall spark-operator -n $NAMESPACE_SPARK 2>/dev/null || true
        helm uninstall monitoring -n $NAMESPACE_MONITORING 2>/dev/null || true
        
        # CRD 제거
        kubectl delete crd -l app.kubernetes.io/name=kueue --ignore-not-found=true
        kubectl delete crd -l app.kubernetes.io/name=spark-operator --ignore-not-found=true
        
        log_info "정리 완료"
    fi
}

# requirements.txt 생성
create_requirements() {
    log_step "Python 요구사항 파일 생성"
    
    cat > requirements.txt <<EOF
# SKRueue 프로젝트 Python 의존성

# Kubernetes 클라이언트
kubernetes==28.1.0

# 강화학습 라이브러리
stable-baselines3==2.2.1
gym==0.26.2
torch>=1.11.0
tensorboard>=2.10.0

# 데이터 처리
pandas>=1.5.0
numpy>=1.21.0
matplotlib>=3.5.0
seaborn>=0.11.0

# 로깅 및 설정
pyyaml>=6.0
python-dotenv>=0.19.0

# 웹 및 API
flask>=2.2.0
requests>=2.28.0

# 시각화
plotly>=5.10.0
dash>=2.6.0

# 유틸리티
tqdm>=4.64.0
python-dateutil>=2.8.0
EOF

    log_info "requirements.txt 생성 완료"
}

# 메인 함수
main() {
    echo -e "${BLUE}
╔══════════════════════════════════════════════════════════════╗
║                     SKRueue 환경 구축                        ║
║              Kubernetes RL Scheduler Setup                  ║
╚══════════════════════════════════════════════════════════════╝
${NC}"

    echo ""
    log_info "SKRueue 환경 구축을 시작합니다..."
    echo ""
    
    # 옵션 파싱
    while [[ $# -gt 0 ]]; do
        case $1 in
            --cleanup)
                cleanup
                exit 0
                ;;
            --skip-monitoring)
                SKIP_MONITORING=true
                shift
                ;;
            --help)
                echo "사용법: $0 [옵션]"
                echo "옵션:"
                echo "  --cleanup          설치된 구성 요소 제거"
                echo "  --skip-monitoring  모니터링 시스템 설치 건너뛰기"
                echo "  --help            이 도움말 표시"
                exit 0
                ;;
            *)
                log_error "알 수 없는 옵션: $1"
                exit 1
                ;;
        esac
    done
    
    # 단계별 실행
    check_cluster_status
    create_namespaces
    check_helm
    install_kueue
    install_spark_operator
    
    if [ "$SKIP_MONITORING" != "true" ]; then
        install_monitoring
        setup_grafana_dashboard
    fi
    
    setup_rbac
    create_requirements
    verify_installation
    
    echo ""
    log_info "🎉 SKRueue 환경 구축이 완료되었습니다!"
    echo ""
}

# 스크립트 실행
main "$@"