# SKRueue - Kubernetes RL-based Scheduler

SKRueue는 Kubernetes의 Kueue 스케줄러에 강화학습(Reinforcement Learning)을 적용하여 작업 스케줄링을 최적화하는 시스템입니다.

RL 훈련을 위한 공개 데이터 셋은 아래 링크에서 제공받을 수 있습니다. (Ensurance due: 2025.06.30)
https://drive.google.com/file/d/1Hiv4E8SJtf5m0xzbIgdDhXY-Tt0QfqJq/view?usp=sharing

## 🚀 주요 특징

-   **67차원 상태 공간**: 클러스터 상태와 작업 큐 정보를 포함한 풍부한 상태 표현
-   **다양한 RL 알고리즘**: DQN, PPO, A2C 지원
-   **실시간 데이터 수집**: 클러스터 메트릭과 작업 실행 데이터 자동 수집
-   **현실적인 워크로드 시뮬레이션**: 시간대별 패턴을 반영한 작업 생성
-   **성능 벤치마크**: 기존 스케줄러와의 성능 비교 도구 내장

## 📋 요구사항

-   Python 3.8+
-   Kubernetes 클러스터 (1.20+)
-   Kueue 설치 (선택사항)
-   kubectl 설정

## 🛠 설치

### 1. 프로젝트 클론

```bash
git clone https://github.com/yourusername/skrueue.git
cd skrueue
```

### 2. Python 환경 설정

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 3. 환경 설정

```bash
python main.py setup
```

이 명령은 다음을 수행합니다:

-   Kubernetes 연결 확인
-   필요한 네임스페이스 생성
-   RBAC 권한 설정
-   디렉토리 구조 생성
-   기본 설정 파일 생성

## 📁 프로젝트 구조

```
skrueue/
├── core/                  # 핵심 모듈
│   ├── environment.py     # RL 환경 (67차원 상태 공간)
│   ├── agent.py          # RL 에이전트 (DQN/PPO/A2C)
│   └── interface.py      # Kueue 인터페이스
├── utils/                # 공통 유틸리티
│   ├── k8s_utils.py     # Kubernetes 헬퍼
│   ├── resource_parser.py # 리소스 파싱
│   └── logger.py        # 로깅
├── data/                 # 데이터 관리
│   ├── collector.py     # 실시간 수집
│   ├── database.py      # SQLite 관리
│   └── exporter.py      # CSV 내보내기
├── workload/            # 워크로드 생성
│   ├── generator.py     # 통합 생성기
│   ├── templates.py     # 작업 템플릿
│   └── strategies.py    # 생성 전략
├── test/                # 테스트 및 벤치마크
│   ├── integration.py   # 통합 테스트
│   ├── benchmark.py     # 성능 벤치마크
│   └── monitor.py       # 모니터링
├── scripts/             # 실행 스크립트
│   ├── setup.py        # 환경 설정
│   ├── run_experiment.py # 실험 실행
│   └── collect_data.py  # 데이터 수집
├── config/              # 설정 파일
│   └── settings.py      # 중앙 설정 관리
└── main.py             # 메인 진입점
```

## 🎯 사용법

### 1. 데이터 수집

```bash
# 24시간 동안 현실적인 패턴으로 데이터 수집
python main.py collect --duration 24 --strategy realistic

# 워크로드 생성 없이 기존 클러스터 데이터만 수집
python main.py collect --no-workload
```

### 2. RL 모델 훈련

```bash
# DQN 모델 훈련 (기본값: 20,000 스텝)
python main.py train --algorithm DQN --timesteps 50000

# PPO 모델 훈련
python main.py train --algorithm PPO --timesteps 100000 --name my_experiment
```

### 3. 추론 실행

```bash
# 훈련된 모델로 실시간 스케줄링
python main.py inference --model models/skrueue_dqn_model --namespace skrueue-test
```

### 4. 성능 벤치마크

```bash
# 30분 동안 모든 알고리즘 비교
python main.py benchmark --duration 30 --algorithms DQN PPO A2C

# 특정 알고리즘만 테스트
python main.py benchmark --duration 15 --algorithms DQN --name quick_test
```

### 5. 테스트 실행

```bash
# 통합 테스트 실행
python main.py test
```

## ⚙️ 설정

`config/skrueue.yaml` 파일을 수정하여 설정을 변경할 수 있습니다:

```yaml
# Kueue 설정
kueue:
    namespaces: [skrueue-test, default]
    max_queue_size: 10 # 상태 공간의 작업 큐 크기

# RL 설정
rl:
    algorithm: DQN
    learning_rate: 0.0001
    training_steps: 20000
    reward_weights:
        throughput: 0.4 # 처리량 보상 가중치
        utilization: 0.3 # 자원 활용도 보상
        wait_penalty: 0.2 # 대기시간 페널티
        failure_penalty: 0.1 # 실패 페널티

# 데이터 수집 설정
data:
    collection_interval: 10 # 초 단위
    db_path: data/skrueue_training_data.db
```

## 📊 상태 공간 구조

SKRueue는 67차원 상태 공간을 사용합니다:

-   **차원 0-3**: 클러스터 리소스 정보
    -   CPU 가용률, 메모리 가용률, CPU 사용률, 메모리 사용률
-   **차원 4-6**: 클러스터 히스토리
    -   실행 중인 작업 수, CPU 사용률(히스토리), 최근 OOM 발생률
-   **차원 7-66**: 작업 큐 정보 (최대 10개 작업 × 6차원)
    -   각 작업: CPU 요청, 메모리 요청, 우선순위, 대기시간, 예상 실행시간, 작업 타입

## 📈 성능 메트릭

벤치마크에서 측정하는 주요 지표:

-   **처리량 (Throughput)**: 시간당 완료된 작업 수
-   **평균 대기시간**: 작업 제출부터 실행까지의 시간
-   **성공률**: 성공적으로 완료된 작업의 비율
-   **자원 활용도**: CPU/메모리 평균 사용률
-   **OOM 발생률**: Out-of-Memory로 실패한 작업 수

## 🔧 고급 사용법

### 사용자 정의 워크로드 템플릿

`workload/templates.py`에서 새로운 작업 템플릿을 추가할 수 있습니다:

```python
JobTemplate(
    name="custom-task",
    category="custom",
    cpu_request="2000m",
    memory_request="8Gi",
    estimated_duration=30,
    priority=7,
    command=["python", "my_script.py"]
)
```

### 사용자 정의 보상 함수

`core/environment.py`의 `_calculate_reward()` 메서드를 수정하여 보상 함수를 커스터마이즈할 수 있습니다.

### 네트워크 아키텍처 변경

`core/agent.py`의 `_get_network_architecture()` 메서드를 수정하여 신경망 구조를 변경할 수 있습니다.

## 🐛 문제 해결

### kubectl 연결 오류

```bash
# kubeconfig 확인
kubectl cluster-info

# 컨텍스트 확인
kubectl config current-context
```

### Kueue 설치 확인

```bash
# CRD 확인
kubectl get crd | grep kueue

# Kueue 설치 (필요시)
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.6.2/manifests.yaml
```

### 메모리 부족

-   `config.rl.buffer_size` 값을 줄이세요
-   배치 크기를 조정하세요

## 🤝 기여하기

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 📚 참고 문헌

-   [Kubernetes Documentation](https://kubernetes.io/docs/)
-   [Kueue Documentation](https://kueue.sigs.k8s.io/)
-   [Stable-Baselines3 Documentation](https://stable-baselines3.readthedocs.io/)

## ✨ 로드맵

-   [ ] 분산 훈련 지원
-   [ ] 더 많은 RL 알고리즘 추가 (SAC, TD3)
-   [ ] 웹 기반 대시보드
-   [ ] Prometheus 메트릭 통합
-   [ ] 다중 클러스터 지원

---

**주의**: 이 프로젝트는 연구 목적으로 개발되었습니다. 프로덕션 환경에서 사용하기 전에 충분한 테스트를 수행하세요.
