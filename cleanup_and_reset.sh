#!/bin/bash
echo "🧹 SKRueue 환경 정리 및 초기화 스크립트"
echo "=========================================="

# 1. 실행 중인 data_collector 프로세스 종료
echo "1️⃣ Data Collector 프로세스 종료..."
pkill -f data_collector.py && echo "✅ Data Collector 종료됨" || echo "⚠️  실행 중인 Data Collector 없음"

# 2. 테스트용으로 생성된 작업들 정리
echo ""
echo "2️⃣ 테스트 작업들 정리..."

# Kubernetes Jobs 정리
kubectl delete jobs --all -n skrueue-test 2>/dev/null && echo "✅ Kubernetes Jobs 정리됨" || echo "⚠️  정리할 Jobs 없음"

# SparkApplications 정리 
kubectl delete sparkapplications --all -n skrueue-test 2>/dev/null && echo "✅ SparkApplications 정리됨" || echo "⚠️  정리할 SparkApplications 없음"

# ConfigMaps 정리
kubectl delete configmaps -l app=skrueue-test -n skrueue-test 2>/dev/null && echo "✅ ConfigMaps 정리됨" || echo "⚠️  정리할 ConfigMaps 없음"

# 테스트 ConfigMap들 개별 정리
for config in test-spark-config $(kubectl get configmaps -n skrueue-test --no-headers | grep -E "(big-data|ml-training|etl-pipeline|realtime-analytics|data-validation|memory-stress)" | awk '{print $1}'); do
    kubectl delete configmap "$config" -n skrueue-test 2>/dev/null
done

echo "✅ 모든 테스트 작업 정리 완료"

# 3. 데이터베이스 및 로그 파일 정리
echo ""
echo "3️⃣ 데이터베이스 및 로그 파일 정리..."

if [ -f "skrueue_training_data.db" ]; then
    rm -f skrueue_training_data.db
    echo "✅ 데이터베이스 파일 삭제됨"
else
    echo "⚠️  데이터베이스 파일 없음"
fi

if [ -f "skrueue_collector.log" ]; then
    rm -f skrueue_collector.log  
    echo "✅ 로그 파일 삭제됨"
else
    echo "⚠️  로그 파일 없음"
fi

# 기존 CSV 파일들 정리
rm -f *training_data.csv test_export.csv working_training_data.csv 2>/dev/null
echo "✅ CSV 파일들 정리됨"

# 4. 임시 파일들 정리
echo ""
echo "4️⃣ 임시 파일들 정리..."
rm -f /tmp/debug_job_*.yaml 2>/dev/null
rm -f debug/* 2>/dev/null
echo "✅ 임시 파일들 정리됨"

# 5. ServiceAccount 및 권한은 유지 (재사용 가능)
echo ""
echo "5️⃣ 권한 설정 확인..."
kubectl get serviceaccount skrueue-agent -n skrueue-test >/dev/null 2>&1 && echo "✅ ServiceAccount 유지됨" || echo "⚠️  ServiceAccount 없음"
kubectl get role spark-role -n skrueue-test >/dev/null 2>&1 && echo "✅ Role 유지됨" || echo "⚠️  Role 없음"

# 6. 현재 상태 확인
echo ""
echo "6️⃣ 정리 후 상태 확인..."
echo "📋 남은 작업들:"
kubectl get jobs,pods,sparkapplications,configmaps -n skrueue-test 2>/dev/null || echo "   (없음)"

echo ""
echo "📁 현재 디렉토리 주요 파일들:"
ls -la *.py *.db *.log *.csv 2>/dev/null || echo "   (정리됨)"

echo ""
echo "🎯 정리 완료! 이제 README.md Phase 2부터 새로 시작할 수 있습니다."
echo "   다음 명령어로 시작하세요:"
echo "   python data_collector.py --interval 10 --namespaces default skrueue-test &"
echo "=========================================="