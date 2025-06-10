#!/bin/bash
echo "🚀 올바른 데이터 수집 테스트 시작"
echo "=================================="

# 1. 기존 프로세스 정리
echo "1️⃣ 기존 프로세스 정리..."
pkill -f sample_workload_generator.py 2>/dev/null || true
pkill -f data_collector.py 2>/dev/null || true
sleep 2

# 2. 데이터 수집 시작
echo "2️⃣ 데이터 수집기 시작..."
python data_collector.py --interval 10 --namespaces default skrueue-test &
DATA_COLLECTOR_PID=$!
echo "   Data Collector PID: $DATA_COLLECTOR_PID"
sleep 5

# 3. 워크로드 생성 (명시적으로 batch 모드)
echo "3️⃣ 워크로드 생성 (batch 모드)..."
echo "   30개 작업을 45초 간격으로 생성"

python sample_workload_generator.py \
    --mode batch \
    --count 30 \
    --interval 45 \
    --force-k8s-jobs

echo "✅ 워크로드 생성 완료"

# 4. 생성된 작업 즉시 확인
echo ""
echo "4️⃣ 생성된 작업 확인..."
kubectl get jobs -n skrueue-test --sort-by=.metadata.creationTimestamp
echo ""
kubectl get pods -n skrueue-test | head -10

# 5. 5분 대기 후 데이터 확인
echo ""
echo "5️⃣ 5분 대기 후 데이터 수집 확인..."
echo "   대기 시작: $(date)"

for i in {1..5}; do
    echo "   ${i}/5분 경과..."
    sleep 60
done

echo "   대기 완료: $(date)"

# 6. 데이터베이스 상태 확인
echo ""
echo "6️⃣ 데이터베이스 상태 확인..."
python check_database.py

# 7. CSV 내보내기
echo ""
echo "7️⃣ CSV 내보내기..."
python data_collector.py --export-only --output test_baseline_training_data.csv

# 8. 결과 확인
echo ""
echo "8️⃣ 최종 결과 확인..."
if [ -f "test_baseline_training_data.csv" ]; then
    echo "✅ CSV 파일 생성 성공!"
    echo "   파일 크기: $(ls -lh test_baseline_training_data.csv | awk '{print $5}')"
    echo "   레코드 수: $(wc -l test_baseline_training_data.csv)"
    echo ""
    echo "📋 첫 3줄 미리보기:"
    head -3 test_baseline_training_data.csv
else if [ -f "test_export.csv" ]; then
    echo "✅ CSV 파일 생성 성공!"
    echo "   파일 크기: $(ls -lh test_export.csv | awk '{print $5}')"
    echo "   레코드 수: $(wc -l test_export.csv)"
    echo ""
    echo "📋 첫 3줄 미리보기:"
    head -3 test_export.csv
    echo "   (test_export.csv 파일 이름 변경 => test_baseline_training_data.csv)"
    mv test_export.csv test_baseline_training_data.csv
    echo "✅ 파일 이름 변경 완료"
else
    echo "❌ CSV 파일 생성 실패"
fi

echo ""
echo "🎯 테스트 완료: $(date)"
echo "=================================="