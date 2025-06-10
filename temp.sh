# 기존 data_collector 종료
pkill -f data_collector.py

# 새로 시작 (더 자주 수집)
python data_collector.py --interval 5 --namespaces default skrueue-test > collector_new.log 2>&1 &
echo "새 Data Collector PID: $!"