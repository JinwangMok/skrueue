# check_database.py
# SKRueue 데이터베이스 검사 및 CSV 내보내기 테스트 스크립트

import sqlite3
import os
import pandas as pd
from datetime import datetime

def check_database(db_path="skrueue_training_data.db"):
    """데이터베이스 내용 확인"""
    
    if not os.path.exists(db_path):
        print(f"❌ 데이터베이스 파일이 존재하지 않음: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        
        print(f"✅ 데이터베이스 연결 성공: {db_path}")
        print(f"📁 파일 크기: {os.path.getsize(db_path) / 1024 / 1024:.2f} MB")
        print()
        
        # 테이블 목록 확인
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("📊 테이블 목록:")
        for table in tables:
            print(f"  - {table[0]}")
        print()
        
        # 각 테이블의 레코드 수 확인
        for table_name in ['jobs', 'cluster_states', 'scheduling_events']:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                print(f"📋 {table_name}: {count:,} 레코드")
            except sqlite3.OperationalError as e:
                print(f"❌ {table_name} 테이블 오류: {e}")
        
        print()
        
        # Jobs 테이블 상세 정보 (있는 경우)
        try:
            cursor.execute("SELECT COUNT(*) FROM jobs;")
            job_count = cursor.fetchone()[0]
            
            if job_count > 0:
                print("🔍 Jobs 테이블 샘플 데이터:")
                cursor.execute("""
                    SELECT name, status, namespace, submission_time 
                    FROM jobs 
                    ORDER BY submission_time DESC 
                    LIMIT 5;
                """)
                
                jobs = cursor.fetchall()
                for job in jobs:
                    print(f"  - {job[0]} | {job[1]} | {job[2]} | {job[3]}")
                print()
                
                # 상태별 작업 수
                print("📈 작업 상태별 분포:")
                cursor.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM jobs 
                    GROUP BY status 
                    ORDER BY count DESC;
                """)
                
                status_counts = cursor.fetchall()
                for status, count in status_counts:
                    print(f"  - {status}: {count}")
                print()
                
        except sqlite3.OperationalError as e:
            print(f"⚠️  Jobs 테이블 조회 오류: {e}")
        
        # Cluster States 테이블 정보
        try:
            cursor.execute("SELECT COUNT(*) FROM cluster_states;")
            cluster_count = cursor.fetchone()[0]
            
            if cluster_count > 0:
                print("🖥️  클러스터 상태 데이터:")
                cursor.execute("""
                    SELECT timestamp, total_cpu_capacity, total_memory_capacity, 
                           available_cpu, available_memory, running_jobs_count 
                    FROM cluster_states 
                    ORDER BY timestamp DESC 
                    LIMIT 3;
                """)
                
                states = cursor.fetchall()
                for state in states:
                    print(f"  - {state[0]} | CPU: {state[1]:.1f}/{state[3]:.1f} | "
                          f"MEM: {state[2]:.1f}/{state[4]:.1f} | Jobs: {state[5]}")
                print()
                
        except sqlite3.OperationalError as e:
            print(f"⚠️  Cluster States 테이블 조회 오류: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 데이터베이스 오류: {e}")
        return False

def export_csv_test(db_path="skrueue_training_data.db", output_file="test_export.csv"):
    """CSV 내보내기 테스트"""
    
    print("📤 CSV 내보내기 테스트...")
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 테이블 존재 확인
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs;")
        job_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM cluster_states;")
        cluster_count = cursor.fetchone()[0]
        
        print(f"📊 내보낼 데이터: Jobs {job_count}, Cluster States {cluster_count}")
        
        if job_count == 0 or cluster_count == 0:
            print("❌ 내보낼 데이터가 부족합니다.")
            conn.close()
            return False
        
        # 간단한 조인 쿼리로 훈련 데이터 생성
        query = """
        SELECT 
            j.name as job_name,
            j.cpu_request,
            j.memory_request,
            j.status,
            j.submission_time,
            c.available_cpu,
            c.available_memory,
            c.running_jobs_count,
            c.pending_jobs_count
        FROM jobs j
        LEFT JOIN cluster_states c ON 
            datetime(j.submission_time) >= datetime(c.timestamp, '-1 hour')
            AND datetime(j.submission_time) <= datetime(c.timestamp, '+1 hour')
        WHERE j.submission_time IS NOT NULL
        LIMIT 1000;
        """
        
        df = pd.read_sql_query(query, conn)
        
        if len(df) > 0:
            df.to_csv(output_file, index=False)
            print(f"✅ CSV 내보내기 성공: {output_file}")
            print(f"📊 내보낸 레코드 수: {len(df)}")
            print(f"📁 파일 크기: {os.path.getsize(output_file)} bytes")
            
            # 첫 3줄 미리보기
            print("\n📋 CSV 내용 미리보기:")
            print(df.head(3).to_string(index=False))
            
            conn.close()
            return True
        else:
            print("❌ 조인된 데이터가 없습니다.")
            conn.close()
            return False
            
    except Exception as e:
        print(f"❌ CSV 내보내기 오류: {e}")
        return False

if __name__ == "__main__":
    print("🔍 SKRueue 데이터베이스 검사 시작...")
    print("=" * 50)
    
    # 1. 데이터베이스 내용 확인
    if check_database():
        print("=" * 50)
        
        # 2. CSV 내보내기 테스트
        export_csv_test()
        
    print("\n🔍 검사 완료!")