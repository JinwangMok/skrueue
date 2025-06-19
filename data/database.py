"""데이터베이스 관리"""
import sqlite3
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import threading
from datetime import datetime

from utils import get_logger
from config.settings import get_config


class DatabaseManager:
    """데이터베이스 관리자"""
    
    def __init__(self, db_path: str = None):
        self.config = get_config()
        self.db_path = db_path or self.config.data.db_path
        self.logger = get_logger('DatabaseManager')
        self._local = threading.local()
        
        # 데이터베이스 초기화
        self._initialize_database()
    
    @contextmanager
    def get_connection(self):
        """스레드별 연결 관리"""
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
        
        try:
            yield self._local.conn
        except Exception as e:
            self._local.conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        else:
            self._local.conn.commit()
    
    def _initialize_database(self):
        """데이터베이스 스키마 초기화"""
        with self.get_connection() as conn:
            # Jobs 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    submission_time TIMESTAMP,
                    start_time TIMESTAMP,
                    completion_time TIMESTAMP,
                    cpu_request REAL,
                    memory_request REAL,
                    cpu_limit REAL,
                    memory_limit REAL,
                    priority INTEGER,
                    user_name TEXT,
                    queue_name TEXT,
                    status TEXT,
                    job_type TEXT,
                    restart_count INTEGER DEFAULT 0,
                    oom_killed BOOLEAN DEFAULT 0,
                    metadata TEXT
                )
            ''')
            
            # Cluster States 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cluster_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL,
                    total_cpu_capacity REAL,
                    total_memory_capacity REAL,
                    available_cpu REAL,
                    available_memory REAL,
                    pending_jobs_count INTEGER,
                    running_jobs_count INTEGER,
                    queue_length INTEGER,
                    node_count INTEGER,
                    avg_cpu_utilization REAL,
                    avg_memory_utilization REAL,
                    metadata TEXT
                )
            ''')
            
            # Scheduling Events 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS scheduling_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL,
                    job_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    scheduler_name TEXT,
                    queue_wait_time REAL,
                    resource_efficiency REAL,
                    decision_metadata TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs (job_id)
                )
            ''')
            
            # Metrics 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL,
                    metric_type TEXT,
                    tags TEXT
                )
            ''')
            
            # 인덱스 생성
            conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_submission ON jobs(submission_time)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_cluster_timestamp ON cluster_states(timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_events_job ON scheduling_events(job_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name, timestamp)')
            
            self.logger.info("Database initialized successfully")
    
    def insert_job(self, job_data: Dict[str, Any]):
        """작업 데이터 삽입"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO jobs 
                (job_id, name, namespace, submission_time, start_time, completion_time,
                 cpu_request, memory_request, cpu_limit, memory_limit, priority,
                 user_name, queue_name, status, job_type, restart_count, oom_killed, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_data['job_id'], job_data['name'], job_data['namespace'],
                job_data.get('submission_time'), job_data.get('start_time'),
                job_data.get('completion_time'), job_data.get('cpu_request', 0),
                job_data.get('memory_request', 0), job_data.get('cpu_limit', 0),
                job_data.get('memory_limit', 0), job_data.get('priority', 0),
                job_data.get('user_name', 'unknown'), job_data.get('queue_name', 'default'),
                job_data.get('status', 'Unknown'), job_data.get('job_type', 'generic'),
                job_data.get('restart_count', 0), job_data.get('oom_killed', False),
                job_data.get('metadata')
            ))
    
    def insert_cluster_state(self, state_data: Dict[str, Any]):
        """클러스터 상태 삽입"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO cluster_states 
                (timestamp, total_cpu_capacity, total_memory_capacity, available_cpu,
                 available_memory, pending_jobs_count, running_jobs_count, queue_length,
                 node_count, avg_cpu_utilization, avg_memory_utilization, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                state_data['timestamp'], state_data.get('total_cpu_capacity', 0),
                state_data.get('total_memory_capacity', 0), state_data.get('available_cpu', 0),
                state_data.get('available_memory', 0), state_data.get('pending_jobs_count', 0),
                state_data.get('running_jobs_count', 0), state_data.get('queue_length', 0),
                state_data.get('node_count', 0), state_data.get('avg_cpu_utilization', 0),
                state_data.get('avg_memory_utilization', 0), state_data.get('metadata')
            ))
    
    def insert_scheduling_event(self, event_data: Dict[str, Any]):
        """스케줄링 이벤트 삽입"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO scheduling_events 
                (timestamp, job_id, event_type, scheduler_name, queue_wait_time,
                 resource_efficiency, decision_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event_data['timestamp'], event_data['job_id'], event_data['event_type'],
                event_data.get('scheduler_name', 'unknown'), event_data.get('queue_wait_time'),
                event_data.get('resource_efficiency'), event_data.get('decision_metadata')
            ))
    
    def insert_metric(self, metric_data: Dict[str, Any]):
        """메트릭 삽입"""
        with self.get_connection() as conn:
            conn.execute('''
                INSERT INTO metrics (timestamp, metric_name, metric_value, metric_type, tags)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                metric_data['timestamp'], metric_data['metric_name'],
                metric_data['metric_value'], metric_data.get('metric_type', 'gauge'),
                metric_data.get('tags')
            ))
    
    def get_recent_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """최근 작업 조회"""
        with self.get_connection() as conn:
            cursor = conn.execute('''
                SELECT * FROM jobs 
                ORDER BY submission_time DESC 
                LIMIT ?
            ''', (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_cluster_state_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """클러스터 상태 히스토리"""
        with self.get_connection() as conn:
            cursor = conn.execute('''
                SELECT * FROM cluster_states 
                WHERE timestamp > datetime('now', '-' || ? || ' hours')
                ORDER BY timestamp
            ''', (hours,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_statistics(self) -> Dict[str, Any]:
        """통계 정보"""
        with self.get_connection() as conn:
            stats = {}
            
            # 작업 통계
            cursor = conn.execute('''
                SELECT 
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END) as succeeded_jobs,
                    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failed_jobs,
                    SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) as running_jobs,
                    SUM(CASE WHEN oom_killed = 1 THEN 1 ELSE 0 END) as oom_jobs,
                    AVG(CASE 
                        WHEN start_time IS NOT NULL AND submission_time IS NOT NULL 
                        THEN (julianday(start_time) - julianday(submission_time)) * 24 * 60 
                        ELSE NULL 
                    END) as avg_wait_time_minutes
                FROM jobs
            ''')
            
            stats['jobs'] = dict(cursor.fetchone())
            
            # 클러스터 상태 통계
            cursor = conn.execute('''
                SELECT 
                    COUNT(*) as total_records,
                    AVG(avg_cpu_utilization) as avg_cpu_util,
                    AVG(avg_memory_utilization) as avg_mem_util,
                    MAX(running_jobs_count) as max_concurrent_jobs
                FROM cluster_states
                WHERE timestamp > datetime('now', '-24 hours')
            ''')
            
            stats['cluster'] = dict(cursor.fetchone())
            
            return stats