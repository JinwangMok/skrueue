"""데이터 내보내기"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from utils import get_logger
from config.settings import get_config
from .database import DatabaseManager


class DataExporter:
    """데이터 내보내기"""
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.config = get_config()
        self.logger = get_logger('DataExporter')
        self.db = db_manager or DatabaseManager()
    
    def export_training_data(self, output_path: str = None, 
                           hours: int = None) -> Optional[str]:
        """학습용 데이터셋 내보내기"""
        try:
            output_path = output_path or self.config.data.export_path
            
            # 데이터 조회
            jobs_df = self._get_jobs_dataframe(hours)
            cluster_df = self._get_cluster_states_dataframe(hours)
            
            if jobs_df.empty or cluster_df.empty:
                self.logger.warning("No data to export")
                return None
            
            # 시계열 매칭
            training_data = self._create_training_dataset(jobs_df, cluster_df)
            
            # CSV 저장
            training_data.to_csv(output_path, index=False)
            
            self.logger.info(
                f"Exported {len(training_data)} records to {output_path}"
            )
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            return None
    
    def export_metrics_summary(self, output_path: str = None) -> Optional[str]:
        """메트릭 요약 내보내기"""
        try:
            output_path = output_path or "metrics_summary.csv"
            
            # 통계 정보 가져오기
            stats = self.db.get_statistics()
            
            # DataFrame 생성
            summary_data = []
            
            # 작업 통계
            if 'jobs' in stats:
                job_stats = stats['jobs']
                summary_data.extend([
                    {'metric': 'total_jobs', 'value': job_stats.get('total_jobs', 0)},
                    {'metric': 'succeeded_jobs', 'value': job_stats.get('succeeded_jobs', 0)},
                    {'metric': 'failed_jobs', 'value': job_stats.get('failed_jobs', 0)},
                    {'metric': 'oom_jobs', 'value': job_stats.get('oom_jobs', 0)},
                    {'metric': 'avg_wait_time_minutes', 'value': job_stats.get('avg_wait_time_minutes', 0)}
                ])
            
            # 클러스터 통계
            if 'cluster' in stats:
                cluster_stats = stats['cluster']
                summary_data.extend([
                    {'metric': 'avg_cpu_utilization', 'value': cluster_stats.get('avg_cpu_util', 0)},
                    {'metric': 'avg_memory_utilization', 'value': cluster_stats.get('avg_mem_util', 0)},
                    {'metric': 'max_concurrent_jobs', 'value': cluster_stats.get('max_concurrent_jobs', 0)}
                ])
            
            df = pd.DataFrame(summary_data)
            df.to_csv(output_path, index=False)
            
            self.logger.info(f"Exported metrics summary to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Metrics export failed: {e}")
            return None
    
    def _get_jobs_dataframe(self, hours: Optional[int]) -> pd.DataFrame:
        """작업 데이터 DataFrame"""
        with self.db.get_connection() as conn:
            if hours:
                query = '''
                    SELECT * FROM jobs 
                    WHERE submission_time > datetime('now', '-' || ? || ' hours')
                '''
                df = pd.read_sql_query(query, conn, params=(hours,))
            else:
                df = pd.read_sql_query("SELECT * FROM jobs", conn)
            
            # 시간 컬럼 변환
            time_columns = ['submission_time', 'start_time', 'completion_time']
            for col in time_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            return df
    
    def _get_cluster_states_dataframe(self, hours: Optional[int]) -> pd.DataFrame:
        """클러스터 상태 DataFrame"""
        with self.db.get_connection() as conn:
            if hours:
                query = '''
                    SELECT * FROM cluster_states 
                    WHERE timestamp > datetime('now', '-' || ? || ' hours')
                '''
                df = pd.read_sql_query(query, conn, params=(hours,))
            else:
                df = pd.read_sql_query("SELECT * FROM cluster_states", conn)
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
    
    def _create_training_dataset(self, jobs_df: pd.DataFrame, 
                               cluster_df: pd.DataFrame) -> pd.DataFrame:
        """학습용 데이터셋 생성"""
        training_records = []
        
        for _, job in jobs_df.iterrows():
            if pd.isna(job['submission_time']):
                continue
            
            # 작업 제출 시점의 클러스터 상태 찾기
            time_diff = abs(cluster_df['timestamp'] - job['submission_time'])
            
            if len(time_diff) == 0:
                continue
                
            closest_idx = time_diff.idxmin()
            cluster_state = cluster_df.loc[closest_idx]
            
            # 특성 구성
            record = {
                # 작업 특성
                'job_id': job['job_id'],
                'job_name': job['name'],
                'job_cpu_request': job['cpu_request'],
                'job_memory_request': job['memory_request'],
                'job_priority': job['priority'],
                'job_type': job['job_type'],
                
                # 클러스터 상태
                'cluster_cpu_available': cluster_state['available_cpu'],
                'cluster_memory_available': cluster_state['available_memory'],
                'cluster_cpu_utilization': cluster_state['avg_cpu_utilization'],
                'cluster_memory_utilization': cluster_state['avg_memory_utilization'],
                'cluster_queue_length': cluster_state['queue_length'],
                'cluster_running_jobs': cluster_state['running_jobs_count'],
                
                # 타겟 변수
                'job_status': job['status'],
                'execution_success': 1 if job['status'] == 'Succeeded' else 0,
                'oom_occurred': int(job['oom_killed']),
                
                # 시간 정보
                'submission_timestamp': job['submission_time'],
                'wait_time_minutes': self._calculate_wait_time(job),
                'execution_time_minutes': self._calculate_execution_time(job)
            }
            
            training_records.append(record)
        
        return pd.DataFrame(training_records)
    
    def _calculate_wait_time(self, job: pd.Series) -> float:
        """대기 시간 계산 (분)"""
        if pd.notna(job['start_time']) and pd.notna(job['submission_time']):
            delta = job['start_time'] - job['submission_time']
            return delta.total_seconds() / 60.0
        return np.nan
    
    def _calculate_execution_time(self, job: pd.Series) -> float:
        """실행 시간 계산 (분)"""
        if pd.notna(job['completion_time']) and pd.notna(job['start_time']):
            delta = job['completion_time'] - job['start_time']
            return delta.total_seconds() / 60.0
        return np.nan