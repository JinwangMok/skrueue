"""데이터 수집 실행 스크립트"""
import argparse
import signal
import sys
import time
from datetime import datetime

from data.collector import DataCollector
from data.database import DatabaseManager
from data.exporter import DataExporter
from workload.generator import create_generator
from utils import get_logger
from config.settings import get_config


class DataCollectionRunner:
    """데이터 수집 실행기"""
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_logger('DataCollectionRunner')
        self.collector = None
        self.generator = None
        self.is_running = False
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        self.logger.info("Received termination signal, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self, duration_hours: int = None, workload_strategy: str = "realistic"):
        """데이터 수집 시작"""
        self.is_running = True
        
        # 데이터베이스 준비
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        db_path = f"data/skrueue_data_{timestamp}.db"
        db_manager = DatabaseManager(db_path)
        
        # 수집기 시작
        self.collector = DataCollector(db_manager)
        self.collector.start_collection()
        
        self.logger.info(f"Data collection started")
        self.logger.info(f"Database: {db_path}")
        
        # 워크로드 생성 (옵션)
        if workload_strategy:
            self.generator = create_generator(workload_strategy)
            self.logger.info(f"Workload generation started with strategy: {workload_strategy}")
            
            # 백그라운드에서 워크로드 생성
            import threading
            gen_thread = threading.Thread(
                target=self.generator.run_simulation,
                args=(duration_hours or 24, True)
            )
            gen_thread.daemon = True
            gen_thread.start()
        
        # 메인 루프
        start_time = time.time()
        
        try:
            while self.is_running:
                # 상태 확인
                stats = db_manager.get_statistics()
                
                self.logger.info(
                    f"Status - Jobs: {stats['jobs']['total_jobs']}, "
                    f"Cluster States: {stats['cluster']['total_records']}"
                )
                
                # 지속 시간 체크
                if duration_hours:
                    elapsed_hours = (time.time() - start_time) / 3600
                    if elapsed_hours >= duration_hours:
                        self.logger.info(f"Duration limit reached ({duration_hours} hours)")
                        break
                
                # 1분 대기
                time.sleep(60)
                
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        
        finally:
            self.stop()
            
            # 데이터 내보내기
            exporter = DataExporter(db_manager)
            export_path = f"data/training_data_{timestamp}.csv"
            exporter.export_training_data(export_path)
            
            self.logger.info(f"Data exported to: {export_path}")
    
    def stop(self):
        """수집 중지"""
        self.is_running = False
        
        if self.collector:
            self.collector.stop_collection()
            
        self.logger.info("Data collection stopped")


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='SKRueue Data Collection')
    
    parser.add_argument(
        '--duration',
        type=int,
        help='Collection duration in hours'
    )
    
    parser.add_argument(
        '--strategy',
        choices=['uniform', 'realistic', 'burst'],
        default='realistic',
        help='Workload generation strategy'
    )
    
    parser.add_argument(
        '--no-workload',
        action='store_true',
        help='Disable workload generation'
    )
    
    args = parser.parse_args()
    
    runner = DataCollectionRunner()
    
    workload_strategy = None if args.no_workload else args.strategy
    
    runner.start(
        duration_hours=args.duration,
        workload_strategy=workload_strategy
    )


if __name__ == "__main__":
    main()