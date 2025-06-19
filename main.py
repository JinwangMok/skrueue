"""SKRueue 메인 진입점"""
import argparse
import sys
from typing import List

from scripts.setup import EnvironmentSetup
from scripts.run_experiment import ExperimentRunner
from scripts.collect_data import DataCollectionRunner
from test.integration import run_integration_tests
from utils import get_logger


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='SKRueue - Kubernetes RL-based Scheduler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Environment setup
  python main.py setup
  
  # Train RL model
  python main.py train --algorithm DQN --timesteps 50000
  
  # Run inference
  python main.py inference --model models/skrueue_dqn_model
  
  # Collect data
  python main.py collect --duration 24 --strategy realistic
  
  # Run benchmark
  python main.py benchmark --duration 30 --algorithms DQN PPO
  
  # Run tests
  python main.py test
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Setup environment')
    
    # Train command
    train_parser = subparsers.add_parser('train', help='Train RL model')
    train_parser.add_argument('--algorithm', choices=['DQN', 'PPO', 'A2C'], 
                            default='DQN', help='RL algorithm')
    train_parser.add_argument('--timesteps', type=int, default=20000, 
                            help='Training timesteps')
    train_parser.add_argument('--name', type=str, help='Experiment name')
    
    # Inference command
    inference_parser = subparsers.add_parser('inference', help='Run RL inference')
    inference_parser.add_argument('--model', type=str, required=True, 
                                help='Model path')
    inference_parser.add_argument('--namespace', type=str, default='skrueue-test',
                                help='Target namespace')
    
    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect data')
    collect_parser.add_argument('--duration', type=int, help='Duration in hours')
    collect_parser.add_argument('--strategy', choices=['uniform', 'realistic', 'burst'],
                              default='realistic', help='Workload strategy')
    collect_parser.add_argument('--no-workload', action='store_true',
                              help='Disable workload generation')
    
    # Benchmark command
    benchmark_parser = subparsers.add_parser('benchmark', help='Run benchmark')
    benchmark_parser.add_argument('--duration', type=int, default=30,
                                help='Duration in minutes')
    benchmark_parser.add_argument('--algorithms', nargs='*', 
                                default=['DQN', 'PPO', 'A2C'],
                                help='Algorithms to test')
    benchmark_parser.add_argument('--name', type=str, help='Experiment name')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Run tests')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    logger = get_logger('SKRueue')
    
    try:
        if args.command == 'setup':
            logger.info("Running environment setup...")
            setup = EnvironmentSetup()
            setup.setup_all()
            
        elif args.command == 'train':
            logger.info("Starting training...")
            runner = ExperimentRunner(args.name)
            runner.run_training_experiment(args.algorithm, args.timesteps)
            
        elif args.command == 'inference':
            logger.info("Starting inference...")
            from core.environment import SKRueueEnvironment
            from core.agent import RLAgent
            from core.interface import KueueInterface
            
            # 환경 설정
            kueue = KueueInterface([args.namespace])
            env = SKRueueEnvironment(kueue)
            
            # 모델 로드
            algorithm = 'DQN'  # 모델 파일에서 추론하거나 인자로 받을 수 있음
            agent = RLAgent(env, algorithm)
            agent.load(args.model)
            
            logger.info(f"Running inference with model: {args.model}")
            logger.info("Press Ctrl+C to stop")
            
            # 추론 루프
            obs, info = env.reset()
            step = 0
            
            while True:
                action = agent.predict(obs)
                obs, reward, terminated, truncated, info = env.step(action)
                
                step += 1
                logger.info(
                    f"Step {step}: action={action}, reward={reward:.3f}, "
                    f"queue={info['queue_length']}"
                )
                
                if terminated or truncated:
                    obs, info = env.reset()
                    step = 0
                
                import time
                time.sleep(5)
                
        elif args.command == 'collect':
            logger.info("Starting data collection...")
            runner = DataCollectionRunner()
            workload_strategy = None if args.no_workload else args.strategy
            runner.start(args.duration, workload_strategy)
            
        elif args.command == 'benchmark':
            logger.info("Starting benchmark...")
            runner = ExperimentRunner(args.name)
            runner.run_benchmark_experiment(args.duration, args.algorithms)
            
        elif args.command == 'test':
            logger.info("Running tests...")
            run_integration_tests()
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()