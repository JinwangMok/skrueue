import argparse
import logging
import sys
from pathlib import Path

from config.simulation_config import SimulationConfig, create_default_config
from data_augmentation.data_generator import DataAugmenter
from experiments import experiment_runner

def setup_logging(verbose: bool = False):
    """로깅 설정"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def setup_project():
    """프로젝트 초기 설정"""
    # 필요한 디렉토리 생성
    directories = ['data', 'results', 'models', 'config', 'logs']
    for dir_name in directories:
        Path(dir_name).mkdir(exist_ok=True)
    
    # 기본 설정 파일 생성
    config_path = Path('config/default_config.yaml')
    if not config_path.exists():
        config = create_default_config()
        config.to_yaml(str(config_path))
        print(f"Created default config at {config_path}")
    
    print("Project setup completed!")

def main():
    parser = argparse.ArgumentParser(description="SKRueue Simulator")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # 설정 명령
    setup_parser = subparsers.add_parser("setup", help="Setup project structure")
    
    # 데이터 생성 명령
    data_parser = subparsers.add_parser("generate", help="Generate training data")
    data_parser.add_argument("--duration", type=int, default=86400, help="Simulation duration (seconds)")
    data_parser.add_argument("--episodes", type=int, default=100, help="Number of episodes")
    data_parser.add_argument("--output", type=str, default="data/training_data.csv", help="Output file")
    
    # 학습 명령
    train_parser = subparsers.add_parser("train", help="Train RL models")
    train_parser.add_argument("--algorithms", nargs="+", default=["DQN", "PPO", "A2C"], help="Algorithms to train")
    train_parser.add_argument("--steps", type=int, default=50000, help="Training steps")
    train_parser.add_argument("--config", type=str, help="Config file path")
    
    # 평가 명령
    eval_parser = subparsers.add_parser("evaluate", help="Evaluate models")
    eval_parser.add_argument("--experiment", type=str, required=True, help="Experiment ID")
    eval_parser.add_argument("--scenarios", nargs="+", help="Evaluation scenarios")
    
    # 전체 실험 명령
    exp_parser = subparsers.add_parser("experiment", help="Run full experiment")
    exp_parser.add_argument("--config", type=str, help="Config file path")
    exp_parser.add_argument("--training-steps", type=int, default=50000, help="Training steps")
    
    # 공통 옵션
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # 명령이 없으면 도움말 출력
    if args.command is None:
        parser.print_help()
        return
    
    # setup 명령은 로깅 전에 실행
    if args.command == "setup":
        setup_project()
        return
    
    # 로깅 설정
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    # 설정 로드
    if hasattr(args, 'config') and args.config:
        config = SimulationConfig.from_yaml(args.config)
    else:
        config = create_default_config()
    
    # 명령 실행
    if args.command == "generate":
        logger.info("Generating training data...")
        augmenter = DataAugmenter(config)
        df = augmenter.generate_training_data(args.duration, args.episodes)
        
        # 출력 디렉토리 생성
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 데이터 저장
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} samples to {output_path}")
    
    elif args.command == "train":
        logger.info("Starting training...")
        runner = experiment_runner(config)
        results = runner.run_training_experiment(args.algorithms, args.steps)
        
        logger.info("Training completed!")
        for algo, result in results.items():
            logger.info(f"{algo}: Training time = {result['training_time']:.2f}s")
    
    elif args.command == "evaluate":
        logger.info("Starting evaluation...")
        
        # 모델 경로 찾기
        exp_dir = Path(f"results/{args.experiment}")
        if not exp_dir.exists():
            logger.error(f"Experiment {args.experiment} not found")
            return
        
        model_paths = {}
        for algo in ["DQN", "PPO", "A2C"]:
            model_path = exp_dir / f"{algo}_model.zip"
            if model_path.exists():
                model_paths[algo] = str(model_path)
        
        if not model_paths:
            logger.error("No models found in experiment directory")
            return
        
        runner = experiment_runner(config)
        results = runner.run_evaluation_experiment(model_paths)
        
        logger.info("Evaluation completed!")
    
    elif args.command == "experiment":
        logger.info("Running full experiment...")
        runner = experiment_runner(config)
        
        # 1. 학습
        logger.info("Phase 1: Training models...")
        training_results = runner.run_training_experiment(
            algorithms=["DQN", "PPO", "A2C"],
            training_steps=args.training_steps
        )
        
        # 2. 평가
        logger.info("Phase 2: Evaluating models...")
        model_paths = {
            algo: result["model_path"] 
            for algo, result in training_results.items()
        }
        evaluation_results = runner.run_evaluation_experiment(model_paths)
        
        logger.info(f"Experiment completed! Results saved in: {runner.experiment_dir}")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()