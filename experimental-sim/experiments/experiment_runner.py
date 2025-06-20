import os
import json
import time
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
import matplotlib.pyplot as plt
import seaborn as sns

from config.simulation_config import SimulationConfig, create_default_config
from simulator.cluster_simulator import ClusterSimulator
from simulator.job_generator import JobGenerator
from rl_environment.simulated_env import SimulatedKubernetesEnv
from models.rl_agents import SchedulingAgent, TrainingCallback
from data_augmentation.data_generator import DataAugmenter

class ExperimentRunner:
    """실험 실행 및 결과 관리"""
    
    def __init__(self, config: SimulationConfig = None):
        self.config = config or create_default_config()
        self.results_dir = "results"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # 실험 ID 생성
        self.experiment_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.experiment_dir = os.path.join(self.results_dir, self.experiment_id)
        os.makedirs(self.experiment_dir, exist_ok=True)
    
    def run_training_experiment(self, 
                              algorithms: List[str] = ["DQN", "PPO", "A2C"],
                              training_steps: int = 50000):
        """학습 실험 실행"""
        results = {}
        
        for algorithm in algorithms:
            print(f"\n{'='*50}")
            print(f"Training {algorithm}")
            print(f"{'='*50}")
            
            # 환경 생성
            env = SimulatedKubernetesEnv(self.config)
            eval_env = SimulatedKubernetesEnv(self.config)
            
            # 에이전트 생성
            agent = SchedulingAgent(env, algorithm)
            
            # 콜백 설정
            callback = TrainingCallback(eval_env, eval_freq=1000)
            
            # 학습 시작
            start_time = time.time()
            agent.train(training_steps, callbacks=[callback])
            training_time = time.time() - start_time
            
            # 모델 저장
            model_path = os.path.join(self.experiment_dir, f"{algorithm}_model")
            agent.save(model_path)
            
            # 결과 저장
            results[algorithm] = {
                "training_time": training_time,
                "evaluations": callback.evaluations,
                "model_path": model_path
            }
            
            # 학습 곡선 플롯
            self._plot_learning_curve(callback.evaluations, algorithm)
        
        # 결과 저장
        with open(os.path.join(self.experiment_dir, "training_results.json"), "w") as f:
            json.dump(results, f, indent=2)
        
        return results
    
    def run_evaluation_experiment(self, 
                                model_paths: Dict[str, str],
                                scenarios: List[Dict] = None):
        """평가 실험 실행"""
        if scenarios is None:
            augmenter = DataAugmenter(self.config)
            scenarios = augmenter.generate_evaluation_scenarios()
        
        results = {}
        
        for scenario in scenarios:
            print(f"\n{'='*50}")
            print(f"Evaluating scenario: {scenario['name']}")
            print(f"Description: {scenario['description']}")
            print(f"{'='*50}")
            
            scenario_results = {}
            
            # 각 알고리즘 평가
            for algorithm, model_path in model_paths.items():
                metrics = self._evaluate_model(model_path, algorithm, scenario)
                scenario_results[algorithm] = metrics
            
            # 베이스라인 평가
            for baseline in ["random", "first_fit", "best_fit"]:
                metrics = self._evaluate_baseline(baseline, scenario)
                scenario_results[baseline] = metrics
            
            results[scenario['name']] = scenario_results
        
        # 결과 저장 및 시각화
        self._save_evaluation_results(results)
        self._visualize_comparison(results)
        
        return results
    
    def _evaluate_model(self, model_path: str, algorithm: str, scenario: Dict) -> Dict:
        """단일 모델 평가"""
        # 환경 생성
        env = SimulatedKubernetesEnv(self.config)
        
        # 모델 로드
        agent = SchedulingAgent(env, algorithm)
        agent.load(model_path)
        
        # 평가 실행
        metrics = self._run_evaluation(env, agent, scenario)
        
        return metrics
    
    def _evaluate_baseline(self, policy: str, scenario: Dict) -> Dict:
        """베이스라인 정책 평가"""
        # 시뮬레이터 직접 사용
        simulator = ClusterSimulator(self.config)
        job_generator = JobGenerator(self.config)
        
        # 워크로드 생성
        workload = job_generator.generate_workload_pattern(
            scenario["duration"],
            scenario.get("arrival_rate", 0.5)
        )
        
        # 시뮬레이션 실행
        simulator.reset()
        workload_idx = 0
        
        while simulator.current_time < scenario["duration"]:
            # 작업 제출
            while (workload_idx < len(workload) and 
                   workload[workload_idx][0] <= simulator.current_time):
                _, job = workload[workload_idx]
                simulator.submit_job(job)
                workload_idx += 1
            
            # 스케줄링
            self._apply_baseline_policy(simulator, policy)
            
            # 시간 진행
            simulator.step(self.config.tick_interval)
        
        # 메트릭 수집
        return self._collect_metrics(simulator)
    
    def _apply_baseline_policy(self, simulator: ClusterSimulator, policy: str):
        """베이스라인 정책 적용"""
        if len(simulator.pending_queue) == 0:
            return
        
        if policy == "random":
            job = simulator.pending_queue[0]
            nodes = list(range(len(simulator.nodes)))
            np.random.shuffle(nodes)
            
            for node_id in nodes:
                if simulator.schedule_job(job.id, node_id):
                    simulator.pending_queue.remove(job)
                    break
        
        elif policy == "first_fit":
            job = simulator.pending_queue[0]
            for node_id in range(len(simulator.nodes)):
                if simulator.schedule_job(job.id, node_id):
                    simulator.pending_queue.remove(job)
                    break
        
        elif policy == "best_fit":
            job = simulator.pending_queue[0]
            best_node = None
            min_waste = float('inf')
            
            for node_id, node in enumerate(simulator.nodes):
                if node.can_fit_job(job):
                    waste = (node.cpu_available - job.cpu_request) + \
                           (node.memory_available - job.memory_request)
                    if waste < min_waste:
                        min_waste = waste
                        best_node = node_id
            
            if best_node is not None:
                if simulator.schedule_job(job.id, best_node):
                    simulator.pending_queue.remove(job)
    
    def _run_evaluation(self, env, agent, scenario: Dict) -> Dict:
        """환경에서 에이전트 평가"""
        total_rewards = []
        episode_metrics = []
        
        for episode in range(10):  # 10회 반복 평가
            obs, info = env.reset()
            done = False
            episode_reward = 0
            
            while not done:
                action = agent.predict(obs, deterministic=True)
                obs, reward, terminated, truncated, info = env.step(action)
                done = terminated or truncated
                episode_reward += reward
            
            total_rewards.append(episode_reward)
            episode_metrics.append(info)
        
        # 평균 메트릭 계산
        metrics = {
            "avg_reward": np.mean(total_rewards),
            "std_reward": np.std(total_rewards),
            "throughput": np.mean([m["completed_jobs"] for m in episode_metrics]),
            "avg_wait_time": np.mean([m.get("avg_wait_time", 0) for m in episode_metrics]),
            "cpu_utilization": np.mean([m["cpu_utilization"] for m in episode_metrics]),
            "memory_utilization": np.mean([m["memory_utilization"] for m in episode_metrics])
        }
        
        return metrics
    
    def _collect_metrics(self, simulator: ClusterSimulator) -> Dict:
        """시뮬레이터에서 메트릭 수집"""
        total_jobs = len(simulator.jobs)
        completed_jobs = simulator.metrics["completed_jobs"]
        
        return {
            "throughput": completed_jobs / (simulator.current_time / 3600),  # jobs/hour
            "success_rate": completed_jobs / total_jobs if total_jobs > 0 else 0,
            "avg_wait_time": simulator.metrics["total_wait_time"] / total_jobs if total_jobs > 0 else 0,
            "cpu_utilization": np.mean([h["value"] for h in simulator.metrics["cpu_utilization_history"]]),
            "memory_utilization": np.mean([h["value"] for h in simulator.metrics["memory_utilization_history"]]),
            "oom_rate": simulator.metrics["oom_events"] / total_jobs if total_jobs > 0 else 0
        }
    
    def _plot_learning_curve(self, evaluations: List[Dict], algorithm: str):
        """학습 곡선 플롯"""
        timesteps = [e["timestep"] for e in evaluations]
        mean_rewards = [e["mean_reward"] for e in evaluations]
        std_rewards = [e["std_reward"] for e in evaluations]
        
        plt.figure(figsize=(10, 6))
        plt.plot(timesteps, mean_rewards, label=algorithm)
        plt.fill_between(timesteps, 
                        np.array(mean_rewards) - np.array(std_rewards),
                        np.array(mean_rewards) + np.array(std_rewards),
                        alpha=0.3)
        
        plt.xlabel("Training Steps")
        plt.ylabel("Mean Episode Reward")
        plt.title(f"Learning Curve - {algorithm}")
        plt.legend()
        plt.grid(True)
        
        plt.savefig(os.path.join(self.experiment_dir, f"learning_curve_{algorithm}.png"))
        plt.close()
    
    def _save_evaluation_results(self, results: Dict):
        """평가 결과 저장"""
        # JSON 저장
        with open(os.path.join(self.experiment_dir, "evaluation_results.json"), "w") as f:
            json.dump(results, f, indent=2)
        
        # CSV 저장 (표 형식)
        rows = []
        for scenario, scenario_results in results.items():
            for algorithm, metrics in scenario_results.items():
                row = {"scenario": scenario, "algorithm": algorithm}
                row.update(metrics)
                rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(self.experiment_dir, "evaluation_results.csv"), index=False)
    
    def _visualize_comparison(self, results: Dict):
        """결과 비교 시각화"""
        # 메트릭별 비교
        metrics_to_plot = ["throughput", "avg_wait_time", "cpu_utilization", "success_rate"]
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.flatten()
        
        for i, metric in enumerate(metrics_to_plot):
            ax = axes[i]
            
            # 데이터 준비
            data = []
            for scenario, scenario_results in results.items():
                for algorithm, metrics in scenario_results.items():
                    if metric in metrics:
                        data.append({
                            "Scenario": scenario,
                            "Algorithm": algorithm,
                            metric: metrics[metric]
                        })
            
            df = pd.DataFrame(data)
            
            # 바 플롯
            sns.barplot(data=df, x="Algorithm", y=metric, hue="Scenario", ax=ax)
            ax.set_title(f"{metric.replace('_', ' ').title()}")
            ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.experiment_dir, "comparison_metrics.png"))
        plt.close()
        
        # 히트맵 - 알고리즘별 성능
        self._create_performance_heatmap(results)
    
    def _create_performance_heatmap(self, results: Dict):
        """성능 히트맵 생성"""
        # 정규화된 점수 계산
        scores = {}
        
        for scenario, scenario_results in results.items():
            scores[scenario] = {}
            
            # 각 메트릭에 대해 정규화
            metrics_data = {}
            for algorithm, metrics in scenario_results.items():
                for metric, value in metrics.items():
                    if metric not in metrics_data:
                        metrics_data[metric] = {}
                    metrics_data[metric][algorithm] = value
            
            # 알고리즘별 종합 점수 계산
            for algorithm in scenario_results.keys():
                score = 0
                count = 0
                
                for metric, values in metrics_data.items():
                    if algorithm in values:
                        # 메트릭 정규화 (0-1)
                        min_val = min(values.values())
                        max_val = max(values.values())
                        
                        if max_val - min_val > 0:
                            # 일부 메트릭은 낮을수록 좋음
                            if metric in ["avg_wait_time", "oom_rate"]:
                                normalized = 1 - (values[algorithm] - min_val) / (max_val - min_val)
                            else:
                                normalized = (values[algorithm] - min_val) / (max_val - min_val)
                            
                            score += normalized
                            count += 1
                
                scores[scenario][algorithm] = score / count if count > 0 else 0
        
        # 히트맵 생성
        df = pd.DataFrame(scores).T
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(df, annot=True, fmt=".3f", cmap="YlOrRd", 
                   cbar_kws={'label': 'Normalized Performance Score'})
        plt.title("Algorithm Performance Across Scenarios")
        plt.xlabel("Algorithm")
        plt.ylabel("Scenario")
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.experiment_dir, "performance_heatmap.png"))
        plt.close()