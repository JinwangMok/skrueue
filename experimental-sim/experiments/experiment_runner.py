import torch
import torch.nn as nn
import numpy as np
from stable_baselines3 import DQN, PPO, A2C
from stable_baselines3.common.callbacks import BaseCallback
from stable_baselines3.common.vec_env import DummyVecEnv
from typing import Dict, Any, Optional

class CustomNetwork(nn.Module):
    """커스텀 신경망 아키텍처"""
    
    def __init__(self, input_dim: int, output_dim: int, hidden_sizes: list = [256, 128, 64]):
        super().__init__()
        
        layers = []
        prev_size = input_dim
        
        for hidden_size in hidden_sizes:
            layers.extend([
                nn.Linear(prev_size, hidden_size),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_size),
                nn.Dropout(0.2)
            ])
            prev_size = hidden_size
        
        layers.append(nn.Linear(prev_size, output_dim))
        
        self.network = nn.Sequential(*layers)
    
    def forward(self, x):
        return self.network(x)

class SchedulingAgent:
    """통합 RL 에이전트"""
    
    def __init__(self, env, algorithm: str = "DQN", config: Dict[str, Any] = None):
        self.env = env
        self.algorithm = algorithm
        self.config = config or {}
        
        # 기본 하이퍼파라미터
        self.hyperparameters = {
            "DQN": {
                "learning_rate": 1e-4,
                "buffer_size": 100000,
                "learning_starts": 1000,
                "batch_size": 64,
                "tau": 0.001,
                "gamma": 0.99,
                "train_freq": 4,
                "target_update_interval": 1000,
                "exploration_fraction": 0.1,
                "exploration_initial_eps": 1.0,
                "exploration_final_eps": 0.05
            },
            "PPO": {
                "learning_rate": 3e-4,
                "n_steps": 2048,
                "batch_size": 64,
                "n_epochs": 10,
                "gamma": 0.99,
                "gae_lambda": 0.95,
                "clip_range": 0.2,
                "ent_coef": 0.01
            },
            "A2C": {
                "learning_rate": 7e-4,
                "n_steps": 5,
                "gamma": 0.99,
                "gae_lambda": 1.0,
                "ent_coef": 0.01,
                "vf_coef": 0.5,
                "max_grad_norm": 0.5
            }
        }
        
        # 사용자 설정으로 덮어쓰기
        if algorithm in self.hyperparameters:
            self.hyperparameters[algorithm].update(self.config)
        
        # 모델 초기화
        self.model = self._create_model()
    
    def _create_model(self):
        """알고리즘에 따른 모델 생성"""
        params = self.hyperparameters.get(self.algorithm, {})
        
        # 정책 네트워크 설정
        policy_kwargs = {
            "net_arch": [256, 128, 64],
            "activation_fn": nn.ReLU
        }
        
        if self.algorithm == "DQN":
            return DQN(
                "MlpPolicy",
                self.env,
                policy_kwargs=policy_kwargs,
                verbose=1,
                **params
            )
        elif self.algorithm == "PPO":
            return PPO(
                "MlpPolicy",
                self.env,
                policy_kwargs=policy_kwargs,
                verbose=1,
                **params
            )
        elif self.algorithm == "A2C":
            return A2C(
                "MlpPolicy",
                self.env,
                policy_kwargs=policy_kwargs,
                verbose=1,
                **params
            )
        else:
            raise ValueError(f"Unknown algorithm: {self.algorithm}")
    
    def train(self, total_timesteps: int, callbacks: list = None):
        """모델 학습"""
        self.model.learn(
            total_timesteps=total_timesteps,
            callback=callbacks,
            progress_bar=True
        )
    
    def predict(self, observation: np.ndarray, deterministic: bool = True):
        """행동 예측"""
        action, _states = self.model.predict(observation, deterministic=deterministic)
        return action
    
    def save(self, path: str):
        """모델 저장"""
        self.model.save(path)
    
    def load(self, path: str):
        """모델 로드"""
        if self.algorithm == "DQN":
            self.model = DQN.load(path, env=self.env)
        elif self.algorithm == "PPO":
            self.model = PPO.load(path, env=self.env)
        elif self.algorithm == "A2C":
            self.model = A2C.load(path, env=self.env)

class TrainingCallback(BaseCallback):
    """학습 과정 모니터링 콜백"""
    
    def __init__(self, eval_env, eval_freq: int = 1000, verbose: int = 1):
        super().__init__(verbose)
        self.eval_env = eval_env
        self.eval_freq = eval_freq
        self.evaluations = []
    
    def _on_step(self) -> bool:
        if self.n_calls % self.eval_freq == 0:
            # 평가 실행
            rewards = []
            for _ in range(10):
                obs, _ = self.eval_env.reset()
                done = False
                episode_reward = 0
                
                while not done:
                    action, _ = self.model.predict(obs, deterministic=True)
                    obs, reward, terminated, truncated, _ = self.eval_env.step(action)
                    done = terminated or truncated
                    episode_reward += reward
                
                rewards.append(episode_reward)
            
            mean_reward = np.mean(rewards)
            std_reward = np.std(rewards)
            
            self.evaluations.append({
                "timestep": self.num_timesteps,
                "mean_reward": mean_reward,
                "std_reward": std_reward
            })
            
            if self.verbose > 0:
                print(f"Eval at {self.num_timesteps} steps: {mean_reward:.2f} +/- {std_reward:.2f}")
        
        return True