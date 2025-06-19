"""SKRueue RL 에이전트"""
import os
import numpy as np
from typing import Dict, Optional, Any

from stable_baselines3 import DQN, PPO, A2C
from stable_baselines3.common.env_checker import check_env
from stable_baselines3.common.callbacks import BaseCallback, EvalCallback

from .environment import SKRueueEnvironment
from utils import get_logger
from config.settings import get_config


class TrainingCallback(BaseCallback):
    """훈련 진행 상황 추적"""
    
    def __init__(self, verbose=0):
        super().__init__(verbose)
        self.episode_rewards = []
        self.episode_lengths = []
        
    def _on_step(self) -> bool:
        return True
    
    def _on_rollout_end(self) -> None:
        """에피소드 종료 시 호출"""
        if len(self.model.ep_info_buffer) > 0:
            last_info = self.model.ep_info_buffer[-1]
            if 'r' in last_info:
                self.episode_rewards.append(last_info['r'])
            if 'l' in last_info:
                self.episode_lengths.append(last_info['l'])


class RLAgent:
    """SKRueue RL 에이전트"""
    
    def __init__(self, env: SKRueueEnvironment, algorithm: str = None):
        self.config = get_config()
        self.env = env
        self.algorithm = algorithm or self.config.rl.algorithm
        self.logger = get_logger('RLAgent')
        self.model = None
        self.callback = TrainingCallback()
        
        # 환경 검증
        try:
            check_env(env)
            self.logger.info("Environment validation passed")
        except Exception as e:
            self.logger.warning(f"Environment validation warning: {e}")
        
        # 모델 초기화
        self._initialize_model()
    
    def _initialize_model(self):
        """모델 초기화"""
        try:
            state_dim = self.env.observation_space.shape[0]
            
            # 네트워크 아키텍처
            policy_kwargs = {
                'net_arch': self._get_network_architecture(state_dim)
            }
            
            if self.algorithm == 'DQN':
                self.model = DQN(
                    'MlpPolicy',
                    self.env,
                    learning_rate=self.config.rl.learning_rate,
                    buffer_size=self.config.rl.buffer_size,
                    learning_starts=1000,
                    target_update_interval=500,
                    train_freq=4,
                    exploration_initial_eps=1.0,
                    exploration_final_eps=0.05,
                    exploration_fraction=0.5,
                    policy_kwargs=policy_kwargs,
                    verbose=1,
                    tensorboard_log=f"./tensorboard/{self.algorithm}/"
                )
                
            elif self.algorithm == 'PPO':
                self.model = PPO(
                    'MlpPolicy',
                    self.env,
                    learning_rate=3e-4,
                    n_steps=1024,
                    batch_size=64,
                    n_epochs=10,
                    gamma=0.99,
                    gae_lambda=0.95,
                    clip_range=0.2,
                    policy_kwargs=policy_kwargs,
                    verbose=1,
                    tensorboard_log=f"./tensorboard/{self.algorithm}/"
                )
                
            elif self.algorithm == 'A2C':
                self.model = A2C(
                    'MlpPolicy',
                    self.env,
                    learning_rate=7e-4,
                    n_steps=8,
                    gamma=0.99,
                    gae_lambda=1.0,
                    ent_coef=0.01,
                    vf_coef=0.25,
                    policy_kwargs=policy_kwargs,
                    verbose=1,
                    tensorboard_log=f"./tensorboard/{self.algorithm}/"
                )
                
            else:
                raise ValueError(f"Unknown algorithm: {self.algorithm}")
            
            self.logger.info(f"{self.algorithm} model initialized (state_dim={state_dim})")
            
        except Exception as e:
            self.logger.error(f"Model initialization failed: {e}")
            raise
    
    def _get_network_architecture(self, state_dim: int) -> list:
        """상태 차원에 따른 네트워크 구조"""
        if state_dim < 50:
            return [128, 128]
        elif state_dim < 100:
            return [256, 256, 128]
        else:
            return [512, 256, 128]
    
    def train(self, total_timesteps: int = None, callback: BaseCallback = None):
        """모델 훈련"""
        timesteps = total_timesteps or self.config.rl.training_steps
        
        # 콜백 설정
        callbacks = [self.callback]
        if callback:
            callbacks.append(callback)
        
        self.logger.info(f"Starting {self.algorithm} training for {timesteps} timesteps")
        
        try:
            self.model.learn(
                total_timesteps=timesteps,
                callback=callbacks,
                progress_bar=True
            )
            self.logger.info("Training completed successfully")
            
            # 훈련 통계
            if self.callback.episode_rewards:
                avg_reward = np.mean(self.callback.episode_rewards[-100:])
                self.logger.info(f"Average reward (last 100 episodes): {avg_reward:.3f}")
                
        except Exception as e:
            self.logger.error(f"Training failed: {e}")
            raise
    
    def save(self, path: str = None):
        """모델 저장"""
        if path is None:
            path = os.path.join(
                self.config.rl.model_save_dir,
                f"skrueue_{self.algorithm.lower()}_model"
            )
        
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.model.save(path)
            self.logger.info(f"Model saved to: {path}")
            
            # 훈련 통계도 저장
            import pickle
            stats_path = path + "_stats.pkl"
            with open(stats_path, 'wb') as f:
                pickle.dump({
                    'episode_rewards': self.callback.episode_rewards,
                    'episode_lengths': self.callback.episode_lengths
                }, f)
                
        except Exception as e:
            self.logger.error(f"Failed to save model: {e}")
    
    def load(self, path: str):
        """모델 로드"""
        try:
            model_class = {'DQN': DQN, 'PPO': PPO, 'A2C': A2C}[self.algorithm]
            self.model = model_class.load(path, env=self.env)
            self.logger.info(f"Model loaded from: {path}")
            
            # 통계 로드 시도
            stats_path = path + "_stats.pkl"
            if os.path.exists(stats_path):
                import pickle
                with open(stats_path, 'rb') as f:
                    stats = pickle.load(f)
                    self.callback.episode_rewards = stats.get('episode_rewards', [])
                    self.callback.episode_lengths = stats.get('episode_lengths', [])
                    
        except Exception as e:
            self.logger.error(f"Failed to load model: {e}")
            raise
    
    def predict(self, observation: np.ndarray, deterministic: bool = True) -> int:
        """액션 예측"""
        try:
            action, _ = self.model.predict(observation, deterministic=deterministic)
            return int(action)
        except Exception as e:
            self.logger.error(f"Prediction failed: {e}")
            return 0  # 기본 액션
    
    def evaluate(self, num_episodes: int = 10) -> Dict[str, float]:
        """모델 평가"""
        episode_rewards = []
        episode_lengths = []
        success_rates = []
        
        for episode in range(num_episodes):
            obs, info = self.env.reset()
            total_reward = 0
            steps = 0
            successful_actions = 0
            total_actions = 0
            
            terminated = truncated = False
            while not (terminated or truncated):
                action = self.predict(obs)
                obs, reward, terminated, truncated, info = self.env.step(action)
                
                total_reward += reward
                steps += 1
                
                if info.get('action_result', {}).get('action_type') == 'admit':
                    total_actions += 1
                    if info['action_result'].get('success'):
                        successful_actions += 1
            
            episode_rewards.append(total_reward)
            episode_lengths.append(steps)
            if total_actions > 0:
                success_rates.append(successful_actions / total_actions)
        
        return {
            'mean_reward': np.mean(episode_rewards),
            'std_reward': np.std(episode_rewards),
            'mean_episode_length': np.mean(episode_lengths),
            'std_episode_length': np.std(episode_lengths),
            'mean_success_rate': np.mean(success_rates) if success_rates else 0.0,
            'total_episodes': num_episodes
        }