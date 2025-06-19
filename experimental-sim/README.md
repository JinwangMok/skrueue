# K8s/Workload Simulator for Skrueue

## Project Structure

```
skrueue/experimental-sim (simulator)
├── config/
│ └── simulation_config.py
├── simulator/
│ ├── **init**.py
│ ├── cluster_simulator.py
│ ├── job_generator.py
│ └── resource_manager.py
├── data_augmentation/
│ ├── **init**.py
│ ├── data_generator.py
│ └── synthetic_patterns.py
├── rl_environment/
│ ├── **init**.py
│ ├── simulated_env.py
│ └── reward_calculator.py
├── models/
│ ├── **init**.py
│ └── rl_agents.py
├── experiments/
│ ├── **init**.py
│ ├── experiment_runner.py
│ └── result_analyzer.py
├── utils/
│ ├── **init**.py
│ ├── logger.py
│ └── metrics.py
└── main.py
```
