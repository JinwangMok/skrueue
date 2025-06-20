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

## Project Demo

```
python3 main.py setup

# Only Dataset Augmentation Purpose
python3 main.py generate --duration 86400 --episodes 100 --output data/training_data.csv

# All-in-One Experiments
python3 main.py experiment --training-steps 50000
```
