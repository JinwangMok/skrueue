"""패키지 설정"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="skrueue",
    version="1.0.0",
    author="Jinwang Mok",
    author_email="jinwangmok@gmail.com",
    description="Kubernetes RL-based Scheduler with Kueue integration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/JinwangMok/skrueue",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: System :: Clustering",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    install_requires=[
        "kubernetes>=28.1.0",
        "stable-baselines3>=2.2.1",
        "gymnasium>=0.29.1",
        "numpy>=1.21.0",
        "pandas>=1.5.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "skrueue=main:main",
        ],
    },
)