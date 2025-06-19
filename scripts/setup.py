"""환경 설정 스크립트"""
import os
import subprocess
import yaml
from typing import List, Optional

from utils import get_logger
from config.settings import get_config, SKRueueConfig


class EnvironmentSetup:
    """환경 설정 자동화"""
    
    def __init__(self):
        self.logger = get_logger('EnvironmentSetup')
        self.config = get_config()
        
    def setup_all(self):
        """전체 환경 설정"""
        self.logger.info("Starting SKRueue environment setup...")
        
        # 1. Kubernetes 연결 확인
        self._check_kubernetes()
        
        # 2. 네임스페이스 생성
        self._create_namespaces()
        
        # 3. RBAC 설정
        self._setup_rbac()
        
        # 4. Kueue 설치 확인
        self._check_kueue()
        
        # 5. 디렉토리 구조 생성
        self._create_directories()
        
        # 6. 설정 파일 생성
        self._create_config_file()
        
        self.logger.info("✅ Environment setup completed!")
    
    def _check_kubernetes(self):
        """Kubernetes 연결 확인"""
        try:
            result = subprocess.run(
                ['kubectl', 'cluster-info'],
                capture_output=True,
                text=True,
                check=True
            )
            self.logger.info("✅ Kubernetes cluster connected")
            
            # 노드 확인
            result = subprocess.run(
                ['kubectl', 'get', 'nodes'],
                capture_output=True,
                text=True
            )
            self.logger.info(f"Cluster nodes:\n{result.stdout}")
            
        except subprocess.CalledProcessError as e:
            self.logger.error("❌ Cannot connect to Kubernetes cluster")
            self.logger.error(f"Error: {e.stderr}")
            raise
    
    def _create_namespaces(self):
        """네임스페이스 생성"""
        for namespace in self.config.kueue.namespaces:
            try:
                subprocess.run(
                    ['kubectl', 'create', 'namespace', namespace],
                    capture_output=True,
                    check=False
                )
                self.logger.info(f"✅ Namespace created/verified: {namespace}")
            except Exception as e:
                self.logger.warning(f"Namespace {namespace} may already exist: {e}")
    
    def _setup_rbac(self):
        """RBAC 설정"""
        rbac_yaml = """
apiVersion: v1
kind: ServiceAccount
metadata:
  name: skrueue-agent
  namespace: {namespace}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: skrueue-controller
rules:
- apiGroups: [""]
  resources: ["nodes", "pods", "events", "services", "configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["apps"]
  resources: ["deployments", "replicasets"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["kueue.x-k8s.io"]
  resources: ["workloads", "localqueues", "clusterqueues"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["metrics.k8s.io"]
  resources: ["nodes", "pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: skrueue-controller-binding
subjects:
- kind: ServiceAccount
  name: skrueue-agent
  namespace: {namespace}
roleRef:
  kind: ClusterRole
  name: skrueue-controller
  apiGroup: rbac.authorization.k8s.io
"""
        
        for namespace in self.config.kueue.namespaces:
            # YAML 적용
            yaml_content = rbac_yaml.format(namespace=namespace)
            
            process = subprocess.Popen(
                ['kubectl', 'apply', '-f', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=yaml_content)
            
            if process.returncode == 0:
                self.logger.info(f"✅ RBAC configured for namespace: {namespace}")
            else:
                self.logger.error(f"❌ RBAC setup failed: {stderr}")
    
    def _check_kueue(self):
        """Kueue 설치 확인"""
        try:
            result = subprocess.run(
                ['kubectl', 'get', 'crd', 'clusterqueues.kueue.x-k8s.io'],
                capture_output=True,
                check=False
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Kueue is installed")
            else:
                self.logger.warning("⚠️ Kueue not found. Please install Kueue:")
                self.logger.warning("kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.6.2/manifests.yaml")
                
        except Exception as e:
            self.logger.error(f"Error checking Kueue: {e}")
    
    def _create_directories(self):
        """디렉토리 구조 생성"""
        directories = [
            self.config.rl.model_save_dir,
            self.config.test.output_dir,
            "logs",
            "data",
            "tensorboard",
            os.path.dirname(self.config.data.db_path)
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
        self.logger.info("✅ Directory structure created")
    
    def _create_config_file(self):
        """설정 파일 생성"""
        config_path = "config/skrueue.yaml"
        
        if not os.path.exists(config_path):
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            self.config.to_yaml(config_path)
            self.logger.info(f"✅ Configuration file created: {config_path}")
        else:
            self.logger.info(f"Configuration file already exists: {config_path}")
