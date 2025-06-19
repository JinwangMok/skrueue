"""Kubernetes 관련 유틸리티"""
import subprocess
import json
from typing import Dict, List, Optional, Any
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import yaml


class K8sClient:
    """Kubernetes 클라이언트 싱글톤"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
            
        self.core_v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.custom_api = client.CustomObjectsApi()
        self._initialized = True
    
    def apply_yaml(self, yaml_content: str) -> bool:
        """YAML 적용"""
        try:
            process = subprocess.Popen(
                ['kubectl', 'apply', '-f', '-'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=yaml_content)
            return process.returncode == 0
        except Exception:
            return False
    
    def get_resource(self, api_version: str, kind: str, name: str, 
                    namespace: str = None) -> Optional[Dict]:
        """리소스 조회"""
        try:
            if kind.lower() == 'job':
                if namespace:
                    return self.batch_v1.read_namespaced_job(name, namespace)
                else:
                    return self.batch_v1.list_job_for_all_namespaces()
            elif kind.lower() == 'pod':
                if namespace:
                    return self.core_v1.read_namespaced_pod(name, namespace)
                else:
                    return self.core_v1.list_pod_for_all_namespaces()
            # 추가 리소스 타입들...
        except ApiException:
            return None