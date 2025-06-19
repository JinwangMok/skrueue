"""리소스 파싱 유틸리티"""
from typing import Tuple, Dict, Any


class ResourceParser:
    """Kubernetes 리소스 파서"""
    
    @staticmethod
    def parse_cpu(cpu_str: str) -> float:
        """CPU 문자열을 float로 변환 (예: '500m' -> 0.5)"""
        if not cpu_str or cpu_str == '0':
            return 0.0
        cpu_str = str(cpu_str)
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000.0
        return float(cpu_str)
    
    @staticmethod
    def parse_memory(memory_str: str) -> float:
        """메모리 문자열을 GB로 변환 (예: '2Gi' -> 2.0)"""
        if not memory_str or memory_str == '0':
            return 0.0
            
        memory_str = str(memory_str)
        units = {
            'Ki': 1024,
            'Mi': 1024**2,
            'Gi': 1024**3,
            'Ti': 1024**4,
            'K': 1000,
            'M': 1000**2,
            'G': 1000**3,
            'T': 1000**4
        }
        
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                value = float(memory_str[:-len(unit)])
                return (value * multiplier) / (1024**3)  # GB로 변환
                
        try:
            return float(memory_str) / (1024**3)  # 바이트를 GB로
        except ValueError:
            return 0.0
    
    @staticmethod
    def extract_resources(spec: Any) -> Tuple[float, float, float, float]:
        """Job/Pod spec에서 리소스 추출"""
        cpu_request = cpu_limit = 0.0
        memory_request = memory_limit = 0.0
        
        containers = []
        if hasattr(spec, 'template') and hasattr(spec.template, 'spec'):
            containers = spec.template.spec.containers or []
        elif hasattr(spec, 'containers'):
            containers = spec.containers or []
            
        if containers:
            container = containers[0]
            if hasattr(container, 'resources'):
                resources = container.resources
                if hasattr(resources, 'requests'):
                    cpu_request = ResourceParser.parse_cpu(
                        getattr(resources.requests, 'cpu', '0')
                    )
                    memory_request = ResourceParser.parse_memory(
                        getattr(resources.requests, 'memory', '0')
                    )
                if hasattr(resources, 'limits'):
                    cpu_limit = ResourceParser.parse_cpu(
                        getattr(resources.limits, 'cpu', '0')
                    )
                    memory_limit = ResourceParser.parse_memory(
                        getattr(resources.limits, 'memory', '0')
                    )
                    
        return cpu_request, memory_request, cpu_limit, memory_limit
