"""
监控指标收集器
"""
import time
import json
import threading
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque
from loguru import logger
import requests

from ..config.config import MonitorConfig


class MetricsCollector:
    """监控指标收集器"""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self._lock = threading.Lock()
        
        # 同步计数器
        self.sync_counters = defaultdict(lambda: defaultdict(int))
        
        # 错误记录
        self.errors = deque(maxlen=1000)
        
        # 性能指标
        self.performance_metrics = {
            'sync_duration': deque(maxlen=1000),
            'queue_size': deque(maxlen=100),
            'memory_usage': deque(maxlen=100)
        }
        
        # 队列统计
        self.queue_stats = {}
        
        # 同步统计
        self.sync_stats = {}
        
        # 启动时间
        self.start_time = datetime.now()
    
    def record_sync(self, direction: str, status: str) -> None:
        """记录同步操作"""
        with self._lock:
            self.sync_counters[direction][status] += 1
            self.sync_counters[direction]['total'] += 1
    
    def record_sync_duration(self, direction: str, duration_seconds: float) -> None:
        """记录同步耗时"""
        with self._lock:
            self.performance_metrics['sync_duration'].append({
                'direction': direction,
                'duration': duration_seconds,
                'timestamp': datetime.now()
            })
    
    def record_error(self, error_type: str, error_message: str) -> None:
        """记录错误"""
        with self._lock:
            self.errors.append({
                'type': error_type,
                'message': error_message,
                'timestamp': datetime.now()
            })
    
    def update_queue_stats(self, stats: Dict[str, Any]) -> None:
        """更新队列统计"""
        with self._lock:
            self.queue_stats = stats
            self.performance_metrics['queue_size'].append({
                'size': stats.get('total', 0),
                'timestamp': datetime.now()
            })
    
    def update_sync_stats(self, stats: Dict[str, Any]) -> None:
        """更新同步统计"""
        with self._lock:
            self.sync_stats = stats
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取所有指标"""
        with self._lock:
            # 计算运行时间
            uptime = (datetime.now() - self.start_time).total_seconds()
            
            # 计算成功率
            success_rates = {}
            for direction, counters in self.sync_counters.items():
                total = counters.get('total', 0)
                success = counters.get('success', 0)
                if total > 0:
                    success_rates[direction] = round(success / total * 100, 2)
                else:
                    success_rates[direction] = 0
            
            # 计算平均同步耗时
            avg_durations = {}
            recent_durations = list(self.performance_metrics['sync_duration'])[-100:]
            if recent_durations:
                for direction in ['feishu_to_db', 'db_to_feishu']:
                    dir_durations = [
                        d['duration'] for d in recent_durations 
                        if d['direction'] == direction
                    ]
                    if dir_durations:
                        avg_durations[direction] = round(
                            sum(dir_durations) / len(dir_durations), 3
                        )
            
            # 获取最近的错误
            recent_errors = list(self.errors)[-10:]
            
            return {
                'uptime_seconds': uptime,
                'sync_counters': dict(self.sync_counters),
                'success_rates': success_rates,
                'average_sync_duration': avg_durations,
                'queue_stats': self.queue_stats,
                'sync_stats': self.sync_stats,
                'recent_errors': recent_errors,
                'timestamp': datetime.now().isoformat()
            }
    
    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        metrics = self.get_metrics()
        
        # 判断健康状态
        health_status = "healthy"
        issues = []
        
        # 检查成功率
        for direction, rate in metrics['success_rates'].items():
            if rate < 90:
                health_status = "degraded"
                issues.append(f"Low success rate for {direction}: {rate}%")
        
        # 检查队列积压
        queue_pending = self.queue_stats.get('by_status', {}).get('pending', 0)
        if queue_pending > 1000:
            health_status = "unhealthy"
            issues.append(f"High queue backlog: {queue_pending} pending items")
        elif queue_pending > 500:
            if health_status == "healthy":
                health_status = "degraded"
            issues.append(f"Moderate queue backlog: {queue_pending} pending items")
        
        # 检查错误率
        recent_errors = len(list(self.errors)[-100:])
        if recent_errors > 50:
            health_status = "unhealthy"
            issues.append(f"High error rate: {recent_errors} errors in recent 100 operations")
        
        return {
            'status': health_status,
            'issues': issues,
            'metrics_summary': {
                'uptime_hours': round(metrics['uptime_seconds'] / 3600, 2),
                'success_rates': metrics['success_rates'],
                'queue_pending': queue_pending,
                'recent_errors': recent_errors
            }
        }
    
    def send_alert(self, alert_type: str, message: str, details: Optional[Dict] = None) -> None:
        """发送告警"""
        if not self.config.alert_webhook:
            return
        
        alert_data = {
            'type': alert_type,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now().isoformat(),
            'service': 'feishu_db_sync'
        }
        
        try:
            # 发送到 webhook（假设是飞书机器人）
            if 'open.feishu.cn' in self.config.alert_webhook:
                # 飞书机器人格式
                payload = {
                    "msg_type": "text",
                    "content": {
                        "text": f"【{alert_type}】{message}\n{json.dumps(details, ensure_ascii=False, indent=2)}"
                    }
                }
            else:
                # 通用 webhook 格式
                payload = alert_data
            
            response = requests.post(
                self.config.alert_webhook,
                json=payload,
                timeout=5
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to send alert: {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending alert: {e}")
    
    def check_and_alert(self) -> None:
        """检查指标并发送告警"""
        health = self.get_health_status()
        
        if health['status'] == 'unhealthy':
            self.send_alert(
                'CRITICAL',
                'Sync service is unhealthy',
                health
            )
        elif health['status'] == 'degraded':
            self.send_alert(
                'WARNING',
                'Sync service is degraded',
                health
            )
    
    def export_metrics(self, format: str = 'json') -> str:
        """导出指标"""
        metrics = self.get_metrics()
        
        if format == 'json':
            return json.dumps(metrics, ensure_ascii=False, indent=2, default=str)
        elif format == 'prometheus':
            # Prometheus 格式
            lines = []
            
            # 运行时间
            lines.append(f'# HELP sync_uptime_seconds Sync service uptime in seconds')
            lines.append(f'# TYPE sync_uptime_seconds gauge')
            lines.append(f'sync_uptime_seconds {metrics["uptime_seconds"]}')
            
            # 同步计数
            for direction, counters in metrics['sync_counters'].items():
                for status, count in counters.items():
                    lines.append(f'sync_total{{direction="{direction}",status="{status}"}} {count}')
            
            # 成功率
            for direction, rate in metrics['success_rates'].items():
                lines.append(f'sync_success_rate{{direction="{direction}"}} {rate}')
            
            # 队列大小
            queue_total = metrics['queue_stats'].get('total', 0)
            lines.append(f'queue_size_total {queue_total}')
            
            return '\n'.join(lines)
        else:
            raise ValueError(f"Unsupported format: {format}")