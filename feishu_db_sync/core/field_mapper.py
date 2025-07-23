"""
字段映射器
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from loguru import logger


class FieldMapper:
    """字段映射器，处理飞书和数据库之间的字段转换"""
    
    def __init__(self, field_mapping: Dict[str, Dict[str, str]]):
        """
        field_mapping格式:
        {
            "table_name": {
                "飞书字段": "数据库字段",
                ...
            }
        }
        """
        self.field_mapping = field_mapping
        
    def feishu_to_db(self, table: str, feishu_record: Dict[str, Any]) -> Dict[str, Any]:
        """飞书记录转换为数据库记录"""
        if table not in self.field_mapping:
            # 如果没有配置映射，直接返回原始数据
            return feishu_record
        
        mapping = self.field_mapping[table]
        db_record = {}
        
        for feishu_field, value in feishu_record.items():
            # 跳过系统字段
            if feishu_field in ['id', 'created_at', 'updated_at']:
                continue
            
            # 使用映射或原字段名
            db_field = mapping.get(feishu_field, feishu_field)
            
            # 转换值
            db_record[db_field] = self._convert_feishu_value(value, feishu_field)
        
        # 保存飞书ID用于映射
        if 'id' in feishu_record:
            db_record['feishu_id'] = feishu_record['id']
        
        return db_record
    
    def db_to_feishu(self, table: str, db_record: Dict[str, Any]) -> Dict[str, Any]:
        """数据库记录转换为飞书记录"""
        if table not in self.field_mapping:
            # 如果没有配置映射，直接返回原始数据
            # 但需要移除数据库特有字段
            return self._clean_db_record(db_record)
        
        mapping = self.field_mapping[table]
        # 反转映射关系
        reverse_mapping = {v: k for k, v in mapping.items()}
        
        feishu_record = {}
        
        for db_field, value in db_record.items():
            # 跳过数据库系统字段
            if db_field in ['id', 'created_at', 'updated_at', 'feishu_id', '_sync_source']:
                continue
            
            # 使用反向映射或原字段名
            feishu_field = reverse_mapping.get(db_field, db_field)
            
            # 转换值
            feishu_record[feishu_field] = self._convert_db_value(value, db_field)
        
        return feishu_record
    
    def _convert_feishu_value(self, value: Any, field_name: str) -> Any:
        """转换飞书字段值为数据库格式"""
        if value is None:
            return None
        
        # 处理飞书特殊字段类型
        if isinstance(value, dict):
            # 人员字段
            if 'id' in value and 'name' in value:
                return value.get('id')  # 只保存ID
            # 其他复杂类型，转为JSON字符串
            import json
            return json.dumps(value, ensure_ascii=False)
        
        # 处理数组类型（多选字段）
        if isinstance(value, list):
            # 如果是字符串数组，用逗号连接
            if all(isinstance(item, str) for item in value):
                return ','.join(value)
            # 否则转为JSON
            import json
            return json.dumps(value, ensure_ascii=False)
        
        # 处理日期时间
        if isinstance(value, str) and self._is_datetime_string(value):
            try:
                # 尝试解析为datetime对象
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                return value
        
        return value
    
    def _convert_db_value(self, value: Any, field_name: str) -> Any:
        """转换数据库字段值为飞书格式"""
        if value is None:
            return None
        
        # 处理datetime对象
        if isinstance(value, datetime):
            return value.isoformat()
        
        # 处理可能的JSON字符串
        if isinstance(value, str):
            # 尝试解析JSON
            if value.startswith(('{', '[')):
                try:
                    import json
                    return json.loads(value)
                except:
                    pass
            
            # 检查是否是逗号分隔的值（多选字段）
            if ',' in value and not value.startswith('"'):
                return value.split(',')
        
        return value
    
    def _clean_db_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """清理数据库记录，移除系统字段"""
        exclude_fields = [
            'id', 'created_at', 'updated_at', 
            'feishu_id', '_sync_source', '_sync_hash'
        ]
        
        return {
            k: v for k, v in record.items() 
            if k not in exclude_fields
        }
    
    def _is_datetime_string(self, value: str) -> bool:
        """检查是否是日期时间字符串"""
        datetime_patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO格式
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # 常规格式
        ]
        
        import re
        for pattern in datetime_patterns:
            if re.match(pattern, value):
                return True
        return False
    
    def get_mapping_for_table(self, table: str) -> Dict[str, str]:
        """获取指定表的字段映射"""
        return self.field_mapping.get(table, {})
    
    def add_mapping(self, table: str, feishu_field: str, db_field: str) -> None:
        """添加字段映射"""
        if table not in self.field_mapping:
            self.field_mapping[table] = {}
        
        self.field_mapping[table][feishu_field] = db_field
        logger.info(f"Added field mapping for {table}: {feishu_field} -> {db_field}")
    
    def remove_mapping(self, table: str, feishu_field: str) -> None:
        """移除字段映射"""
        if table in self.field_mapping and feishu_field in self.field_mapping[table]:
            del self.field_mapping[table][feishu_field]
            logger.info(f"Removed field mapping for {table}: {feishu_field}")
    
    def validate_mapping(self, table: str, 
                        feishu_fields: List[str], 
                        db_fields: List[str]) -> List[str]:
        """验证字段映射配置"""
        errors = []
        
        if table not in self.field_mapping:
            return errors
        
        mapping = self.field_mapping[table]
        
        # 检查飞书字段是否存在
        for feishu_field in mapping.keys():
            if feishu_field not in feishu_fields:
                errors.append(f"飞书字段 '{feishu_field}' 不存在于表 '{table}' 中")
        
        # 检查数据库字段是否存在
        for db_field in mapping.values():
            if db_field not in db_fields:
                errors.append(f"数据库字段 '{db_field}' 不存在于表 '{table}' 中")
        
        return errors