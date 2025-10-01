#!/usr/bin/env python3
"""
达梦数据库MCP服务 - FastMCP版本
专为Smithery.ai平台设计

Copyright (c) 2025 qyue
Licensed under the MIT License.
"""

import os
import json
import logging
from datetime import datetime
from typing import Any, List, Optional, Dict
from pydantic import BaseModel, Field

from mcp.server.fastmcp import Context, FastMCP

# 导入现有的模块
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from config import get_config_instance, DamengConfig
from database import DamengDatabase
from document_generator import doc_generator

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConfigSchema(BaseModel):
    """会话配置模式"""
    dameng_host: str = Field(..., description="达梦数据库服务器地址")
    dameng_port: int = Field(5236, description="达梦数据库端口")
    dameng_username: str = Field(..., description="数据库用户名")
    dameng_password: str = Field(..., description="数据库密码")
    dameng_database: str = Field("DAMENG", description="数据库名称")
    security_mode: str = Field("readonly", description="安全模式: readonly/limited_write/full_access")
    allowed_schemas: str = Field("*", description="允许访问的模式")
    max_result_rows: int = Field(1000, description="最大返回行数")

def create_server():
    """
    创建并返回FastMCP服务器实例
    
    Smithery Container Runtime模式：
    - 服务器启动时配置可能还未设置（部署阶段）
    - 配置是在用户连接时才提供的（使用阶段）
    - 使用懒加载模式：第一次调用工具时才初始化数据库连接
    """
    
    logger.info("=" * 60)
    logger.info("启动dm-mcp服务器 (Smithery Container Runtime v2.0.2)...")
    logger.info("使用懒加载模式：数据库将在第一次使用时连接")
    logger.info("=" * 60)
    
    server = FastMCP(name="dm-mcp")
    
    # 懒加载数据库实例
    _db_instance = None
    _db_init_error = None
    
    def get_db() -> DamengDatabase:
        """
        获取数据库实例（懒加载）
        只在第一次调用工具时才初始化数据库连接
        """
        nonlocal _db_instance, _db_init_error
        
        # 如果已经初始化过（无论成功或失败），直接返回/抛出
        if _db_instance is not None:
            return _db_instance
        if _db_init_error is not None:
            raise _db_init_error
        
        # 第一次调用，开始初始化
        logger.info("=" * 60)
        logger.info("第一次工具调用，开始初始化数据库连接...")
        
        # Smithery字段名映射到环境变量名
        field_mapping = {
            "dameng_host": "DAMENG_HOST",
            "dameng_port": "DAMENG_PORT",
            "dameng_username": "DAMENG_USERNAME",
            "dameng_password": "DAMENG_PASSWORD",
            "dameng_database": "DAMENG_DATABASE",
            "security_mode": "DAMENG_SECURITY_MODE",
            "allowed_schemas": "DAMENG_ALLOWED_SCHEMAS",
            "max_result_rows": "DAMENG_MAX_RESULT_ROWS"
        }
        
        # 检查并映射环境变量
        mapped_count = 0
        for smithery_field, env_var in field_mapping.items():
            value = os.getenv(smithery_field)
            if value and not os.getenv(env_var):
                os.environ[env_var] = value
                mapped_count += 1
        
        if mapped_count > 0:
            logger.info(f"✅ 映射了 {mapped_count} 个Smithery配置字段到环境变量")
        
        # 显示环境变量状态
        env_vars = {
            "DAMENG_HOST": os.getenv("DAMENG_HOST", "未设置"),
            "DAMENG_PORT": os.getenv("DAMENG_PORT", "未设置"),
            "DAMENG_USERNAME": os.getenv("DAMENG_USERNAME", "未设置"),
            "DAMENG_DATABASE": os.getenv("DAMENG_DATABASE", "未设置"),
            "DAMENG_SECURITY_MODE": os.getenv("DAMENG_SECURITY_MODE", "未设置"),
        }
        logger.info(f"环境变量状态:\n{json.dumps(env_vars, ensure_ascii=False, indent=2)}")
        
        # 创建数据库实例
        try:
            db_config = DamengConfig.from_env()
            _db_instance = DamengDatabase(db_config)
            logger.info("✅ 数据库配置加载成功")
            
            # 测试连接
            if _db_instance.test_connection():
                logger.info("✅ 数据库连接测试成功！")
            else:
                logger.warning("⚠️  数据库连接测试失败")
            
            logger.info("=" * 60)
            return _db_instance
            
        except Exception as e:
            _db_init_error = Exception(
                f"数据库初始化失败: {e}\n\n"
                f"💡 请确保在Smithery配置界面填写了所有必需的参数：\n"
                f"   - 达梦数据库服务器地址 (dameng_host)\n"
                f"   - 数据库用户名 (dameng_username)\n"
                f"   - 数据库密码 (dameng_password)\n"
                f"   - 数据库端口 (dameng_port，默认5236)\n"
                f"   - 数据库名称 (dameng_database，默认DAMENG)"
            )
            logger.error("=" * 60)
            logger.error(str(_db_init_error))
            logger.error("=" * 60)
            raise _db_init_error
    
    def normalize_data(data_list):
        """标准化数据，将大写字段名转换为小写"""
        normalized = []
        for item in data_list:
            normalized_item = {}
            for key, value in item.items():
                normalized_item[key] = value
                normalized_item[key.lower()] = value
            normalized.append(normalized_item)
        return normalized
    
    @server.tool()
    def test_connection() -> str:
        """测试达梦数据库连接"""
        try:
            db = get_db()
            result = db.test_connection()
            return f"达梦数据库连接测试: {'成功' if result else '失败'}"
        except Exception as e:
            return f"连接测试失败: {str(e)}"
    
    @server.tool()
    def get_security_info() -> str:
        """获取当前安全配置信息"""
        try:
            db = get_db()
            security_info = db.get_security_info()
            info_text = "当前安全配置信息:\n\n"
            info_text += f"安全模式: {security_info['security_mode']}\n"
            info_text += f"只读模式: {'是' if security_info['readonly_mode'] else '否'}\n"
            info_text += f"允许写入操作: {'是' if security_info['write_allowed'] else '否'}\n"
            info_text += f"允许危险操作: {'是' if security_info['dangerous_operations_allowed'] else '否'}\n"
            info_text += f"允许访问的模式: {', '.join(security_info['allowed_schemas'])}\n"
            info_text += f"最大返回行数: {security_info['max_result_rows']}\n"
            info_text += f"查询日志: {'启用' if security_info['query_log_enabled'] else '禁用'}\n"
            return info_text
        except Exception as e:
            return f"获取安全信息失败: {str(e)}"
    
    @server.tool()
    def list_tables(schema: str = "SYSDBA") -> str:
        """获取数据库中所有表的列表"""
        try:
            db = get_db()
            tables = db.get_all_tables(schema)
            
            if not tables:
                return f"在模式 '{schema}' 中没有找到任何表"
            
            table_list = "\n".join([f"- {table.get('tablename') or table.get('TABLENAME', 'Unknown')}" for table in tables])
            return f"模式 '{schema}' 中的表列表:\n{table_list}\n\n总计: {len(tables)} 个表"
        except Exception as e:
            return f"获取表列表失败: {str(e)}"
    
    @server.tool()
    def describe_table(table_name: str, schema: str = "SYSDBA") -> str:
        """获取指定表的详细结构信息"""
        try:
            db = get_db()
            structure = db.get_table_structure(table_name, schema)
            indexes = db.get_table_indexes(table_name, schema)
            constraints = db.get_table_constraints(table_name, schema)
            table_comment = db.get_table_comment(table_name, schema)
            
            if not structure:
                return f"表 '{table_name}' 在模式 '{schema}' 中不存在"
            
            result = f"表 '{table_name}' 结构信息:\n\n"
            if table_comment:
                result += f"表注释: {table_comment}\n\n"
            result += "字段列表:\n"
            
            for col in structure:
                column_name = col.get('column_name') or col.get('COLUMN_NAME', 'Unknown')
                data_type = col.get('data_type') or col.get('DATA_TYPE', 'Unknown')
                is_nullable = col.get('is_nullable') or col.get('IS_NULLABLE', 'YES')
                is_primary_key = col.get('is_primary_key') or col.get('IS_PRIMARY_KEY', 'NO')
                column_comment = col.get('column_comment') or col.get('COLUMN_COMMENT', '')
                
                result += f"- {column_name} ({data_type}) "
                if is_nullable == 'NO':
                    result += "NOT NULL "
                if is_primary_key == 'YES':
                    result += "[主键] "
                if column_comment:
                    result += f"-- {column_comment}"
                result += "\n"
            
            if indexes:
                result += f"\n索引 ({len(indexes)} 个):\n"
                for idx in indexes:
                    indexname = idx.get('indexname') or idx.get('INDEXNAME', 'Unknown')
                    is_unique = idx.get('is_unique') or idx.get('IS_UNIQUE', 'NO')
                    result += f"- {indexname} {'[唯一]' if is_unique == 'YES' else ''}\n"
            
            if constraints:
                result += f"\n约束 ({len(constraints)} 个):\n"
                for constraint in constraints:
                    constraint_name = constraint.get('constraint_name') or constraint.get('CONSTRAINT_NAME', 'Unknown')
                    constraint_type = constraint.get('constraint_type') or constraint.get('CONSTRAINT_TYPE', 'Unknown')
                    result += f"- {constraint_name} ({constraint_type})\n"
            
            return result
        except Exception as e:
            return f"获取表结构失败: {str(e)}"
    
    @server.tool()
    def execute_query(sql: str) -> str:
        """执行SQL语句（根据安全模式限制操作类型）"""
        try:
            db = get_db()
            results = db.execute_query(sql)
            
            if not results:
                return "语句执行成功，但没有返回结果"
            
            if sql.upper().strip().startswith(('SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN')):
                result_text = f"查询结果 ({len(results)} 条记录):\n\n"
                
                if len(results) <= 100:
                    result_text += json.dumps(results, ensure_ascii=False, indent=2)
                else:
                    result_text += f"结果集过大，仅显示前100条:\n"
                    result_text += json.dumps(results[:100], ensure_ascii=False, indent=2)
                    result_text += f"\n\n... (还有 {len(results) - 100} 条记录)"
            else:
                result_text = f"操作执行成功:\n\n"
                result_text += json.dumps(results, ensure_ascii=False, indent=2)
            
            return result_text
        except Exception as e:
            return f"SQL执行失败: {str(e)}"
    
    @server.tool()
    def list_schemas() -> str:
        """获取用户有权限访问的所有数据库模式"""
        try:
            db = get_db()
            schemas = db.get_available_schemas()
            
            if not schemas:
                return "没有找到可访问的数据库模式"
            
            schema_list = "\n".join([f"- {schema.get('schemaname') or schema.get('SCHEMANAME', 'Unknown')}" for schema in schemas])
            config_info = f"当前schema访问策略: {db._get_allowed_schemas_display()}\n\n"
            return config_info + f"可访问的数据库模式:\n{schema_list}\n\n总计: {len(schemas)} 个模式"
        except Exception as e:
            return f"获取schema列表失败: {str(e)}"
    
    # 添加健康检查端点
    @server.resource("health://status")
    def get_health() -> str:
        """健康检查端点"""
        return "OK"
    
    return server
