"""
达梦数据库连接配置模块
专为Cursor MCP集成设计，支持环境变量配置

Copyright (c) 2025 qyue
Licensed under the MIT License.
See LICENSE file in the project root for full license information.
"""
from pydantic import BaseModel, Field, validator
from typing import List
from enum import Enum
import os


class SecurityMode(str, Enum):
    """安全模式枚举"""
    READONLY = "readonly"           # 只读模式：仅允许SELECT、SHOW等查询操作
    LIMITED_WRITE = "limited_write" # 限制写入模式：允许INSERT、UPDATE，禁止DELETE、DROP等危险操作
    FULL_ACCESS = "full_access"     # 完全访问模式：允许所有操作（谨慎使用）


class DamengConfig(BaseModel):
    """达梦数据库配置"""
    
    # 常量定义
    DEFAULT_DATABASE: str = "DAMENG"
    DEFAULT_CONNECT_TIMEOUT: int = 30
    DEFAULT_QUERY_TIMEOUT: int = 60
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_ALLOWED_SCHEMAS: List[str] = ["*"]
    DEFAULT_MAX_RESULT_ROWS: int = 1000
    
    # 数据库连接参数 - 必须从环境变量获取，无默认值
    host: str = Field(..., description="数据库主机地址")
    port: int = Field(..., description="数据库端口")
    username: str = Field(..., description="数据库用户名")
    password: str = Field(..., description="数据库密码")
    database: str = Field(DEFAULT_DATABASE, description="数据库实例名，默认为DAMENG，支持动态配置")
    
    # 连接控制参数
    connect_timeout: int = Field(DEFAULT_CONNECT_TIMEOUT, description="连接超时时间（秒）")
    query_timeout: int = Field(DEFAULT_QUERY_TIMEOUT, description="查询超时时间（秒）")
    max_retries: int = Field(DEFAULT_MAX_RETRIES, description="最大重试次数")
    
    # 安全控制 - 默认最严格的只读模式
    security_mode: SecurityMode = Field(SecurityMode.READONLY, description="安全模式")
    allowed_schemas: List[str] = Field(DEFAULT_ALLOWED_SCHEMAS, description="允许访问的模式列表，支持'*'表示所有模式，'auto'表示自动发现")
    
    # 高级配置
    enable_query_log: bool = Field(False, description="是否启用查询日志")
    max_result_rows: int = Field(DEFAULT_MAX_RESULT_ROWS, description="最大返回行数")
    
    @validator('security_mode', pre=True)
    def validate_security_mode(cls, v):
        """验证安全模式"""
        if isinstance(v, str):
            try:
                return SecurityMode(v.lower())
            except ValueError:
                raise ValueError(f"无效的安全模式: {v}，支持的模式: {[mode.value for mode in SecurityMode]}")
        return v
    
    @validator('allowed_schemas')
    def validate_schemas(cls, v):
        """验证模式列表"""
        if not v:
            raise ValueError("至少需要指定一个允许访问的模式")
        return v
    
    def get_connection_string(self) -> str:
        """获取数据库连接字符串（达梦数据库格式）"""
        return (
            f"dm://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
            f"?appName=dm-mcp-{self.security_mode.value}"
        )
    
    def is_readonly_mode(self) -> bool:
        """判断是否为只读模式"""
        return self.security_mode == SecurityMode.READONLY
    
    def is_write_allowed(self) -> bool:
        """判断是否允许写入操作"""
        return self.security_mode in [SecurityMode.LIMITED_WRITE, SecurityMode.FULL_ACCESS]
    
    def is_dangerous_operation_allowed(self) -> bool:
        """判断是否允许危险操作（DELETE、DROP等）"""
        return self.security_mode == SecurityMode.FULL_ACCESS
    
    def is_all_schemas_allowed(self) -> bool:
        """判断是否允许访问所有schema"""
        return "*" in self.allowed_schemas
    
    def is_auto_discover_schemas(self) -> bool:
        """判断是否自动发现schema"""
        return "auto" in self.allowed_schemas
    
    def should_validate_schema(self) -> bool:
        """判断是否需要验证schema"""
        return not (self.is_all_schemas_allowed() or self.is_auto_discover_schemas())
    
    @classmethod
    def from_env(cls) -> "DamengConfig":
        """从环境变量加载配置（Cursor MCP专用）"""
        # 必需的环境变量
        required_env_vars = {
            "DAMENG_HOST": "host",
            "DAMENG_PORT": "port", 
            "DAMENG_USERNAME": "username",
            "DAMENG_PASSWORD": "password"
        }
        
        config_data = {}
        missing_vars = []
        
        for env_var, field_name in required_env_vars.items():
            value = os.getenv(env_var)
            if value is None:
                missing_vars.append(env_var)
            else:
                if field_name == "port":
                    config_data[field_name] = int(value)
                else:
                    config_data[field_name] = value
        
        if missing_vars:
            raise ValueError(f"缺少必需的环境变量: {', '.join(missing_vars)}")
        
        # 可选的环境变量
        optional_env_vars = {
            "DAMENG_DATABASE": ("database", str),
            "DAMENG_CONNECT_TIMEOUT": ("connect_timeout", int),
            "DAMENG_QUERY_TIMEOUT": ("query_timeout", int),
            "DAMENG_MAX_RETRIES": ("max_retries", int),
            "DAMENG_SECURITY_MODE": ("security_mode", str),
            "DAMENG_ALLOWED_SCHEMAS": ("allowed_schemas", lambda x: x.split(",")),
            "DAMENG_ENABLE_QUERY_LOG": ("enable_query_log", lambda x: x.lower() == "true"),
            "DAMENG_MAX_RESULT_ROWS": ("max_result_rows", int)
        }
        
        for env_var, (field_name, type_converter) in optional_env_vars.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    config_data[field_name] = type_converter(value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"环境变量 {env_var} 格式错误: {e}")
        
        return cls(**config_data)


def get_config() -> DamengConfig:
    """获取配置实例（专为Cursor MCP设计）"""
    try:
        return DamengConfig.from_env()
    except ValueError as e:
        raise ValueError(f"配置加载失败: {e}. 请检查Cursor MCP配置中的环境变量设置")


# 全局配置实例 - 延迟初始化
_config_instance = None

def get_config_instance() -> DamengConfig:
    """获取全局配置实例"""
    global _config_instance
    if _config_instance is None:
        _config_instance = get_config()
    return _config_instance 