# Dockerfile for dm-mcp Smithery Deployment
# 基于Python 3.11镜像，支持达梦数据库MCP服务

FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libssl-dev \
    libcrypto++-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件（包括README.md，pyproject.toml需要它）
COPY requirements.txt .
COPY pyproject.toml .
COPY README.md .
COPY config.py .
COPY database.py .
COPY document_generator.py .
COPY src/ ./src/

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 设置Python路径
ENV PYTHONPATH=/app

# 暴露端口（Smithery通过PORT环境变量指定，默认8081）
ENV PORT=8081
EXPOSE ${PORT}

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# 启动命令 - 使用smithery运行FastMCP服务器
CMD ["sh", "-c", "smithery run --host 0.0.0.0 --port ${PORT}"]

