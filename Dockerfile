FROM python:3.11-slim

# 国内构建加速：--build-arg PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/
ARG PIP_INDEX_URL

# mootdx（通达信直连）在 slim 镜像上可纯 wheel 安装，无需编译工具链；
# 若未来依赖需要编译，取消下一行注释：
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
#     && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=30 \
    PIP_RETRIES=5

WORKDIR /app

COPY requirements.txt ./
# --timeout/--retries：镜像站偶发"连上但不回数据"，pip 默认无超时会永久 poll 在
# 一个 socket 上（factor-miner 曾连续卡 54 分钟、构建缓存冻住；本服务也实测卡过 5 分钟）
RUN pip install --timeout 30 --retries 5 -r requirements.txt

COPY . .

RUN useradd --uid 10001 --no-create-home --home-dir /app appuser \
    && mkdir -p /app/.data_cache \
    && chown -R appuser:appuser /app
USER appuser

# 整跑模式；分域模式覆盖 CMD，如：
# CMD ["python3", "mcp_domain_server.py", "--domain", "market", "--host", "0.0.0.0", "--port", "50056"]
EXPOSE 50052

CMD ["python3", "server.py"]
