#!/usr/bin/env bash
# Clawith / iDataMate — rsync 代码到远程主机并重启前后端（与 restart.sh --source 配合）
#
# 默认目标（可直接 ./deploy.sh；也可用环境变量或参数覆盖）：
#   DEPLOY_HOST=root@172.16.19.56
#   DEPLOY_PATH=/data/iDataMate
#
# 覆盖示例：
#   DEPLOY_HOST=root@other ./deploy.sh
#   ./deploy.sh root@other /opt/clawith
#
# 选项：
#   --dry-run      只打印 rsync 将做什么，不真正传输
#   --no-restart   只同步，不 SSH 执行 restart.sh
#   --restart-only 不同步，只 SSH 重启（用于服务器上已手动更新代码时）
#   --sync-env     同时同步本机 .env（默认不同步，避免覆盖服务器密钥/数据库地址）
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# 默认部署目标（改这里即可换默认机；也可用 DEPLOY_HOST / DEPLOY_PATH 临时覆盖）
DEFAULT_DEPLOY_HOST="root@172.16.19.56"
DEFAULT_DEPLOY_PATH="/data/iDataMate"

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

DRY_RUN=false
NO_RESTART=false
RESTART_ONLY=false
SYNC_ENV=false
RSYNC_EXTRA=()

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)       DRY_RUN=true; shift ;;
    --no-restart)    NO_RESTART=true; shift ;;
    --restart-only)  RESTART_ONLY=true; shift ;;
    --sync-env)      SYNC_ENV=true; shift ;;
    -h|--help)
      cat <<'EOF'
Clawith deploy — rsync to remote and run ./restart.sh --source

  ./deploy.sh                          # 使用脚本内默认 HOST + PATH
  DEPLOY_HOST=root@x ./deploy.sh       # 覆盖默认主机
  ./deploy.sh root@x [/remote/path]   # 参数覆盖（优先于默认，次于 DEPLOY_* 环境变量）

Options:
  --dry-run       rsync dry run only
  --no-restart    sync only, no SSH restart
  --restart-only  restart only, no rsync
  --sync-env      include .env (default: excluded)
EOF
      exit 0
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

# 优先级：DEPLOY_* 环境变量 > 命令行位置参数 > 脚本内 DEFAULT_*
HOST="${DEPLOY_HOST:-${POSITIONAL[0]:-$DEFAULT_DEPLOY_HOST}}"
REMOTE_PATH="${DEPLOY_PATH:-${POSITIONAL[1]:-$DEFAULT_DEPLOY_PATH}}"

# 去掉末尾斜杠，统一成 REMOTE_PATH 无尾斜杠
REMOTE_PATH="${REMOTE_PATH%/}"

if [[ "$DRY_RUN" == true ]]; then
  RSYNC_EXTRA+=(-n)
fi

EXCLUDES=(
  --exclude '.git'
  --exclude 'node_modules'
  --exclude '.venv'
  --exclude '__pycache__'
  --exclude '*.pyc'
  --exclude '.pytest_cache'
  --exclude '.mypy_cache'
  --exclude 'backend/data'
  --exclude '.data'
  --exclude 'frontend/dist'
  --exclude '.DS_Store'
  --exclude 'backend/clawith_backend.egg-info'
  --exclude '.cursor'
)

if [[ "$SYNC_ENV" != true ]]; then
  EXCLUDES+=(--exclude '.env')
fi

REMOTE_TARGET="${HOST}:${REMOTE_PATH}/"

if [[ "$RESTART_ONLY" != true ]]; then
  echo -e "${CYAN}▶ rsync → ${REMOTE_TARGET}${NC}"
  rsync -avz "${RSYNC_EXTRA[@]}" "${EXCLUDES[@]}" ./ "$REMOTE_TARGET"
  echo -e "${GREEN}✓ 同步完成${NC}"
else
  echo -e "${YELLOW}▶ 跳过 rsync（--restart-only）${NC}"
fi

if [[ "$NO_RESTART" == true ]]; then
  echo -e "${YELLOW}▶ 跳过重启（--no-restart）${NC}"
  exit 0
fi

echo -e "${CYAN}▶ 远程重启: ${HOST} ${REMOTE_PATH}/restart.sh --source${NC}"
ssh -o BatchMode=yes "$HOST" "cd $(printf '%q' "$REMOTE_PATH") && ./restart.sh --source"

echo -e "${GREEN}✓ 部署完成${NC}"
echo -e "  前端代理: ${CYAN}http://${HOST#*@}/3008${NC}  后端: ${CYAN}http://${HOST#*@}/8008${NC}"
