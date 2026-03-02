#!/usr/bin/env bash
set -euo pipefail

# 配置
HOST=127.0.0.1
PORTS=(7100 7101 7102 7103 7104 7105)
BASE_DIR=$(cd "$(dirname "$0")" && pwd)
SERVER_BIN="${BASE_DIR}/target/debug/server"
LOG_DIR="${BASE_DIR}/cluster-logs"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# 统一输出函数
log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# 检查 server 二进制
if [[ ! -x "${SERVER_BIN}" ]]; then
  log_error "server 二进制不存在：${SERVER_BIN}"
  log_info "请先执行：cargo build --bin server"
  exit 1
fi

# 创建日志目录
mkdir -p "${LOG_DIR}"

# 生成最小化集群配置
generate_config() {
  local port=$1
  local conf_file="${BASE_DIR}/cluster-${port}.conf"
  cat > "${conf_file}" <<EOF
port ${port}
bind 127.0.0.1

# 集群配置
cluster-enabled yes
cluster-config-file "node-${port}.conf"
cluster-node-timeout 5000

# 持久化配置
appendonly yes
appendfilename "appendonly-${port}.aof"
appendfsync everysec
save 3600 1
save 300 100
save 60 10000

# 内存配置
maxmemory 1073741824
maxmemory-policy noeviction
maxmemory-samples 5

# 性能配置
maxclients 1024
rdbcompression yes
rdbchecksum yes
stop-writes-on-bgsave-error yes

# 日志配置
logfile "${LOG_DIR}/server-${port}.log"
EOF
  echo "${conf_file}"
}

# 检查端口是否被占用
check_port() {
  local port=$1
  if lsof -i :"${port}" >/dev/null 2>&1; then
    log_error "端口 ${port} 已被占用"
    return 1
  fi
  return 0
}

# 启动单个节点
start_node() {
  local port=$1
  local conf_file
  conf_file=$(generate_config "${port}")
  
  log_info "启动节点 ${HOST}:${port} ..."
  
  # 清理旧数据
  rm -f "${BASE_DIR}/node-${port}.conf" "${BASE_DIR}/appendonly-${port}.aof" "${BASE_DIR}/dump-${port}.rdb"
  
  # 启动 server
  nohup "${SERVER_BIN}" "${conf_file}" > "${LOG_DIR}/node-${port}.out" 2>&1 &
  local pid=$!
  
  # 等待节点启动
  local retries=30
  while [[ ${retries} -gt 0 ]]; do
    if redis-cli -h "${HOST}" -p "${port}" PING >/dev/null 2>&1; then
      log_info "节点 ${HOST}:${port} 启动成功 (PID: ${pid})"
      return 0
    fi
    sleep 0.5
    ((retries--))
  done
  
  log_error "节点 ${HOST}:${port} 启动超时"
  return 1
}

# 停止所有节点
stop_nodes() {
  log_info "停止所有集群节点..."
  for port in "${PORTS[@]}"; do
    local pids
    pids=$(lsof -t -i :"${port}" 2>/dev/null || true)
    if [[ -n "${pids}" ]]; then
      log_info "停止端口 ${port} 的进程: ${pids}"
      kill -TERM ${pids} 2>/dev/null || true
    fi
  done
  
  # 等待进程结束
  sleep 2
  
  # 强制终止残留进程
  for port in "${PORTS[@]}"; do
    local pids
    pids=$(lsof -t -i :"${port}" 2>/dev/null || true)
    if [[ -n "${pids}" ]]; then
      log_warn "强制终止端口 ${port} 的残留进程: ${pids}"
      kill -KILL ${pids} 2>/dev/null || true
    fi
  done
  
  log_info "所有节点已停止"
}

# 显示状态
show_status() {
  log_info "集群节点状态："
  local all_running=true
  for port in "${PORTS[@]}"; do
    if redis-cli -h "${HOST}" -p "${port}" PING >/dev/null 2>&1; then
      log_info "  ${HOST}:${port} ✓ 运行中"
    else
      log_error "  ${HOST}:${port} ✗ 未运行"
      all_running=false
    fi
  done
  
  if [[ "${all_running}" == true ]]; then
    log_info "所有节点运行正常"
  else
    log_warn "部分节点未运行"
  fi
}

# 主函数
main() {
  local cmd="${1:-start}"
  
  case "${cmd}" in
    start)
      log_info "开始启动集群节点..."
      
      # 检查端口占用
      for port in "${PORTS[@]}"; do
        if ! check_port "${port}"; then
          log_error "端口检查失败，请确保端口未被占用"
          exit 1
        fi
      done
      
      # 启动所有节点
      for port in "${PORTS[@]}"; do
        if ! start_node "${port}"; then
          log_error "节点启动失败，正在清理..."
          stop_nodes
          exit 1
        fi
      done
      
      log_info "所有集群节点启动完成！"
      show_status
      
      log_info "接下来可以执行："
      log_info "  bash ./3m3s.sh  # 创建 3 主 + 3 副本集群"
      log_info "  bash $0 stop    # 停止所有节点"
      log_info "  bash $0 status  # 查看节点状态"
      exit 1
      ;;
      
    stop)
      stop_nodes
      ;;
      
    status)
      show_status
      ;;
      
    restart)
      stop_nodes
      sleep 1
      main start
      ;;
      
    *)
      echo "用法: $0 {start|stop|status|restart}"
      echo ""
      echo "命令说明："
      echo "  start   - 启动 6 个集群节点（7000-7005）"
      echo "  stop    - 停止所有集群节点"
      echo "  status  - 查看节点运行状态"
      echo "  restart - 重启所有节点"
      exit 1
      ;;
  esac
}

# 捕获中断信号进行清理
# trap stop_nodes EXIT

# 运行主函数
main "$@"