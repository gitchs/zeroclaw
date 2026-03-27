THIS_DIR=`dirname ${BASH_SOURCE[0]}`
PROJECT_ROOT="$THIS_DIR/../"
if [[ -f "${PROJECT_ROOT}/envs.sh" ]]; then
	source "${PROJECT_ROOT}/envs.sh"
fi


RUST_BACKTRACE=full \
RUST_LOG=debug \
${PROJECT_ROOT}/target/debug/zeroclaw agent --temperature=0.1 <<'EOF'
generate sql statement to alter tacnode GUC query history for database=report, set log threshold = 0
EOF
