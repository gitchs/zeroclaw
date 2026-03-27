THIS_DIR=`dirname ${BASH_SOURCE[0]}`
PROJECT_ROOT="$THIS_DIR/../"
if [[ -f "${PROJECT_ROOT}/envs.sh" ]]; then
	source "${PROJECT_ROOT}/envs.sh"
fi


RUST_BACKTRACE=full \
RUST_LOG=debug \
${PROJECT_ROOT}/target/debug/zeroclaw agent --temperature=0.1 <<'EOF'
1. list all datasync running jobs in datacloud='dc1rqqjhex'
2. iterate all datasync jobs from step 1, find which one contains sink table 'act_goods'
3. break the loop when we find the target datasync job
4. only output target job info in markdown format.
EOF
