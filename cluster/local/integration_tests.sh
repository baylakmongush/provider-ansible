#!/usr/bin/env bash
set -aeuo pipefail

# setting up colors
BLU='\033[0;104m'
YLW='\033[0;33m'
GRN='\033[0;32m'
RED='\033[0;31m'
NOC='\033[0m' # No Color

echo_info(){
    printf "\n${BLU}%s${NOC}\n" "$1"
}
echo_step(){
    printf "\n${BLU}>>>>>>> %s${NOC}\n" "$1"
}
echo_step_completed(){
    printf "${GRN} [✔] %s${NOC}\n" "$1"
}
echo_fail(){
    printf "${RED}FAIL: %s${NOC}\n" "$1"
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR="${SCRIPT_DIR}/../.."
KUBECTL="${KUBECTL:-kubectl}"

echo_info "Running integration tests"

# Setup
echo_step "Running setup"
KUBECTL="${KUBECTL}" "${ROOT_DIR}/cluster/test/setup.sh"
echo_step_completed "Setup done"

# Wait for cluster-scoped AnsibleRun to become Available
echo_step "Waiting for cluster-scoped AnsibleRun to become Available"
${KUBECTL} wait ansiblerun test-cluster \
    --for=condition=Ready \
    --timeout=3m || {
    echo_fail "cluster AnsibleRun did not become Ready"
    ${KUBECTL} describe ansiblerun test-cluster
    KUBECTL="${KUBECTL}" "${ROOT_DIR}/cluster/test/teardown.sh"
    exit 1
}
echo_step_completed "cluster AnsibleRun is Ready"

# Wait for namespaced AnsibleRun to become Available
echo_step "Waiting for namespaced AnsibleRun to become Available"
${KUBECTL} wait ansiblerun.ansible.m.crossplane.io test-ns -n demo \
    --for=condition=Ready \
    --timeout=3m || {
    echo_fail "namespaced AnsibleRun did not become Ready"
    ${KUBECTL} describe ansiblerun.ansible.m.crossplane.io test-ns -n demo
    KUBECTL="${KUBECTL}" "${ROOT_DIR}/cluster/test/teardown.sh"
    exit 1
}
echo_step_completed "namespaced AnsibleRun is Ready"

# Verify cluster AnsibleRun API group
echo_step "Verifying cluster AnsibleRun apiVersion"
CLUSTER_API=$(${KUBECTL} get ansiblerun test-cluster -o jsonpath='{.apiVersion}')
if [[ "$CLUSTER_API" != "ansible.crossplane.io/v1alpha1" ]]; then
    echo_fail "expected ansible.crossplane.io/v1alpha1, got $CLUSTER_API"
    KUBECTL="${KUBECTL}" "${ROOT_DIR}/cluster/test/teardown.sh"
    exit 1
fi
echo_step_completed "cluster AnsibleRun apiVersion: $CLUSTER_API"

# Verify namespaced AnsibleRun API group
echo_step "Verifying namespaced AnsibleRun apiVersion"
NS_API=$(${KUBECTL} get ansiblerun.ansible.m.crossplane.io test-ns -n demo -o jsonpath='{.apiVersion}')
if [[ "$NS_API" != "ansible.m.crossplane.io/v1alpha1" ]]; then
    echo_fail "expected ansible.m.crossplane.io/v1alpha1, got $NS_API"
    KUBECTL="${KUBECTL}" "${ROOT_DIR}/cluster/test/teardown.sh"
    exit 1
fi
echo_step_completed "namespaced AnsibleRun apiVersion: $NS_API"

# Teardown
echo_step "Running teardown"
KUBECTL="${KUBECTL}" "${ROOT_DIR}/cluster/test/teardown.sh"
echo_step_completed "Teardown done"

echo_info "All integration tests passed"
