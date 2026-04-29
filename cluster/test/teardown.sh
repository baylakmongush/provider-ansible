#!/usr/bin/env bash
set -aeuo pipefail

# setting up colors
GRN='\033[0;32m'
BLU='\033[0;104m'
NOC='\033[0m'

echo_info(){
    printf "\n${BLU}%s${NOC}\n" "$1"
}
echo_step_completed(){
    printf "${GRN} [✔] %s${NOC}\n" "$1"
}

KUBECTL="${KUBECTL:-kubectl}"

echo_info "Cleaning up test resources"

${KUBECTL} delete ansiblerun test-cluster --ignore-not-found
echo_step_completed "cluster AnsibleRun deleted"

${KUBECTL} delete providerconfig provider-ansible-config --ignore-not-found
echo_step_completed "cluster ProviderConfig deleted"

${KUBECTL} delete ansiblerun test-ns -n demo --ignore-not-found
${KUBECTL} delete providerconfig provider-ansible-config -n demo --ignore-not-found
echo_step_completed "namespaced resources deleted"

${KUBECTL} delete namespace demo --ignore-not-found
echo_step_completed "namespace demo deleted"

echo_info "Teardown complete"
