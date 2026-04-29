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

echo_info "Running setup.sh"

KUBECTL="${KUBECTL:-kubectl}"

echo_info "Waiting until provider is healthy..."
${KUBECTL} wait provider.pkg --all --for condition=Healthy --timeout 5m || true

echo_info "Waiting for all pods to come online..."
${KUBECTL} -n upbound-system wait --for=condition=Available deployment --all --timeout=5m || true

# Apply CRDs
echo_step "Applying CRDs"
${KUBECTL} apply -R -f package/crds/
echo_step_completed "CRDs applied"

# Verify scopes
echo_step "Verifying CRD scopes"
CLUSTER_SCOPE=$(${KUBECTL} get crd ansibleruns.ansible.crossplane.io -o jsonpath='{.spec.scope}')
NS_SCOPE=$(${KUBECTL} get crd ansibleruns.ansible.m.crossplane.io -o jsonpath='{.spec.scope}')

if [[ "$CLUSTER_SCOPE" != "Cluster" ]]; then
    printf "${RED}FAIL: ansibleruns.ansible.crossplane.io scope is '$CLUSTER_SCOPE', expected 'Cluster'${NOC}\n"
    exit 1
fi
echo_step_completed "ansibleruns.ansible.crossplane.io scope: $CLUSTER_SCOPE"

if [[ "$NS_SCOPE" != "Namespaced" ]]; then
    printf "${RED}FAIL: ansibleruns.ansible.m.crossplane.io scope is '$NS_SCOPE', expected 'Namespaced'${NOC}\n"
    exit 1
fi
echo_step_completed "ansibleruns.ansible.m.crossplane.io scope: $NS_SCOPE"

# Apply cluster-scoped ProviderConfig and AnsibleRun
echo_step "Applying cluster-scoped ProviderConfig"
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: ansible.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: provider-ansible-config
spec:
  credentials: []
EOF
echo_step_completed "cluster ProviderConfig created"

echo_step "Applying cluster-scoped AnsibleRun"
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: ansible.crossplane.io/v1alpha1
kind: AnsibleRun
metadata:
  name: test-cluster
spec:
  providerConfigRef:
    name: provider-ansible-config
  forProvider:
    inventoryInline: |
      localhost ansible_connection=local
    playbookInline: |
      - hosts: localhost
        gather_facts: false
        tasks:
          - debug:
              msg: "hello from cluster ansiblerun"
EOF
echo_step_completed "cluster AnsibleRun created"

# Apply namespaced AnsibleRun
echo_step "Creating namespace demo"
${KUBECTL} create namespace demo --dry-run=client -o yaml | ${KUBECTL} apply -f -
echo_step_completed "namespace demo ready"

echo_step "Applying namespaced AnsibleRun"
cat <<EOF | ${KUBECTL} apply -f -
apiVersion: ansible.m.crossplane.io/v1alpha1
kind: AnsibleRun
metadata:
  name: test-ns
  namespace: demo
spec:
  providerConfigRef:
    name: provider-ansible-config
  forProvider:
    inventoryInline: |
      localhost ansible_connection=local
    playbookInline: |
      - hosts: localhost
        gather_facts: false
        tasks:
          - debug:
              msg: "hello from namespaced ansiblerun"
EOF
echo_step_completed "namespaced AnsibleRun created"

echo_info "Setup complete. Resources created:"
${KUBECTL} get ansiblerun.ansible.crossplane.io test-cluster
${KUBECTL} get ansiblerun.ansible.m.crossplane.io -n demo
