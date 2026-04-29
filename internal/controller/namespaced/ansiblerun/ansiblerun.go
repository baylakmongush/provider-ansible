/*
Copyright 2020 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package ansiblerun is a thin wrapper around the common ansibleRun controller.
// It registers the namespace-scoped AnsibleRun CR and supplies namespace-specific
// configuration (working directory, lease naming, ProviderConfig namespace).
package ansiblerun

import (
	"context"
	"os"
	"time"

	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-ansible/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-ansible/apis/namespaced/v1alpha1"
	ansiblerun "github.com/crossplane-contrib/provider-ansible/internal/controller/ansibleRun"
)

// SetupOptions contains settings for the namespace-scoped AnsibleRun controller.
type SetupOptions struct {
	AnsibleCollectionsPath string
	AnsibleRolesPath       string
	Timeout                time.Duration
	ArtifactsHistoryLimit  int
	ReplicasCount          uint32
	ProviderCtx            context.Context
	ProviderCancel         context.CancelFunc
}

// leaseNamespace returns the namespace used to store controller coordination leases.
// It reads the LEASE_NAMESPACE environment variable and falls back to "upbound-system".
func leaseNamespace() string {
	if ns := os.Getenv("LEASE_NAMESPACE"); ns != "" {
		return ns
	}
	return "upbound-system"
}

// Setup adds a controller that reconciles namespace-scoped AnsibleRun managed resources.
func Setup(mgr ctrl.Manager, o controller.Options, s SetupOptions) error {
	return ansiblerun.Setup(mgr, o, ansiblerun.SetupOptions[*namespacedv1alpha1.AnsibleRun]{
		AnsibleCollectionsPath: s.AnsibleCollectionsPath,
		AnsibleRolesPath:       s.AnsibleRolesPath,
		Timeout:                s.Timeout,
		ArtifactsHistoryLimit:  s.ArtifactsHistoryLimit,
		ReplicasCount:          s.ReplicasCount,
		ProviderCtx:            s.ProviderCtx,
		ProviderCancel:         s.ProviderCancel,
		LeaseNameTemplate:      "provider-ansible-namespaced-lease-%d",
		LeaseNamespace:         leaseNamespace(),
		BaseWorkingDir:         "/ansibleDir/ns",
		// For namespace-scoped resources the ProviderConfig lookup can be scoped
		// to the same namespace as the AnsibleRun; the API server ignores the
		// namespace field for cluster-scoped resources.
		GetProviderConfigNS: func(cr client.Object) string { return cr.GetNamespace() },
		GroupKind:           namespacedv1alpha1.AnsibleRunGroupKind,
		GroupVersionKind:    namespacedv1alpha1.AnsibleRunGroupVersionKind,
		NewAnsibleRun:       func() *namespacedv1alpha1.AnsibleRun { return &namespacedv1alpha1.AnsibleRun{} },
		NewAnsibleRunList:   func() resource.ManagedList { return &namespacedv1alpha1.AnsibleRunList{} },
		GetForProvider:      namespacedForProvider,
		// ProviderConfigUsage tracking always uses the cluster-scoped PCU type.
		NewUsageTracker: func(kube client.Client) resource.Tracker {
			return resource.NewProviderConfigUsageTracker(kube, &clusterv1alpha1.ProviderConfigUsage{})
		},
	})
}

// namespacedForProvider converts a namespace-scoped AnsibleRun's spec into the common ForProvider type.
func namespacedForProvider(cr *namespacedv1alpha1.AnsibleRun) ansiblerun.ForProvider {
	fp := ansiblerun.ForProvider{
		InventoryInline:     cr.Spec.ForProvider.InventoryInline,
		ExecutableInventory: cr.Spec.ForProvider.ExecutableInventory,
		PlaybookInline:      cr.Spec.ForProvider.PlaybookInline,
		Vars:                cr.Spec.ForProvider.Vars,
	}
	for _, inv := range cr.Spec.ForProvider.Inventories {
		fp.Inventories = append(fp.Inventories, ansiblerun.Inventory{
			Source:                   inv.Source,
			CommonCredentialSelectors: inv.CommonCredentialSelectors,
		})
	}
	for _, r := range cr.Spec.ForProvider.Roles {
		fp.Roles = append(fp.Roles, ansiblerun.Role{
			Name:    r.Name,
			Src:     r.Src,
			Version: r.Version,
		})
	}
	return fp
}
