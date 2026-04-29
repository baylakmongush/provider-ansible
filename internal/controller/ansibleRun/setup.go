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

// Package ansiblerun contains the common AnsibleRun controller logic shared by
// both cluster-scoped and namespace-scoped variants.
package ansiblerun

import (
	"context"
	"fmt"
	"time"

	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/statemetrics"
	"github.com/google/uuid"
	"github.com/spf13/afero"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane-contrib/provider-ansible/internal/ansible"
	"github.com/crossplane-contrib/provider-ansible/pkg/galaxyutil"
	"github.com/crossplane-contrib/provider-ansible/pkg/runnerutil"
	"github.com/crossplane-contrib/provider-ansible/pkg/shardutil"
)

// ForProvider holds the reconciliation parameters from an AnsibleRun CR.
// Its JSON field names intentionally mirror AnsibleRunParameters so that
// annotations written by either the old or new code remain mutually readable.
type ForProvider struct {
	InventoryInline     *string              `json:"inventoryInline"`
	Inventories         []Inventory          `json:"inventories"`
	ExecutableInventory bool                 `json:"executableInventory"`
	PlaybookInline      *string              `json:"playbookInline"`
	Roles               []Role               `json:"roles"`
	Vars                runtime.RawExtension `json:"vars,omitempty"`
}

// Inventory is the inventory source configuration.
type Inventory struct {
	Source                         xpv1.CredentialsSource `json:"source"`
	xpv1.CommonCredentialSelectors `json:",inline"`
}

// Role is the ansible role definition.
type Role struct {
	Name    string `json:"name"`
	Src     string `json:"src"`
	Version string `json:"version,omitempty"`
}

// SetupOptions configures the common AnsibleRun controller.
// It is parameterized on T, the concrete AnsibleRun CR type (cluster or namespaced).
type SetupOptions[T resource.Managed] struct {
	AnsibleCollectionsPath string
	AnsibleRolesPath       string
	Timeout                time.Duration
	ArtifactsHistoryLimit  int
	ReplicasCount          uint32
	ProviderCtx            context.Context
	ProviderCancel         context.CancelFunc

	// LeaseNameTemplate is the printf format string used to derive lease names, e.g. "provider-ansible-lease-%d".
	LeaseNameTemplate string
	// LeaseNamespace is the Kubernetes namespace where coordination leases are stored.
	LeaseNamespace string
	// BaseWorkingDir is the root directory under which per-CR ansible working directories are created.
	BaseWorkingDir string
	// GetProviderConfigNS resolves the namespace to use when looking up the ProviderConfig.
	// Returns "" for cluster-scoped resources; cr.GetNamespace() for namespace-scoped resources.
	GetProviderConfigNS func(cr client.Object) string

	// GroupKind is the GroupKind string used to derive the managed controller name.
	GroupKind string
	// GroupVersionKind identifies the CR type registered with the managed reconciler.
	GroupVersionKind schema.GroupVersionKind
	// NewAnsibleRun returns a new, empty instance of the CR type for controller registration.
	NewAnsibleRun func() T
	// NewAnsibleRunList returns an empty AnsibleRunList used by the metrics state recorder.
	NewAnsibleRunList func() resource.ManagedList
	// GetForProvider extracts the reconciliation parameters from a CR instance.
	GetForProvider func(cr T) ForProvider
	// NewUsageTracker constructs the ProviderConfig usage tracker for the appropriate PCU type.
	NewUsageTracker func(kube client.Client) resource.Tracker
}

// Setup registers a controller that reconciles AnsibleRun managed resources of type T.
// Both cluster-scoped and namespace-scoped variants call this with their own SetupOptions.
func Setup[T resource.Managed](mgr ctrl.Manager, o controller.Options, s SetupOptions[T]) error {
	name := managed.ControllerName(s.GroupKind)

	fs := afero.Afero{Fs: afero.NewOsFs()}

	galaxyBinary, err := galaxyutil.GalaxyBinary()
	if err != nil {
		return err
	}
	runnerBinary, err := runnerutil.RunnerBinary()
	if err != nil {
		return err
	}

	c := &connector[T]{
		kube:  mgr.GetClient(),
		usage: s.NewUsageTracker(mgr.GetClient()),
		fs:    fs,
		ansibleFunc: func(dir string) params {
			return ansible.Parameters{
				WorkingDirPath:        dir,
				GalaxyBinary:          galaxyBinary,
				RunnerBinary:          runnerBinary,
				CollectionsPath:       s.AnsibleCollectionsPath,
				RolesPath:             s.AnsibleRolesPath,
				ArtifactsHistoryLimit: s.ArtifactsHistoryLimit,
			}
		},
		replicaID:           uuid.New().String(),
		logger:              o.Logger,
		baseWorkingDir:      s.BaseWorkingDir,
		leaseNamespace:      s.LeaseNamespace,
		leaseNameTemplate:   s.LeaseNameTemplate,
		getProviderConfigNS: s.GetProviderConfigNS,
		getForProvider:      s.GetForProvider,
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector(c),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithTimeout(s.Timeout),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
	}

	if o.MetricOptions != nil {
		opts = append(opts, managed.WithMetricRecorder(o.MetricOptions.MRMetrics))
		if o.MetricOptions.MRStateMetrics != nil {
			stateMetricsRecorder := statemetrics.NewMRStateRecorder(
				mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics,
				s.NewAnsibleRunList(), o.MetricOptions.PollStateMetricInterval,
			)
			if err := mgr.Add(stateMetricsRecorder); err != nil {
				return fmt.Errorf("cannot register MR state metrics recorder: %w", err)
			}
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(s.GroupVersionKind), opts...)

	currentShard, err := c.acquireAndHoldShard(o, s)
	if err != nil {
		return fmt.Errorf("cannot acquire and hold shard: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(s.NewAnsibleRun()).
		WithEventFilter(shardutil.IsResourceForShard(currentShard, s.ReplicasCount)).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
