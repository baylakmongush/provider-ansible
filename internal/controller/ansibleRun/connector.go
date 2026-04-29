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

package ansiblerun

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apenella/go-ansible/pkg/stdoutcallback/results"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/spf13/afero"
	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-ansible/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-ansible/internal/ansible"
	"github.com/crossplane-contrib/provider-ansible/pkg/galaxyutil"
	"github.com/crossplane-contrib/provider-ansible/pkg/runnerutil"
)

const (
	errTrackPCUsage        = "cannot track ProviderConfig usage"
	errGetPC               = "cannot get ProviderConfig"
	errGetCreds            = "cannot get credentials"
	errGetInventory        = "cannot get Inventory"
	errWriteGitCreds       = "cannot write .git-credentials to /tmp dir"
	errWriteConfig         = "cannot write ansible collection requirements in" + galaxyutil.RequirementsFile
	errWriteCreds          = "cannot write Playbook credentials"
	errRemoteConfiguration = "cannot get remote AnsibleRun configuration"
	errWriteAnsibleRun     = "cannot write AnsibleRun configuration in" + runnerutil.PlaybookYml
	errWriteInventory      = "cannot write AnsibleRun inventory in"
	errChmodInventory      = "cannot change permissions of inventory file"
	errMarshalRoles        = "cannot marshal Roles into yaml document"
	errMkdir               = "cannot make directory"
	errInit                = "cannot initialize Ansible client"
	gitCredentialsFilename = ".git-credentials"

	errGetAnsibleRun     = "cannot get AnsibleRun"
	errGetLastApplied    = "cannot get last applied"
	errUnmarshalTemplate = "cannot unmarshal template"
)

const (
	leaseDurationSeconds        = 30
	leaseRenewalInterval        = 5 * time.Second
	leaseAcquireAttemptInterval = 5 * time.Second
)

// params is the interface for creating and running ansible commands.
type params interface {
	Init(ctx context.Context, cr ansible.RunCR, behaviorVars map[string]string) (*ansible.Runner, error)
	GalaxyInstall(ctx context.Context, behaviorVars map[string]string, requirementsType string) error
}

// ansibleRunner is the interface for executing an already-initialised ansible run.
type ansibleRunner interface {
	GetAnsibleRunPolicy() *ansible.RunPolicy
	WriteExtraVar(extraVar map[string]interface{}) error
	EnableCheckMode(checkMode bool)
	Run(ctx context.Context) (io.Reader, error)
}

// connector creates an external client for an AnsibleRun CR of type T.
type connector[T resource.Managed] struct {
	kube        client.Client
	usage       resource.Tracker
	fs          afero.Afero
	ansibleFunc func(dir string) params
	replicaID   string
	logger      logging.Logger

	baseWorkingDir      string
	leaseNamespace      string
	leaseNameTemplate   string
	getProviderConfigNS func(cr client.Object) string
	getForProvider      func(cr T) ForProvider
}

// Connect prepares the working directory, resolves the ProviderConfig, writes credentials and
// inventory files, installs galaxy requirements, and returns an initialised external client.
func (c *connector[T]) Connect(ctx context.Context, cr T) (managed.TypedExternalClient[T], error) { //nolint:gocyclo
	dir := filepath.Join(c.baseWorkingDir, string(cr.GetUID()))
	if err := c.fs.MkdirAll(dir, 0700); resource.Ignore(os.IsExist, err) != nil {
		return nil, fmt.Errorf("%s: %s: %w", c.baseWorkingDir, errMkdir, err)
	}

	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, fmt.Errorf("%s: %w", errTrackPCUsage, err)
	}

	// ProviderConfig is always cluster-scoped; the namespace passed here is
	// effectively ignored by the API server but is configurable via GetProviderConfigNS.
	pc := &clusterv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{
		Namespace: c.getProviderConfigNS(cr),
		Name:      cr.GetProviderConfigReference().Name,
	}, pc); err != nil {
		return nil, fmt.Errorf("%s: %w", errGetPC, err)
	}

	fp := c.getForProvider(cr)

	var inventoryPerm os.FileMode = 0600
	if fp.ExecutableInventory {
		inventoryPerm = 0700
	}
	var buff bytes.Buffer
	for _, i := range fp.Inventories {
		data, err := resource.CommonCredentialExtractor(ctx, i.Source, c.kube, i.CommonCredentialSelectors)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", errGetInventory, err)
		}
		if _, err := buff.WriteString(string(data) + "\n"); err != nil {
			return nil, err
		}
	}
	if fp.InventoryInline != nil {
		if _, err := buff.WriteString(*fp.InventoryInline + "\n"); err != nil {
			return nil, err
		}
	}
	if buff.Len() != 0 {
		if err := c.fs.WriteFile(filepath.Join(dir, runnerutil.Hosts), buff.Bytes(), inventoryPerm); err != nil {
			return nil, fmt.Errorf("%s %s: %w", errWriteInventory, runnerutil.Hosts, err)
		}
		// WriteFile only sets permissions for new files; an explicit Chmod ensures
		// permissions are updated for pre-existing files on re-connect.
		if err := c.fs.Chmod(filepath.Join(dir, runnerutil.Hosts), inventoryPerm); err != nil {
			return nil, fmt.Errorf("%s %s: %w", errChmodInventory, runnerutil.Hosts, err)
		}
	}

	// Write .git-credentials to /tmp so ansible-galaxy can pull private roles.
	gitCredDir := filepath.Clean(filepath.Join("/tmp", dir))
	if err := c.fs.MkdirAll(gitCredDir, 0700); err != nil {
		return nil, fmt.Errorf("%s: %w", errWriteGitCreds, err)
	}
	for _, cd := range pc.Spec.Credentials {
		if cd.Filename != gitCredentialsFilename {
			continue
		}
		data, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", errGetCreds, err)
		}
		p := filepath.Clean(filepath.Join(gitCredDir, filepath.Base(cd.Filename)))
		if err := c.fs.WriteFile(p, data, 0600); err != nil {
			return nil, fmt.Errorf("%s: %w", errWriteGitCreds, err)
		}
		if err := os.Setenv("GIT_CRED_DIR", gitCredDir); err != nil {
			return nil, fmt.Errorf("%s: %w", errRemoteConfiguration, err)
		}
	}

	// Marshal roles into a galaxy requirements file when roles are specified;
	// otherwise write the inline playbook to playbook.yml.
	var requirementRoles []byte
	if len(fp.Roles) != 0 {
		rolesMap := map[string][]Role{"roles": fp.Roles}
		var err error
		requirementRoles, err = yaml.Marshal(&rolesMap)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", errMarshalRoles, err)
		}
	} else if fp.PlaybookInline != nil {
		if err := c.fs.WriteFile(filepath.Join(dir, runnerutil.PlaybookYml), []byte(*fp.PlaybookInline), 0600); err != nil {
			return nil, fmt.Errorf("%s: %w", errWriteAnsibleRun, err)
		}
	}

	// Write all provider credentials (SSH keys, vault passwords, etc.) into the working dir.
	for _, cd := range pc.Spec.Credentials {
		data, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", errGetCreds, err)
		}
		p := filepath.Clean(filepath.Join(dir, filepath.Base(cd.Filename)))
		if err := c.fs.WriteFile(p, data, 0600); err != nil {
			return nil, fmt.Errorf("%s: %w", errWriteCreds, err)
		}
	}

	ps := c.ansibleFunc(dir)
	behaviorVars := addBehaviorVars(pc)

	requirementRolesStr := string(requirementRoles)
	if pc.Spec.Requirements != nil || requirementRolesStr != "" {
		var installCollections, installRoles bool
		var reqSlice []string
		if pc.Spec.Requirements != nil {
			reqSlice = append(reqSlice, *pc.Spec.Requirements)
			installCollections = true
			installRoles = true
		}
		if requirementRolesStr != "" {
			reqSlice = append(reqSlice, requirementRolesStr)
			installRoles = true
		}

		req := strings.Join(reqSlice, "\n")
		if err := c.fs.WriteFile(filepath.Join(dir, galaxyutil.RequirementsFile), []byte(req), 0600); err != nil {
			return nil, fmt.Errorf("%s: %w", errWriteConfig, err)
		}
		if installCollections {
			if err := ps.GalaxyInstall(ctx, behaviorVars, "collection"); err != nil {
				return nil, err
			}
		}
		if installRoles {
			if err := ps.GalaxyInstall(ctx, behaviorVars, "role"); err != nil {
				return nil, err
			}
		}
	}

	runCR := ansible.RunCR{
		PlaybookInline: fp.PlaybookInline,
		Vars:           fp.Vars,
		RunPolicy:      ansible.GetPolicyRun(cr),
	}
	for _, role := range fp.Roles {
		runCR.Roles = append(runCR.Roles, ansible.RunRole{Name: role.Name})
	}
	r, err := ps.Init(ctx, runCR, behaviorVars)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errInit, err)
	}

	return &external[T]{runner: r, kube: c.kube, getForProvider: c.getForProvider}, nil
}

// external implements the TypedExternalClient for an AnsibleRun CR.
type external[T resource.Managed] struct {
	runner         ansibleRunner
	kube           client.Client
	getForProvider func(cr T) ForProvider
}

func (e *external[T]) Disconnect(_ context.Context) error {
	return nil
}

// nolint:gocyclo
func (e *external[T]) Observe(ctx context.Context, cr T) (managed.ExternalObservation, error) {
	// Ansible has no queryable external state, so we always set DeletionOrphan
	// to avoid blocking on phantom external resource deletion.
	cr.SetDeletionPolicy(xpv1.DeletionOrphan)

	switch e.runner.GetAnsibleRunPolicy().Name {
	case "ObserveAndDelete", "":
		if e.runner.GetAnsibleRunPolicy().Name == "" {
			ansible.SetPolicyRun(cr, "ObserveAndDelete")
		}
		if meta.WasDeleted(cr) {
			return managed.ExternalObservation{ResourceExists: true}, nil
		}
		observed := cr.DeepCopyObject().(T)
		if err := e.kube.Get(ctx, types.NamespacedName{
			Namespace: observed.GetNamespace(),
			Name:      observed.GetName(),
		}, observed); err != nil {
			if kerrors.IsNotFound(err) {
				return managed.ExternalObservation{ResourceExists: false}, nil
			}
			return managed.ExternalObservation{}, fmt.Errorf("%s: %w", errGetAnsibleRun, err)
		}
		lastParameters, err := getLastAppliedParameters(observed)
		if err != nil {
			return managed.ExternalObservation{}, fmt.Errorf("%s: %w", errGetLastApplied, err)
		}
		return e.handleLastApplied(ctx, lastParameters, cr)
	case "CheckWhenObserve":
		stateVar := map[string]string{"state": "present"}
		nestedMap := map[string]interface{}{cr.GetName(): stateVar}
		if err := e.runner.WriteExtraVar(nestedMap); err != nil {
			return managed.ExternalObservation{}, err
		}
		e.runner.EnableCheckMode(true)
		stdoutBuf, err := e.runner.Run(ctx)
		if err != nil {
			return managed.ExternalObservation{}, err
		}
		res, err := results.ParseJSONResultsStream(stdoutBuf)
		if err != nil {
			return managed.ExternalObservation{}, err
		}
		changes := ansible.Diff(res)
		// Ansible cannot report whether the external resource exists at all,
		// so we always say it exists and drive updates via the changed flag.
		return managed.ExternalObservation{
			ResourceExists:          true,
			ResourceUpToDate:        !changes,
			ResourceLateInitialized: false,
		}, nil
	}

	return managed.ExternalObservation{}, nil
}

func (e *external[T]) Create(ctx context.Context, cr T) (managed.ExternalCreation, error) {
	// Create and Update are equivalent from Ansible's perspective.
	u, err := e.Update(ctx, cr)
	return managed.ExternalCreation(u), err
}

func (e *external[T]) Update(ctx context.Context, cr T) (managed.ExternalUpdate, error) {
	e.runner.EnableCheckMode(false)
	if err := e.runAnsible(ctx, cr); err != nil {
		return managed.ExternalUpdate{}, fmt.Errorf("running ansible: %w", err)
	}
	return managed.ExternalUpdate{ConnectionDetails: nil}, nil
}

func (e *external[T]) Delete(ctx context.Context, cr T) (managed.ExternalDelete, error) {
	cr.SetConditions(xpv1.Deleting())

	stateVar := map[string]string{"state": "absent"}
	nestedMap := map[string]interface{}{cr.GetName(): stateVar}
	if err := e.runner.WriteExtraVar(nestedMap); err != nil {
		return managed.ExternalDelete{}, err
	}
	_, err := e.runner.Run(ctx)
	if err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// getLastAppliedParameters reads the last-applied ForProvider from the CR's annotation.
func getLastAppliedParameters(cr client.Object) (*ForProvider, error) {
	lastApplied, ok := cr.GetAnnotations()[v1.LastAppliedConfigAnnotation]
	if !ok {
		return nil, nil
	}
	lastParameters := &ForProvider{}
	if err := json.Unmarshal([]byte(lastApplied), lastParameters); err != nil {
		return nil, fmt.Errorf("%s: %w", errUnmarshalTemplate, err)
	}
	return lastParameters, nil
}

// handleLastApplied compares the persisted last-applied parameters with the current desired state.
// If they differ (or the last sync failed), it re-runs ansible and updates the annotation.
func (e *external[T]) handleLastApplied(ctx context.Context, lastParameters *ForProvider, desired T) (managed.ExternalObservation, error) {
	fp := e.getForProvider(desired)
	isUpToDate := lastParameters != nil && equality.Semantic.DeepEqual(*lastParameters, fp)
	isLastSyncOK := desired.GetCondition(xpv1.TypeSynced).Status == v1.ConditionTrue

	if isUpToDate && isLastSyncOK {
		desired.SetConditions(xpv1.Available())
		if err := e.kube.Status().Update(ctx, desired); err != nil {
			return managed.ExternalObservation{}, fmt.Errorf("updating status: %w", err)
		}
		return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
	}

	// Persist the current ForProvider so future observations can detect no-op reconciles.
	out, err := json.Marshal(fp)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	meta.AddAnnotations(desired, map[string]string{
		v1.LastAppliedConfigAnnotation: string(out),
	})

	if err := e.kube.Update(ctx, desired); err != nil {
		return managed.ExternalObservation{}, err
	}

	stateVar := map[string]string{"state": "present"}
	nestedMap := map[string]interface{}{desired.GetName(): stateVar}
	if err := e.runner.WriteExtraVar(nestedMap); err != nil {
		return managed.ExternalObservation{}, err
	}

	if err := e.runAnsible(ctx, desired); err != nil {
		return managed.ExternalObservation{}, fmt.Errorf("running ansible: %w", err)
	}

	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}

// runAnsible executes ansible-runner and updates the CR's Ready condition accordingly.
func (e *external[T]) runAnsible(ctx context.Context, cr T) error {
	_, err := e.runner.Run(ctx)
	if err != nil {
		cond := xpv1.Unavailable()
		cond.Message = err.Error()
		cr.SetConditions(cond)
	} else {
		cr.SetConditions(xpv1.Available())
	}

	if err := e.kube.Status().Update(ctx, cr); err != nil {
		return fmt.Errorf("updating status: %w", err)
	}

	return err
}

// addBehaviorVars converts ProviderConfig.Spec.Vars into the map expected by ansible.Parameters.
func addBehaviorVars(pc *clusterv1alpha1.ProviderConfig) map[string]string {
	behaviorVars := make(map[string]string, len(pc.Spec.Vars))
	for _, v := range pc.Spec.Vars {
		behaviorVars[v.Key] = v.Value
	}
	return behaviorVars
}

// generateLeaseName formats the lease name for a given shard index.
func (c *connector[T]) generateLeaseName(index uint32) string {
	return fmt.Sprintf(c.leaseNameTemplate, index)
}

// releaseLease deletes the coordination lease for the given shard index.
func (c *connector[T]) releaseLease(ctx context.Context, kube client.Client, index uint32) error {
	leaseName := c.generateLeaseName(index)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.leaseNamespace, Name: leaseName},
	}
	return kube.Delete(ctx, lease)
}

// acquireLease attempts to acquire or renew the coordination lease for the given shard index.
// Returns an error when the lease is currently held by a different replica that has not expired.
func (c *connector[T]) acquireLease(ctx context.Context, kube client.Client, index uint32) error {
	lease := &coordinationv1.Lease{}
	leaseName := c.generateLeaseName(index)
	leaseDuration := ptr.To(int32(leaseDurationSeconds))

	if err := kube.Get(ctx, client.ObjectKey{Namespace: c.leaseNamespace, Name: leaseName}, lease); err != nil {
		if !kerrors.IsNotFound(err) {
			return err
		}

		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      leaseName,
				Namespace: c.leaseNamespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &c.replicaID,
				RenewTime:            &metav1.MicroTime{Time: time.Now()},
				LeaseDurationSeconds: leaseDuration,
			},
		}
		if err := kube.Create(ctx, lease); err != nil {
			return err
		}
		c.logger.Debug("created lease", "lease", lease)
		return nil
	}

	// Only block acquisition when another replica holds a non-expired lease.
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != c.replicaID {
		if lease.Spec.RenewTime != nil && time.Since(lease.Spec.RenewTime.Time) < time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second {
			return fmt.Errorf("lease is still held by %s", *lease.Spec.HolderIdentity)
		}
	}

	lease.Spec.HolderIdentity = ptr.To(c.replicaID)
	lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
	lease.Spec.LeaseDurationSeconds = leaseDuration
	if err := kube.Update(ctx, lease); err != nil {
		if kerrors.IsConflict(err) {
			return err
		}
		return fmt.Errorf("failed to update lease: %w", err)
	}

	c.logger.Debug("updated lease", "lease", lease)
	return nil
}

// acquireAndHoldShard finds an available shard, acquires its lease, and starts a background
// goroutine to renew the lease continuously until the provider context is cancelled.
func (c *connector[T]) acquireAndHoldShard(o controller.Options, s SetupOptions[T]) (uint32, error) {
	ctx := s.ProviderCtx
	var currentShard uint32

	cfg := ctrl.GetConfigOrDie()
	kube, err := client.New(cfg, client.Options{})
	if err != nil {
		return 0, err
	}

AcquireLease:
	for {
		for i := uint32(0); i < s.ReplicasCount; i++ {
			if err := c.acquireLease(ctx, kube, i); err == nil {
				currentShard = i
				o.Logger.Debug("acquired lease", "id", i)
				go func() {
					for {
						select {
						case <-time.After(leaseRenewalInterval):
							if err := c.acquireLease(ctx, kube, i); err != nil {
								o.Logger.Info("failed to renew lease", "id", i, "err", err)
								s.ProviderCancel()
							} else {
								o.Logger.Debug("renewed lease", "id", i)
							}
						case <-ctx.Done():
							o.Logger.Info("controller is shutting down, releasing lease")
							if err := c.releaseLease(context.Background(), kube, i); err != nil {
								o.Logger.Info("failed to release lease", "lease", err)
							}
							o.Logger.Debug("released lease")
							return
						}
					}
				}()
				break AcquireLease
			} else {
				o.Logger.Debug("cannot acquire lease", "id", i, "err", err)
				time.Sleep(leaseAcquireAttemptInterval)
			}
		}
	}

	return currentShard, nil
}
