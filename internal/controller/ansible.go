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

package controller

import (
	"github.com/crossplane/crossplane-runtime/pkg/controller"
	ctrl "sigs.k8s.io/controller-runtime"

	clusteransiblerun "github.com/crossplane-contrib/provider-ansible/internal/controller/cluster/ansiblerun"
	"github.com/crossplane-contrib/provider-ansible/internal/controller/config"
	namespacedansiblerun "github.com/crossplane-contrib/provider-ansible/internal/controller/namespaced/ansiblerun"
)

// Setup creates all AnsibleRun controllers with the supplied logger and adds them to
// the supplied manager.
func Setup(mgr ctrl.Manager, o controller.Options, s clusteransiblerun.SetupOptions) error {
	if err := config.Setup(mgr, o); err != nil {
		return err
	}

	if err := clusteransiblerun.Setup(mgr, o, s); err != nil {
		return err
	}

	nss := namespacedansiblerun.SetupOptions{
		AnsibleCollectionsPath: s.AnsibleCollectionsPath,
		AnsibleRolesPath:       s.AnsibleRolesPath,
		Timeout:                s.Timeout,
		ArtifactsHistoryLimit:  s.ArtifactsHistoryLimit,
		ReplicasCount:          s.ReplicasCount,
		ProviderCtx:            s.ProviderCtx,
		ProviderCancel:         s.ProviderCancel,
	}
	if err := namespacedansiblerun.Setup(mgr, o, nss); err != nil {
		return err
	}

	return nil
}
