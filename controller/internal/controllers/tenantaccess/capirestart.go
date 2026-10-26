// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package tenantaccess

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/dsx-ai-factory/nvke/controller/internal/metrics"
)

const (
	// capiNamespace and capiDeploymentName name the Cluster API core deployment
	// on the management cluster. These are the upstream defaults, which the
	// capi-core manifest deployed alongside the controller does not change.
	capiNamespace      = "capi-system"
	capiDeploymentName = "capi-controller-manager"

	// capiRestartAnnotation is the pod-template annotation whose change rolls
	// the deployment. It mirrors what `kubectl rollout restart` does, under an
	// NVKE-owned key so the two cannot overwrite each other.
	capiRestartAnnotation = "nvke.nvidia.com/restarted-at"

	// capiRestartMinInterval coalesces rolls. A site that rotates several tenant
	// kubeconfigs in one pass needs one roll, not one per cluster: each roll
	// drops every workload-cluster informer Cluster API holds and rebuilds them
	// against every tenant apiserver at once.
	capiRestartMinInterval = 10 * time.Minute
)

// restartCAPI rolls capi-controller-manager so it rebuilds its workload-cluster
// clients from the kubeconfig Secrets as they now stand.
//
// Cluster API reads a tenant kubeconfig Secret exactly once, when it first
// connects, and keeps the resulting client for the life of the pod: its
// ClusterCache reconciler watches Clusters only, nothing watches the Secret,
// and nothing compares its resourceVersion. The only path back to a fresh
// Secret is a failing health probe, so a rotated client certificate does not
// take effect until the cached one is actually rejected -- and the expiry alert
// fires seven days before that. Rolling the deployment is what makes a rotation
// take effect when it happens rather than after the page.
//
// Best effort. A failure here leaves a correctly published Secret and a stale
// cache, which is exactly the behavior without this call, so it is logged and
// counted rather than failing the publish.
func (c *Controller) restartCAPI(ctx context.Context, clusterName string) {
	deployment := capiNamespace + "/" + capiDeploymentName

	at := c.now()
	wasPending, claimed := c.claimCAPIRestart(at, clusterName)
	if !claimed {
		c.logger.Debug("Cluster API rolled recently; holding this rotation until the coalescing window closes",
			"clusterName", clusterName, "deployment", deployment)
		return
	}

	switch err := c.rollCAPIDeployment(ctx, at); {
	case err == nil:
		c.logger.Info("Rolled Cluster API after a tenant kubeconfig rotation",
			"clusterName", clusterName, "deployment", deployment)
		metrics.RecordCAPIKubeconfigRestart(metrics.CAPIRestartResultRolled)
	case apierrors.IsNotFound(err):
		// No Cluster API core on this management cluster -- a local or
		// Tilt-driven run. Nothing to roll, and nothing wrong.
		c.logger.Debug("Cluster API deployment not found; nothing to roll",
			"clusterName", clusterName, "deployment", deployment)
		metrics.RecordCAPIKubeconfigRestart(metrics.CAPIRestartResultAbsent)
	default:
		// Reopen the window so the next rotation retries instead of waiting it
		// out behind a roll that never happened, and restore any request that
		// was already being held so a failure here does not discard it.
		c.releaseCAPIRestart(at, wasPending)
		c.logger.Error("Failed to roll Cluster API after a tenant kubeconfig rotation",
			"clusterName", clusterName, "deployment", deployment, "error", err)
		metrics.RecordCAPIKubeconfigRestart(metrics.CAPIRestartResultError)
	}
}

// claimCAPIRestart reserves the coalescing window, returning whether a request
// was already being held and whether this caller may roll now, in that order.
//
// A request that arrives inside the window is held rather than dropped. The
// roll that opened the window ran before this Secret was written, so it cannot
// have loaded it; dropping the request would leave that one tenant on a
// superseded credential until something else happened to roll the deployment.
func (c *Controller) claimCAPIRestart(at time.Time, clusterName string) (bool, bool) {
	c.capiRestartMu.Lock()
	defer c.capiRestartMu.Unlock()

	wasPending := c.capiRestartPending
	if !c.capiRestartedAt.IsZero() && at.Sub(c.capiRestartedAt) < capiRestartMinInterval {
		c.capiRestartPending = true
		c.capiRestartPendingFor = clusterName
		return wasPending, false
	}

	c.capiRestartedAt = at
	c.capiRestartPending = false
	c.capiRestartPendingFor = ""
	return wasPending, true
}

// releaseCAPIRestart reopens a window claimed for a roll that then failed, and
// restores the held request the claim consumed. The equality check keeps it
// from discarding a newer claim.
func (c *Controller) releaseCAPIRestart(at time.Time, wasPending bool) {
	c.capiRestartMu.Lock()
	defer c.capiRestartMu.Unlock()

	if c.capiRestartedAt.Equal(at) {
		c.capiRestartedAt = time.Time{}
		c.capiRestartPending = wasPending
	}
}

// pendingCAPIRestart returns the cluster whose rotation is being held, how long
// until that roll is due, and whether one is held at all, in that order. A zero
// delay means it is due now.
func (c *Controller) pendingCAPIRestart(at time.Time) (string, time.Duration, bool) {
	c.capiRestartMu.Lock()
	defer c.capiRestartMu.Unlock()

	if !c.capiRestartPending {
		return "", 0, false
	}

	elapsed := at.Sub(c.capiRestartedAt)
	if elapsed >= capiRestartMinInterval {
		return c.capiRestartPendingFor, 0, true
	}
	return c.capiRestartPendingFor, capiRestartMinInterval - elapsed, true
}

// drainPendingCAPIRestart rolls a held request once its window has closed, and
// otherwise asks the reconciler to come back when it does.
//
// The requeue is the point: a held request must not wait for whatever reconcile
// happens to arrive next, which on a quiet site is the periodic resync. That
// delay is the one this change exists to remove.
func (c *Controller) drainPendingCAPIRestart(ctx context.Context) ctrl.Result {
	clusterName, delay, pending := c.pendingCAPIRestart(c.now())
	switch {
	case !pending:
		return ctrl.Result{}
	case delay > 0:
		return ctrl.Result{RequeueAfter: delay}
	default:
		c.restartCAPI(ctx, clusterName)
		return ctrl.Result{}
	}
}

// rollCAPIDeployment stamps the pod-template annotation that triggers a rolling
// restart. The Get error is returned unwrapped so the caller's
// apierrors.IsNotFound can tell "no Cluster API on this cluster" from a real
// failure.
func (c *Controller) rollCAPIDeployment(ctx context.Context, at time.Time) error {
	key := types.NamespacedName{Namespace: capiNamespace, Name: capiDeploymentName}

	deployment := &appsv1.Deployment{}
	if err := c.k8sClient.Get(ctx, key, deployment); err != nil {
		return err
	}

	patch := ctrlclient.MergeFrom(deployment.DeepCopy())
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}
	deployment.Spec.Template.Annotations[capiRestartAnnotation] = at.UTC().Format(time.RFC3339)

	if err := c.k8sClient.Patch(ctx, deployment, patch); err != nil {
		return fmt.Errorf("patching deployment %s: %w", key, err)
	}
	return nil
}
