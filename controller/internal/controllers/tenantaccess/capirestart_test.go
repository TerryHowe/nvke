// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package tenantaccess

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/dsx-ai-factory/nvke/controller/internal/config"
	"github.com/dsx-ai-factory/nvke/controller/internal/constants"
	"github.com/dsx-ai-factory/nvke/controller/internal/db"
	"github.com/dsx-ai-factory/nvke/controller/internal/testutil"
)

// capiRestartScheme is testScheme plus apps/v1, which the Cluster API
// deployment needs and no other tenant-access test does.
func capiRestartScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := testScheme(t)
	require.NoError(t, appsv1.AddToScheme(scheme))
	return scheme
}

func capiDeploymentFixture() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: capiDeploymentName, Namespace: capiNamespace},
	}
}

// capiRestartedAtAnnotation returns the restart stamp on the live deployment,
// or "" when it carries none.
func capiRestartedAtAnnotation(t *testing.T, ctx context.Context, k8sClient ctrlclient.Client) string {
	t.Helper()
	deployment := &appsv1.Deployment{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: capiDeploymentName, Namespace: capiNamespace}, deployment))
	return deployment.Spec.Template.Annotations[capiRestartAnnotation]
}

func TestRestartCAPI(t *testing.T) {
	start := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		// offsets are the clock readings for successive restartCAPI calls,
		// relative to start.
		offsets    []time.Duration
		wantStamps []string
	}{
		{
			name:       "first rotation rolls the deployment",
			offsets:    []time.Duration{0},
			wantStamps: []string{"2026-10-24T12:00:00Z"},
		},
		{
			name: "rotations inside the window do not each roll",
			// Several tenant kubeconfigs rotating in one pass must not roll
			// Cluster API once per cluster. They are held, not dropped --
			// TestDrainPendingCAPIRestart covers the trailing roll.
			offsets:    []time.Duration{0, time.Minute, 2 * time.Minute},
			wantStamps: []string{"2026-10-24T12:00:00Z", "2026-10-24T12:00:00Z", "2026-10-24T12:00:00Z"},
		},
		{
			name:       "a rotation after the window rolls again",
			offsets:    []time.Duration{0, capiRestartMinInterval},
			wantStamps: []string{"2026-10-24T12:00:00Z", "2026-10-24T12:10:00Z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			k8sClient := fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(capiDeploymentFixture()).Build()

			c := newTestController(config.Config{}, &testutil.MockDB{}, k8sClient, nil)

			for i, offset := range tt.offsets {
				c.now = func() time.Time { return start.Add(offset) }
				c.restartCAPI(ctx, "tenant")
				assert.Equal(t, tt.wantStamps[i], capiRestartedAtAnnotation(t, ctx, k8sClient), "after call %d", i)
			}
		})
	}
}

// A management cluster without Cluster API core -- a local or Tilt-driven run --
// must not turn every rotation into an error.
func TestRestartCAPIWithNoDeployment(t *testing.T) {
	ctx := context.Background()
	k8sClient := fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).Build()

	c := newTestController(config.Config{}, &testutil.MockDB{}, k8sClient, nil)
	c.restartCAPI(ctx, "tenant")

	deployment := &appsv1.Deployment{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: capiDeploymentName, Namespace: capiNamespace}, deployment)
	assert.True(t, apierrors.IsNotFound(err), "expected the deployment to still be absent, got %v", err)
}

// A failed roll must reopen the coalescing window, or one transient API error
// suppresses the retry for the whole interval.
func TestRestartCAPIReopensWindowAfterFailure(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	k8sClient := &patchFailingClient{
		Client: fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(capiDeploymentFixture()).Build(),
		fail:   true,
	}

	c := newTestController(config.Config{}, &testutil.MockDB{}, k8sClient, nil)
	c.now = func() time.Time { return start }
	c.restartCAPI(ctx, "tenant")
	assert.Empty(t, capiRestartedAtAnnotation(t, ctx, k8sClient), "the failing patch must not have landed")

	// One second later -- far inside the window -- the retry must still roll.
	k8sClient.fail = false
	c.now = func() time.Time { return start.Add(time.Second) }
	c.restartCAPI(ctx, "tenant")
	assert.Equal(t, "2026-10-24T12:00:01Z", capiRestartedAtAnnotation(t, ctx, k8sClient))
}

// A rotation that lands inside the window must be held, not dropped: the roll
// that opened the window ran before that Secret was written, so it cannot have
// loaded it. Without the trailing roll that tenant stays on a superseded
// credential until something else happens to roll the deployment.
func TestDrainPendingCAPIRestart(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	k8sClient := fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(capiDeploymentFixture()).Build()
	c := newTestController(config.Config{}, &testutil.MockDB{}, k8sClient, nil)

	// Nothing held yet: no roll and no requeue.
	c.now = func() time.Time { return start }
	assert.Equal(t, ctrl.Result{}, c.drainPendingCAPIRestart(ctx))

	// First rotation rolls.
	c.restartCAPI(ctx, "tenant-a")
	require.Equal(t, "2026-10-24T12:00:00Z", capiRestartedAtAnnotation(t, ctx, k8sClient))

	// A second tenant rotates one minute in. It is suppressed now...
	c.now = func() time.Time { return start.Add(time.Minute) }
	c.restartCAPI(ctx, "tenant-b")
	require.Equal(t, "2026-10-24T12:00:00Z", capiRestartedAtAnnotation(t, ctx, k8sClient))

	// ...and the reconciler is asked to come back when the window closes.
	assert.Equal(t, ctrl.Result{RequeueAfter: 9 * time.Minute}, c.drainPendingCAPIRestart(ctx))

	// On that callback the held rotation rolls.
	c.now = func() time.Time { return start.Add(capiRestartMinInterval) }
	assert.Equal(t, ctrl.Result{}, c.drainPendingCAPIRestart(ctx))
	assert.Equal(t, "2026-10-24T12:10:00Z", capiRestartedAtAnnotation(t, ctx, k8sClient))

	// And it is not rolled twice.
	c.now = func() time.Time { return start.Add(capiRestartMinInterval + time.Second) }
	assert.Equal(t, ctrl.Result{}, c.drainPendingCAPIRestart(ctx))
	assert.Equal(t, "2026-10-24T12:10:00Z", capiRestartedAtAnnotation(t, ctx, k8sClient))
}

// A roll that fails must not swallow a rotation that was already being held.
func TestRestartCAPIKeepsHeldRotationWhenRollFails(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	k8sClient := &patchFailingClient{
		Client: fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(capiDeploymentFixture()).Build(),
	}
	c := newTestController(config.Config{}, &testutil.MockDB{}, k8sClient, nil)

	c.now = func() time.Time { return start }
	c.restartCAPI(ctx, "tenant-a")

	// tenant-b rotates inside the window and is held.
	c.now = func() time.Time { return start.Add(time.Minute) }
	c.restartCAPI(ctx, "tenant-b")

	// The window closes and the held roll is attempted, but the patch fails.
	// The drain must ask to come back: nothing else will, and the rotated bytes
	// are already published, so no later reconcile detects that rotation again.
	k8sClient.fail = true
	c.now = func() time.Time { return start.Add(capiRestartMinInterval) }
	assert.Equal(t, ctrl.Result{RequeueAfter: capiRestartRetryDelay}, c.drainPendingCAPIRestart(ctx))
	assert.Equal(t, "2026-10-24T12:00:00Z", capiRestartedAtAnnotation(t, ctx, k8sClient), "the failing patch must not have landed")

	// tenant-b is still held, so the next drain retries and succeeds.
	k8sClient.fail = false
	c.now = func() time.Time { return start.Add(capiRestartMinInterval + time.Second) }
	assert.Equal(t, ctrl.Result{}, c.drainPendingCAPIRestart(ctx))
	assert.Equal(t, "2026-10-24T12:10:01Z", capiRestartedAtAnnotation(t, ctx, k8sClient))
}

// The first rotation for a cluster is the one with no second chance: the Secret
// now holds the rotated bytes, so kubeconfigRotated never fires for it again. A
// roll that fails must therefore be held for retry even though nothing was
// pending before it, or one transient patch error leaves that tenant on the
// stale client until the superseded credential is actually rejected -- which is
// the page this whole path exists to prevent.
func TestRestartCAPIHoldsFirstRotationWhenRollFails(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	k8sClient := &patchFailingClient{
		Client: fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(capiDeploymentFixture()).Build(),
		fail:   true,
	}
	c := newTestController(config.Config{}, &testutil.MockDB{}, k8sClient, nil)

	// Nothing has rolled yet, so this rotation claims the window and fails.
	c.now = func() time.Time { return start }
	c.restartCAPI(ctx, "tenant")
	require.Empty(t, capiRestartedAtAnnotation(t, ctx, k8sClient), "the failing patch must not have landed")

	// The publish path's own drain retries immediately, and fails again.
	assert.Equal(t, ctrl.Result{RequeueAfter: capiRestartRetryDelay}, c.drainPendingCAPIRestart(ctx))

	// On the requeue the rotation is still held, and rolls.
	k8sClient.fail = false
	c.now = func() time.Time { return start.Add(capiRestartRetryDelay) }
	assert.Equal(t, ctrl.Result{}, c.drainPendingCAPIRestart(ctx))
	assert.Equal(t, "2026-10-24T12:00:30Z", capiRestartedAtAnnotation(t, ctx, k8sClient))

	// And the roll is not repeated once it has landed.
	c.now = func() time.Time { return start.Add(capiRestartRetryDelay + time.Second) }
	assert.Equal(t, ctrl.Result{}, c.drainPendingCAPIRestart(ctx))
	assert.Equal(t, "2026-10-24T12:00:30Z", capiRestartedAtAnnotation(t, ctx, k8sClient))
}

func TestKubeconfigRotated(t *testing.T) {
	const clusterName = "tenant"

	published := []byte("published-kubeconfig")

	const (
		server    = "https://198.51.100.10:6443"
		otherHost = "https://198.51.100.11:6443"
		ca        = "cluster-ca"
	)
	nodeA := nodeKubeconfig(server, ca, "node-a-client-cert")
	nodeB := nodeKubeconfig(server, ca, "node-b-client-cert")

	tests := []struct {
		name       string
		secretName string
		existing   []byte
		// publishedBy is the source node recorded on the existing Secret.
		publishedBy string
		kubeconfig  []byte
		// sourceNode is the node serving the kubeconfig being published.
		sourceNode string
		want       bool
	}{
		{
			name:       "different bytes in the controller secret is a rotation",
			secretName: KubeconfigSecretName(clusterName),
			existing:   []byte("superseded-kubeconfig"),
			kubeconfig: published,
			want:       true,
		},
		{
			name:       "republishing the same bytes is not a rotation",
			secretName: KubeconfigSecretName(clusterName),
			existing:   published,
			kubeconfig: published,
			want:       false,
		},
		{
			name:       "a first publish is not a rotation",
			secretName: KubeconfigSecretName(clusterName),
			existing:   nil,
			kubeconfig: published,
			want:       false,
		},
		{
			name:       "the breakglass admin secret is never a rotation",
			secretName: AdminKubeconfigSecretName(clusterName),
			existing:   []byte("superseded-kubeconfig"),
			kubeconfig: published,
			want:       false,
		},
		{
			// The spurious roll: one flaked GetKubeconfig probe, or the
			// lowest-UUID healthy candidate changing, republishes another
			// node's admin.conf. Cluster API still holds node A's credential
			// and node A's credential still works.
			name:        "a different source node alone is not a rotation",
			secretName:  KubeconfigSecretName(clusterName),
			existing:    nodeA,
			publishedBy: "node-a",
			kubeconfig:  nodeB,
			sourceNode:  "node-b",
			want:        false,
		},
		{
			name:        "the same source node with different bytes is a rotation",
			secretName:  KubeconfigSecretName(clusterName),
			existing:    nodeA,
			publishedBy: "node-a",
			kubeconfig:  nodeKubeconfig(server, ca, "node-a-rotated-cert"),
			sourceNode:  "node-a",
			want:        true,
		},
		{
			// The server and the CA belong to the cluster, not to the node
			// that served the file, so a change in either is a rotation
			// however the source node moved.
			name:        "a changed server is a rotation across source nodes",
			secretName:  KubeconfigSecretName(clusterName),
			existing:    nodeA,
			publishedBy: "node-a",
			kubeconfig:  nodeKubeconfig(otherHost, ca, "node-b-client-cert"),
			sourceNode:  "node-b",
			want:        true,
		},
		{
			name:        "a changed CA is a rotation across source nodes",
			secretName:  KubeconfigSecretName(clusterName),
			existing:    nodeA,
			publishedBy: "node-a",
			kubeconfig:  nodeKubeconfig(server, "rotated-cluster-ca", "node-b-client-cert"),
			sourceNode:  "node-b",
			want:        true,
		},
		{
			// Secrets published before the source node was recorded carry no
			// annotation. Nothing can be ruled out about them, so they keep
			// the old behavior.
			name:       "a secret with no recorded source node is a rotation",
			secretName: KubeconfigSecretName(clusterName),
			existing:   nodeA,
			kubeconfig: nodeB,
			sourceNode: "node-b",
			want:       true,
		},
		{
			name:        "an unparseable published secret is a rotation",
			secretName:  KubeconfigSecretName(clusterName),
			existing:    []byte("{not a kubeconfig"),
			publishedBy: "node-a",
			kubeconfig:  nodeB,
			sourceNode:  "node-b",
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			builder := fake.NewClientBuilder().WithScheme(capiRestartScheme(t))
			if tt.existing != nil {
				meta := metav1.ObjectMeta{Name: tt.secretName, Namespace: constants.LegacyManagementNamespace}
				if tt.publishedBy != "" {
					meta.Annotations = map[string]string{kubeconfigSourceNodeAnnotation: tt.publishedBy}
				}
				builder = builder.WithObjects(&corev1.Secret{
					ObjectMeta: meta,
					Data:       map[string][]byte{KubeconfigSecretKey: tt.existing},
				})
			}

			c := newTestController(config.Config{}, &testutil.MockDB{}, builder.Build(), nil)

			got, err := c.kubeconfigRotated(ctx, tt.secretName, tt.kubeconfig, tt.sourceNode)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The end the fix exists for: republishing a tenant kubeconfig with different
// bytes rolls Cluster API, so it rebuilds the workload-cluster client it would
// otherwise hold until the superseded credential expired.
func TestUpsertKubeconfigSecretRollsCAPIOnRotation(t *testing.T) {
	ctx := context.Background()
	cluster := db.Cluster{ID: uuid.New(), Name: "tenant"}

	mockDB := &testutil.MockDB{}
	mockDB.On("GetCluster", mock.Anything, cluster.ID).Return(cluster, nil)

	k8sClient := fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(
		capiClusterFixture(cluster),
		capiDeploymentFixture(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      KubeconfigSecretName(cluster.Name),
				Namespace: constants.LegacyManagementNamespace,
			},
			Data: map[string][]byte{KubeconfigSecretKey: []byte("superseded-kubeconfig")},
		},
	).Build()

	c := newTestController(config.Config{TenantAccessEnabled: true}, mockDB, k8sClient, nil)
	c.scheme = capiRestartScheme(t)
	c.now = func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) }

	rotated := []byte("rotated-kubeconfig")
	require.NoError(t, c.upsertKubeconfigSecret(
		ctx, cluster, KubeconfigSecretName(cluster.Name), rotated, constants.LabelValueKubeconfigModeAdmin, "node-a",
	))

	secret := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{
		Name: KubeconfigSecretName(cluster.Name), Namespace: constants.LegacyManagementNamespace,
	}, secret))
	assert.Equal(t, rotated, secret.Data[KubeconfigSecretKey])
	assert.Equal(t, "node-a", secret.Annotations[kubeconfigSourceNodeAnnotation], "the publish must record its source node")
	assert.Equal(t, "2026-10-24T12:00:00Z", capiRestartedAtAnnotation(t, ctx, k8sClient))
}

// The spurious roll this gate exists to stop: a pass that reads a different
// control-plane node republishes that node's admin.conf, which differs only in
// the client certificate. Cluster API is holding the previous node's
// credential, which still works, so rolling would drop every workload-cluster
// informer on the site for nothing.
func TestUpsertKubeconfigSecretDoesNotRollOnSourceNodeChange(t *testing.T) {
	ctx := context.Background()
	cluster := db.Cluster{ID: uuid.New(), Name: "tenant"}

	mockDB := &testutil.MockDB{}
	mockDB.On("GetCluster", mock.Anything, cluster.ID).Return(cluster, nil)

	const server = "https://198.51.100.10:6443"
	k8sClient := fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(
		capiClusterFixture(cluster),
		capiDeploymentFixture(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        KubeconfigSecretName(cluster.Name),
				Namespace:   constants.LegacyManagementNamespace,
				Annotations: map[string]string{kubeconfigSourceNodeAnnotation: "node-a"},
			},
			Data: map[string][]byte{
				KubeconfigSecretKey: nodeKubeconfig(server, "cluster-ca", "node-a-client-cert"),
			},
		},
	).Build()

	c := newTestController(config.Config{TenantAccessEnabled: true}, mockDB, k8sClient, nil)
	c.scheme = capiRestartScheme(t)

	fromNodeB := nodeKubeconfig(server, "cluster-ca", "node-b-client-cert")
	require.NoError(t, c.upsertKubeconfigSecret(
		ctx, cluster, KubeconfigSecretName(cluster.Name), fromNodeB, constants.LabelValueKubeconfigModeAdmin, "node-b",
	))

	// The Secret still tracks whichever node answered, so a later rotation on
	// node B is measured against node B.
	secret := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{
		Name: KubeconfigSecretName(cluster.Name), Namespace: constants.LegacyManagementNamespace,
	}, secret))
	assert.Equal(t, fromNodeB, secret.Data[KubeconfigSecretKey])
	assert.Equal(t, "node-b", secret.Annotations[kubeconfigSourceNodeAnnotation])

	assert.Empty(t, capiRestartedAtAnnotation(t, ctx, k8sClient), "a source-node change must not roll Cluster API")
}

// Publishing a tenant kubeconfig for the first time must not roll Cluster API:
// it has not connected to the cluster yet, and a fleet bring-up would otherwise
// roll the deployment once per cluster.
func TestUpsertKubeconfigSecretDoesNotRollOnFirstPublish(t *testing.T) {
	ctx := context.Background()
	cluster := db.Cluster{ID: uuid.New(), Name: "tenant"}

	mockDB := &testutil.MockDB{}
	mockDB.On("GetCluster", mock.Anything, cluster.ID).Return(cluster, nil)

	k8sClient := fake.NewClientBuilder().WithScheme(capiRestartScheme(t)).WithObjects(
		capiClusterFixture(cluster),
		capiDeploymentFixture(),
	).Build()

	c := newTestController(config.Config{TenantAccessEnabled: true}, mockDB, k8sClient, nil)
	c.scheme = capiRestartScheme(t)

	require.NoError(t, c.upsertKubeconfigSecret(
		ctx, cluster, KubeconfigSecretName(cluster.Name), []byte("first-kubeconfig"), constants.LabelValueKubeconfigModeAdmin, "node-a",
	))

	assert.Empty(t, capiRestartedAtAnnotation(t, ctx, k8sClient))
}

// nodeKubeconfig renders an admin.conf-shaped kubeconfig. The server and the CA
// belong to the cluster; the client certificate belongs to the control-plane
// node kubeadm issued the file on, which is what differs between two nodes of
// one healthy cluster.
func nodeKubeconfig(server, ca, clientCert string) []byte {
	encode := base64.StdEncoding.EncodeToString
	return fmt.Appendf(nil, `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: %s
    certificate-authority-data: %s
contexts:
- name: ctx
  context: {cluster: c, user: u}
current-context: ctx
users:
- name: u
  user:
    client-certificate-data: %s
`, server, encode([]byte(ca)), encode([]byte(clientCert)))
}

// patchFailingClient fails Patch on demand so the restart failure path can be
// exercised without a real apiserver.
type patchFailingClient struct {
	ctrlclient.Client
	fail bool
}

func (c *patchFailingClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	if c.fail {
		if _, ok := obj.(*appsv1.Deployment); ok {
			return assert.AnError
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
