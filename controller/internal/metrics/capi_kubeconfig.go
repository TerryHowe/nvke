// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//nolint:gochecknoglobals // Prometheus metrics are conventionally package-level globals
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Results recorded by RecordCAPIKubeconfigRestart.
const (
	// CAPIRestartResultRolled is a pod-template annotation written, so Cluster
	// API rebuilds its workload-cluster clients.
	CAPIRestartResultRolled = "rolled"

	// CAPIRestartResultAbsent is no Cluster API core deployment on this
	// management cluster -- a local or Tilt-driven run, not a failure.
	CAPIRestartResultAbsent = "absent"

	// CAPIRestartResultError is a roll that was needed and did not happen. The
	// rotated kubeconfig is published but Cluster API is still on the old one.
	CAPIRestartResultError = "error"
)

// capiKubeconfigRestartsTotal counts only the rolls that were attempted, not
// the rotations that triggered them: rolls inside the coalescing window are
// deliberately suppressed, so this is the signal for "how often does Cluster
// API have to be bounced on this site", which is the question a run of
// APIServerClientCertificateExpirySoon pages asks.
var capiKubeconfigRestartsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "nvke_controller_capi_kubeconfig_restarts_total",
		Help: "Rolls of capi-controller-manager triggered by a rotated tenant kubeconfig Secret, by result. " +
			"Cluster API reads a kubeconfig Secret once per connection and caches the client for the life of " +
			"its pod, so a rotation does not take effect until the deployment is rolled.",
	},
	[]string{"result"},
)

func init() {
	prometheus.MustRegister(capiKubeconfigRestartsTotal)
}

// RecordCAPIKubeconfigRestart counts one attempt to roll Cluster API after a
// tenant kubeconfig rotation.
func RecordCAPIKubeconfigRestart(result string) {
	capiKubeconfigRestartsTotal.WithLabelValues(result).Inc()
}
