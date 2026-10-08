// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command manager runs the controllers; --controllers selects which.
package main

import (
	"fmt"
	"os"

	"github.com/blairham/k8s-controller-kit/manager"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	kafkav1alpha1 "github.com/blairham/kafka-access-controller/apis/kafka/v1alpha1"
	"github.com/blairham/kafka-access-controller/internal/controller/kafkaaccess"
)

func main() {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kafkav1alpha1.AddToScheme(scheme))

	err := manager.Main(manager.Config{
		Scheme:           scheme,
		LeaderElectionID: "kafka-access-controller.io",
		Controllers: map[string]manager.Controller{
			"kafkaaccess": {
				Watches: &kafkav1alpha1.KafkaAccess{},
				Setup: func(mgr ctrl.Manager) error {
					// The kit's Reconciler takes the old-API recorder until
					// blairham/k8s-controller-kit#6.
					recorder := mgr.GetEventRecorderFor("kafkaaccess") //nolint:staticcheck // kit#6
					return kafkaaccess.New(kafkaaccess.Config{
						Client:    mgr.GetClient(),
						Recorder:  recorder,
						NewEngine: kafkaaccess.NewEngineFactory(mgr.GetClient()),
						Registry:  metrics.Registry,
					}).SetupWithManager(mgr)
				},
			},
		},
	}, os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "manager: %v\n", err)
		os.Exit(1)
	}
}
