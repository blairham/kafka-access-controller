// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package kafkaaccess reconciles KafkaAccess resources against a Kafka
// cluster's data plane.
package kafkaaccess

import (
	"context"

	"github.com/blairham/k8s-controller-kit/plan"
	"github.com/blairham/k8s-controller-kit/reconciler"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kafkav1alpha1 "github.com/blairham/kafka-controller/apis/kafka/v1alpha1"
	"github.com/blairham/kafka-controller/internal/engine/kafka"
	"github.com/blairham/kafka-controller/internal/mskiam"
)

// finalizer keeps the resource around long enough to revoke, and is only
// added when the spec asks for revocation.
const finalizer = "kafka-controller.io/revoke-on-delete"

// Condition types, re-exported for callers that only import this package.
const (
	ConditionReady     = reconciler.ConditionReady
	ConditionConverged = reconciler.ConditionConverged
)

// +kubebuilder:rbac:groups=kafka-controller.io,resources=kafkaaccesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka-controller.io,resources=kafkaaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kafka-controller.io,resources=kafkaaccesses/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Config wires the reconciler.
type Config struct {
	Client   client.Client
	Recorder record.EventRecorder

	// NewEngine opens the engine for a resource.
	NewEngine EngineFactory

	// Registry receives the per-resource metrics; nil disables them.
	Registry prometheus.Registerer

	// Options are passed to the underlying controller.
	Options controller.Options
}

// session is one resource's engine and its resolved access. The access is
// held by value but its SCRAM field is a pointer the engine updates on a
// successful rotation, which is how the re-plan and status learn of it.
type session struct {
	eng    Engine
	access kafka.Access
}

func (s *session) Plan(ctx context.Context) (*plan.Plan, error) {
	return s.eng.BuildPlan(ctx, s.access)
}

func (s *session) RevokePlan(ctx context.Context) (*plan.Plan, error) {
	return s.eng.BuildRevokePlan(ctx, s.access)
}

func (s *session) Close() error { return s.eng.Close() }

// Owned implements reconciler.Pruner: the ACLs the spec declares.
func (s *session) Owned(context.Context) ([]string, error) {
	return kafka.OwnedKeys(s.access), nil
}

// Prune implements reconciler.Pruner.
func (s *session) Prune(ctx context.Context, stale []string) ([]reconciler.PruneStep, error) {
	steps, err := s.eng.Prune(ctx, stale)
	if err != nil {
		return nil, err
	}
	out := make([]reconciler.PruneStep, len(steps))
	for i, st := range steps {
		out[i] = st
	}
	return out, nil
}

// New returns the KafkaAccess reconciler.
func New(cfg Config) *reconciler.Reconciler[*kafkav1alpha1.KafkaAccess] {
	var metrics *reconciler.Metrics
	if cfg.Registry != nil {
		metrics = reconciler.NewMetrics(cfg.Registry, reconciler.MetricsConfig{
			Prefix: "kafka_controller", Kind: "KafkaAccess", Noun: "operation",
		})
	}
	return &reconciler.Reconciler[*kafkav1alpha1.KafkaAccess]{
		Client:   cfg.Client,
		Recorder: cfg.Recorder,
		Name:     "kafkaaccess",
		New:      func() *kafkav1alpha1.KafkaAccess { return &kafkav1alpha1.KafkaAccess{} },
		Open: func(ctx context.Context, ka *kafkav1alpha1.KafkaAccess) (reconciler.Session, error) {
			// Resolve the access first: a missing password Secret should fail
			// before a connection is opened.
			a, err := resolveAccess(ctx, cfg.Client, ka)
			if err != nil {
				return nil, err
			}
			eng, err := cfg.NewEngine(ctx, ka)
			if err != nil {
				return nil, err
			}
			return &session{eng: eng, access: a}, nil
		},
		Observing:      func(ka *kafkav1alpha1.KafkaAccess) bool { return ka.Spec.Mode == kafkav1alpha1.ModeObserve },
		RevokeOnDelete: func(ka *kafkav1alpha1.KafkaAccess) bool { return ka.Spec.RevokeOnDelete },
		StatusOf: func(ka *kafkav1alpha1.KafkaAccess) reconciler.Status {
			st := &ka.Status
			return reconciler.Status{
				Conditions:         &st.Conditions,
				ObservedGeneration: &st.ObservedGeneration,
				AppliedPlanHash:    &st.AppliedPlanHash,
				LastAppliedTime:    &st.LastAppliedTime,
				Applied:            &st.OperationsApplied,
				Warnings:           &st.Warnings,
				PendingCount:       &st.PendingOperations,
				Pending:            &st.Pending,
				LastPlannedTime:    &st.LastPlannedTime,
				Owned:              &st.OwnedACLs,
			}
		},
		BeforeStatusUpdate: func(ctx context.Context, ka *kafkav1alpha1.KafkaAccess, s reconciler.Session) {
			ss, ok := s.(*session)
			if !ok {
				return
			}
			recordExtras(ctx, cfg.Recorder, ka, ss.access)
		},
		Finalizer: finalizer,
		Metrics:   metrics,
		Noun:      "operation",
		Target:    "cluster",
		Options:   cfg.Options,
	}
}

// recordExtras writes the status this domain adds: the SCRAM Secret version
// last applied, and the IAM policy under authorization iam.
func recordExtras(ctx context.Context, rec record.EventRecorder, ka *kafkav1alpha1.KafkaAccess, a kafka.Access) {
	if a.SCRAM != nil {
		ka.Status.SCRAMSecretVersion = a.SCRAM.AppliedVersion
	}
	ka.Status.IAMPolicy = ""
	if a.Authorization != kafka.AuthorizationIAM {
		return
	}
	pol, err := mskiam.Render(ka.Spec.Cluster.MSKClusterARN, a)
	if err != nil {
		// Admission validates the ARN, so this is a resource admitted before
		// the pattern existed; say so rather than failing the topics.
		log.FromContext(ctx).Error(err, "rendering IAM policy")
		rec.Event(ka, "Warning", "IAMPolicyFailed", err.Error())
		return
	}
	ka.Status.IAMPolicy = pol
}
