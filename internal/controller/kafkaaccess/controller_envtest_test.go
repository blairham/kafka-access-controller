// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build envtest

package kafkaaccess

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafkav1alpha1 "github.com/blairham/kafka-controller/apis/kafka/v1alpha1"
	"github.com/blairham/kafka-controller/internal/engine/kafka"
	"github.com/blairham/kafka-controller/internal/engine/kafka/kafkatest"
)

const wait = 10 * time.Second

// clusterFactory opens a real engine over one shared in-memory cluster.
func clusterFactory(c *kafkatest.Admin) EngineFactory {
	return func(context.Context, *kafkav1alpha1.KafkaAccess) (Engine, error) {
		return kafka.New(c), nil
	}
}

func newNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "ka-"}}
	if err := k8sClient.Create(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func create(t *testing.T, ns string, mutate func(*kafkav1alpha1.KafkaAccessSpec)) *kafkav1alpha1.KafkaAccess {
	t.Helper()
	spec := validSpec()
	mutate(&spec)
	ka := &kafkav1alpha1.KafkaAccess{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: ns}, Spec: spec}
	if err := k8sClient.Create(context.Background(), ka); err != nil {
		t.Fatal(err)
	}
	return ka
}

func fetch(t *testing.T, ka *kafkav1alpha1.KafkaAccess) *kafkav1alpha1.KafkaAccess {
	t.Helper()
	var out kafkav1alpha1.KafkaAccess
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(ka), &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

// updateSpec changes the spec on a fresh read, retrying on conflict: the
// controller writes status concurrently, which bumps resourceVersion under a
// test's feet.
func updateSpec(t *testing.T, ka *kafkav1alpha1.KafkaAccess, mutate func(*kafkav1alpha1.KafkaAccessSpec)) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		got := fetch(t, ka)
		mutate(&got.Spec)
		return k8sClient.Update(context.Background(), got)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func condIs(ka *kafkav1alpha1.KafkaAccess, typ string, status metav1.ConditionStatus, reason string) bool {
	c := apimeta.FindStatusCondition(ka.Status.Conditions, typ)
	return c != nil && c.Status == status && (reason == "" || c.Reason == reason) &&
		c.ObservedGeneration == ka.Generation
}

func TestEnforceProvisionsAndConverges(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	ka := create(t, ns, func(*kafkav1alpha1.KafkaAccessSpec) {})

	eventually(t, wait, "Ready and Converged", func() bool {
		got := fetch(t, ka)
		return condIs(got, ConditionReady, metav1.ConditionTrue, "Applied") &&
			condIs(got, ConditionConverged, metav1.ConditionTrue, "Converged")
	})
	got := fetch(t, ka)
	if got.Status.LastAppliedTime == nil || got.Status.AppliedPlanHash == "" || got.Status.PendingOperations != 0 {
		t.Errorf("status = %+v", got.Status)
	}
	if got.Status.IAMPolicy != "" {
		t.Errorf("iamPolicy set under acl: %q", got.Status.IAMPolicy)
	}
	cluster.Do(func(c *kafkatest.Admin) {
		if c.TopicMap["orders"].Partitions != 6 || len(c.Granted) != 2 {
			t.Errorf("cluster = %+v / %v", c.TopicMap, c.Granted)
		}
	})
}

func TestObserveChangesNothing(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	ka := create(t, ns, func(s *kafkav1alpha1.KafkaAccessSpec) {
		s.Mode = kafkav1alpha1.ModeObserve
		s.RevokeOnDelete = true
	})

	eventually(t, wait, "Observed with 3 pending", func() bool {
		got := fetch(t, ka)
		return condIs(got, ConditionReady, metav1.ConditionTrue, "Observed") &&
			got.Status.PendingOperations == 3
	})
	got := fetch(t, ka)
	if len(got.Finalizers) != 0 {
		t.Errorf("finalizers = %v; Observe never revokes", got.Finalizers)
	}
	cluster.Do(func(c *kafkatest.Admin) {
		if len(c.TopicMap) != 0 || len(c.Granted) != 0 {
			t.Errorf("observe wrote to the cluster: %+v %v", c.TopicMap, c.Granted)
		}
	})
}

// The SCRAM rotation round trip crosses the engine, the session and status:
// the version the engine records on apply must reach status, or every
// reconcile would rotate again.
func TestSCRAMVersionReachesStatusAndRotates(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	ctx := context.Background()
	pw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-pw", Namespace: ns},
		Data:       map[string][]byte{"password": []byte("one")},
	}
	if err := k8sClient.Create(ctx, pw); err != nil {
		t.Fatal(err)
	}
	ka := create(t, ns, func(s *kafkav1alpha1.KafkaAccessSpec) {
		s.SCRAMCredential = &kafkav1alpha1.SCRAMCredential{
			PasswordSecretRef: kafkav1alpha1.LocalObjectReference{Name: "orders-pw"},
		}
	})

	eventually(t, wait, "the credential applied and recorded", func() bool {
		got := fetch(t, ka)
		return got.Status.SCRAMSecretVersion == pw.ResourceVersion &&
			condIs(got, ConditionConverged, metav1.ConditionTrue, "")
	})
	cluster.Do(func(c *kafkatest.Admin) {
		if c.Passwords["orders/SCRAM-SHA-512"] != "one" {
			t.Errorf("password = %q", c.Passwords["orders/SCRAM-SHA-512"])
		}
	})

	pw.Data["password"] = []byte("two")
	if err := k8sClient.Update(ctx, pw); err != nil {
		t.Fatal(err)
	}
	// Nothing watches the Secret; a spec change triggers the reconcile.
	updateSpec(t, ka, func(s *kafkav1alpha1.KafkaAccessSpec) { s.IdempotentWrite = true })
	eventually(t, wait, "the rotation applied and recorded", func() bool {
		return fetch(t, ka).Status.SCRAMSecretVersion == pw.ResourceVersion
	})
	cluster.Do(func(c *kafkatest.Admin) {
		if c.Passwords["orders/SCRAM-SHA-512"] != "two" || c.Upserts != 2 {
			t.Errorf("password %q after %d upserts", c.Passwords["orders/SCRAM-SHA-512"], c.Upserts)
		}
	})
}

func TestIAMAuthorizationRendersThePolicy(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	ka := create(t, ns, func(s *kafkav1alpha1.KafkaAccessSpec) {
		s.Authorization = kafkav1alpha1.AuthorizationIAM
		s.Principal = ""
		s.Cluster.MSKClusterARN = testARN
	})

	eventually(t, wait, "Ready with a policy", func() bool {
		got := fetch(t, ka)
		return condIs(got, ConditionReady, metav1.ConditionTrue, "") && got.Status.IAMPolicy != ""
	})
	pol := fetch(t, ka).Status.IAMPolicy
	for _, want := range []string{
		`"kafka-cluster:WriteData"`,
		"arn:aws:kafka:us-east-1:123456789012:topic/prod/0b1c2d3e-aaaa-bbbb-cccc-1234567890ab-7/orders",
	} {
		if !strings.Contains(pol, want) {
			t.Errorf("policy lacks %s:\n%s", want, pol)
		}
	}
	cluster.Do(func(c *kafkatest.Admin) {
		if len(c.Granted) != 0 || len(c.TopicMap) != 1 {
			t.Errorf("iam: ACLs %v, topics %v; want no ACLs and the topic", c.Granted, c.TopicMap)
		}
	})
}

func TestRevokeOnDeleteRemovesTheACLs(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	ka := create(t, ns, func(s *kafkav1alpha1.KafkaAccessSpec) { s.RevokeOnDelete = true })

	eventually(t, wait, "applied with the finalizer", func() bool {
		got := fetch(t, ka)
		return condIs(got, ConditionReady, metav1.ConditionTrue, "Applied") && len(got.Finalizers) == 1
	})
	if err := k8sClient.Delete(context.Background(), fetch(t, ka)); err != nil {
		t.Fatal(err)
	}
	eventually(t, wait, "the resource gone", func() bool {
		var out kafkav1alpha1.KafkaAccess
		return apierrors.IsNotFound(k8sClient.Get(context.Background(), client.ObjectKeyFromObject(ka), &out))
	})
	cluster.Do(func(c *kafkatest.Admin) {
		if len(c.Granted) != 0 {
			t.Errorf("ACLs left after revoke: %v", c.Granted)
		}
		if _, ok := c.TopicMap["orders"]; !ok {
			t.Error("revoke deleted the topic")
		}
	})
}

func TestMissingPasswordSecretFailsBeforeConnecting(t *testing.T) {
	var opened atomic.Bool
	ns := newNamespace(t)
	startManager(t, ns, func(context.Context, *kafkav1alpha1.KafkaAccess) (Engine, error) {
		opened.Store(true)
		return kafka.New(kafkatest.New()), nil
	})
	ka := create(t, ns, func(s *kafkav1alpha1.KafkaAccessSpec) {
		s.SCRAMCredential = &kafkav1alpha1.SCRAMCredential{
			PasswordSecretRef: kafkav1alpha1.LocalObjectReference{Name: "absent"},
		}
	})
	eventually(t, wait, "ConnectionFailed", func() bool {
		return condIs(fetch(t, ka), ConditionReady, metav1.ConditionFalse, "ConnectionFailed")
	})
	if opened.Load() {
		t.Error("an engine was opened before the Secret was resolved")
	}
}

// Dropping a topic from the spec deletes the ACLs this resource created for
// it, and only those.
func TestRemovedTopicACLsArePruned(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	foreign := kafka.ACL{
		Principal: "User:orders",
		Host:      "*",
		Resource:  kafka.ResourceTopic,
		Name:      "audit",
		Operation: "READ",
	}
	cluster.Do(func(c *kafkatest.Admin) { c.Granted[foreign] = true })

	ka := create(t, ns, func(s *kafkav1alpha1.KafkaAccessSpec) {
		s.Topics = append(s.Topics, kafkav1alpha1.Topic{Name: "payments", Operations: []kafkav1alpha1.Operation{"Read"}})
	})
	eventually(t, wait, "three ACLs owned", func() bool { return len(fetch(t, ka).Status.OwnedACLs) == 3 })

	updateSpec(t, ka, func(s *kafkav1alpha1.KafkaAccessSpec) { s.Topics = s.Topics[:1] })
	eventually(t, wait, "the payments ACL pruned", func() bool {
		got := fetch(t, ka)
		return len(got.Status.OwnedACLs) == 2 && condIs(got, ConditionConverged, metav1.ConditionTrue, "")
	})
	cluster.Do(func(c *kafkatest.Admin) {
		if len(c.Granted) != 3 || !c.Granted[foreign] {
			t.Errorf("ACLs = %v; want orders' two plus the foreign one", c.Granted)
		}
	})
}

// Moving a resource to MSK IAM authorization withdraws its ACLs.
func TestSwitchingToIAMPrunesTheACLs(t *testing.T) {
	cluster := kafkatest.New()
	ns := newNamespace(t)
	startManager(t, ns, clusterFactory(cluster))
	ka := create(t, ns, func(*kafkav1alpha1.KafkaAccessSpec) {})
	eventually(t, wait, "ACLs owned", func() bool { return len(fetch(t, ka).Status.OwnedACLs) == 2 })

	updateSpec(t, ka, func(s *kafkav1alpha1.KafkaAccessSpec) {
		s.Authorization = kafkav1alpha1.AuthorizationIAM
		s.Cluster.MSKClusterARN = testARN
	})
	eventually(t, wait, "inventory empty and a policy rendered", func() bool {
		got := fetch(t, ka)
		return len(got.Status.OwnedACLs) == 0 && got.Status.IAMPolicy != ""
	})
	cluster.Do(func(c *kafkatest.Admin) {
		if len(c.Granted) != 0 {
			t.Errorf("ACLs left under iam: %v", c.Granted)
		}
	})
}
