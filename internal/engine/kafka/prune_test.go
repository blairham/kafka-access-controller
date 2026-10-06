// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka_test

import (
	"context"
	"slices"
	"testing"

	"github.com/blairham/kafka-controller/internal/engine/kafka"
	"github.com/blairham/kafka-controller/internal/engine/kafka/kafkatest"
)

func TestACLKeyRoundTrips(t *testing.T) {
	t.Parallel()
	for _, acl := range []kafka.ACL{
		{Principal: "User:orders", Host: "*", Resource: kafka.ResourceTopic, Name: "orders.v1", Operation: "READ"},
		// An mTLS principal: commas, spaces, an equals sign and a slash.
		{Principal: "User:CN=Orders Svc,OU=a/b,O=Acme", Host: "10.0.0.1", Resource: kafka.ResourceGroup, Name: "g 1/x", Prefixed: true, Operation: "DESCRIBE"},
		{Principal: "User:x", Host: "*", Resource: kafka.ResourceCluster, Name: "kafka-cluster", Operation: "IDEMPOTENT_WRITE"},
	} {
		got, err := kafka.ParseACLKey(acl.Key())
		if err != nil || got != acl {
			t.Errorf("round trip of %+v = %+v, %v (key %q)", acl, got, err, acl.Key())
		}
	}
	for _, bad := range []string{"", "topic/x", "acl/a/b/c", "acl/User:x/*/TOPIC/FUZZY/t/READ", "acl/%zz/*/TOPIC/LITERAL/t/READ"} {
		if _, err := kafka.ParseACLKey(bad); err == nil {
			t.Errorf("ParseACLKey(%q) accepted", bad)
		}
	}
}

func TestOwnedKeys(t *testing.T) {
	t.Parallel()
	a := orders()
	if got := kafka.OwnedKeys(a); len(got) != len(kafka.DesiredACLs(a)) {
		t.Errorf("acl: %d keys for %d ACLs", len(got), len(kafka.DesiredACLs(a)))
	}
	a.Authorization = kafka.AuthorizationIAM
	if got := kafka.OwnedKeys(a); len(got) != 0 {
		t.Errorf("iam owns %v; it declares no ACLs", got)
	}
}

func TestPruneDeletesOnlyHeldKeys(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	held := kafka.ACL{Principal: "User:old", Host: "*", Resource: kafka.ResourceTopic, Name: "t", Operation: "READ"}
	gone := kafka.ACL{Principal: "User:old", Host: "*", Resource: kafka.ResourceTopic, Name: "t", Operation: "WRITE"}
	f.Granted[held] = true

	steps, err := e.Prune(context.Background(), []string{held.Key(), gone.Key(), "not-a-key"})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].Key() != held.Key() || steps[0].Describe() != "DELETE ACL "+held.String() {
		t.Fatalf("steps = %v", steps)
	}
	if !steps[0].IsBestEffort() {
		t.Error("a prune step must be best-effort, so one refusal cannot block the rest")
	}
	if err := steps[0].Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.Granted) != 0 {
		t.Errorf("left %v", f.Granted)
	}
	// Re-asking after the delete is how the reconciler learns the key is gone.
	if again, _ := e.Prune(context.Background(), []string{held.Key()}); len(again) != 0 {
		t.Errorf("re-prune = %v", again)
	}
}

// A principal renamed in the spec leaves the old principal's ACLs as stale
// keys; Prune must look them up under the old principal.
func TestPruneFindsAnOldPrincipal(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	a := orders()
	apply(t, e, a)
	old := kafka.OwnedKeys(a)

	a.Principal = "User:orders-v2"
	apply(t, e, a)
	stale := slices.DeleteFunc(slices.Clone(old), func(k string) bool { return slices.Contains(kafka.OwnedKeys(a), k) })
	steps, err := e.Prune(context.Background(), stale)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != len(old) {
		t.Errorf("%d prune steps for %d old ACLs", len(steps), len(old))
	}
}
