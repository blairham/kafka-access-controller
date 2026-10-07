// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/blairham/k8s-controller-kit/reconciler"

	"github.com/blairham/kafka-controller/internal/engine/kafka"
	"github.com/blairham/kafka-controller/internal/engine/kafka/kafkatest"
)

func pending(t *testing.T, e *kafka.Engine, a kafka.Access) []string {
	t.Helper()
	p, err := e.BuildPlan(context.Background(), a)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return reconciler.PendingOf(p)
}

func apply(t *testing.T, e *kafka.Engine, a kafka.Access) {
	t.Helper()
	p, err := e.BuildPlan(context.Background(), a)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if _, err := p.Apply(context.Background()); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func i32(v int32) int32 { return v }

func orders() kafka.Access {
	return kafka.Access{
		Principal:     "User:orders",
		Authorization: kafka.AuthorizationACL,
		Topics: []kafka.Topic{
			{
				Name: "orders", Manage: true, Partitions: i32(6), ReplicationFactor: 3,
				Config:     map[string]string{"retention.ms": "604800000"},
				Operations: []string{"Read", "Write"},
			},
			{Name: "payments.", Prefixed: true, Operations: []string{"Read"}},
		},
		Groups:           []kafka.Grant{{Name: "orders-svc", Operations: []string{"Read"}}},
		TransactionalIDs: []kafka.Grant{{Name: "orders-", Prefixed: true, Operations: []string{"Write", "Describe"}}},
		IdempotentWrite:  true,
	}
}

func TestFreshClusterPlan(t *testing.T) {
	t.Parallel()
	e := kafka.New(kafkatest.New())
	got := pending(t, e, orders())
	want := []string{
		"CREATE TOPIC orders PARTITIONS 6 REPLICATION-FACTOR 3 CONFIG retention.ms=604800000",
		"CREATE ACL User:orders ALLOW READ ON TOPIC orders LITERAL HOST *",
		"CREATE ACL User:orders ALLOW WRITE ON TOPIC orders LITERAL HOST *",
		"CREATE ACL User:orders ALLOW READ ON TOPIC payments. PREFIXED HOST *",
		"CREATE ACL User:orders ALLOW READ ON GROUP orders-svc LITERAL HOST *",
		"CREATE ACL User:orders ALLOW WRITE ON TRANSACTIONAL_ID orders- PREFIXED HOST *",
		"CREATE ACL User:orders ALLOW DESCRIBE ON TRANSACTIONAL_ID orders- PREFIXED HOST *",
		"CREATE ACL User:orders ALLOW IDEMPOTENT_WRITE ON CLUSTER kafka-cluster LITERAL HOST *",
	}
	if !slices.Equal(got, want) {
		t.Errorf("plan =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestAppliedPlanConverges(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	apply(t, e, orders())
	if got := pending(t, e, orders()); len(got) != 0 {
		t.Errorf("converged cluster still plans:\n%s", strings.Join(got, "\n"))
	}
}

func TestDriftIsRepairedExactly(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	apply(t, e, orders())

	// Out-of-band changes: an ACL deleted, a config changed.
	delete(f.Granted, kafka.ACL{
		Principal: "User:orders", Host: "*", Resource: kafka.ResourceGroup,
		Name: "orders-svc", Operation: "READ",
	})
	st := f.TopicMap["orders"]
	st.Config["retention.ms"] = "1"
	f.TopicMap["orders"] = st

	got := pending(t, e, orders())
	want := []string{
		"ALTER TOPIC orders SET retention.ms=604800000",
		"CREATE ACL User:orders ALLOW READ ON GROUP orders-svc LITERAL HOST *",
	}
	if !slices.Equal(got, want) {
		t.Errorf("repair plan = %q, want %q", got, want)
	}
}

func TestACLsAreAdditive(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	extra := kafka.ACL{
		Principal: "User:orders",
		Host:      "*",
		Resource:  kafka.ResourceTopic,
		Name:      "audit",
		Operation: "READ",
	}
	f.Granted[extra] = true
	apply(t, e, orders())
	if !f.Granted[extra] {
		t.Error("an ACL outside the spec was removed; another resource may own it")
	}
}

func TestPartitionsGrowButNeverShrink(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	apply(t, e, orders())

	a := orders()
	a.Topics[0].Partitions = i32(12)
	if got := pending(t, e, a); !slices.Equal(got, []string{"ALTER TOPIC orders PARTITIONS 6 -> 12"}) {
		t.Errorf("grow plan = %q", got)
	}
	apply(t, e, a)

	a.Topics[0].Partitions = i32(3)
	p, err := e.BuildPlan(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if got := reconciler.PendingOf(p); !slices.Equal(got, []string{"KEEP TOPIC orders PARTITIONS 12 (spec asks for 3)"}) {
		t.Errorf("shrink plan = %q", got)
	}
	res, err := p.Apply(context.Background())
	if err != nil {
		t.Fatalf("a refusal must be a warning, not fatal: %v", err)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "cannot remove partitions") {
		t.Errorf("warnings = %q", res.Warnings)
	}
	if f.TopicMap["orders"].Partitions != 12 {
		t.Errorf("partitions = %d, want untouched 12", f.TopicMap["orders"].Partitions)
	}
}

func TestReplicationFactorChangeIsRefused(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	apply(t, e, orders())
	a := orders()
	a.Topics[0].ReplicationFactor = 2
	got := pending(t, e, a)
	if len(got) != 1 || !strings.HasPrefix(got[0], "KEEP TOPIC orders REPLICATION-FACTOR 3") {
		t.Errorf("plan = %q", got)
	}
}

func TestUnmanagedTopicIsNeverCreated(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	a := orders()
	a.Topics[0].Manage = false
	apply(t, e, a)
	if _, ok := f.TopicMap["orders"]; ok {
		t.Error("an unmanaged topic was created")
	}
}

func TestIAMAuthorizationPlansNoACLs(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	a := orders()
	a.Authorization = kafka.AuthorizationIAM
	a.Principal = ""
	got := pending(t, e, a)
	if len(got) != 1 || !strings.HasPrefix(got[0], "CREATE TOPIC orders") {
		t.Errorf("iam plan = %q, want only the topic", got)
	}
	p, err := e.BuildRevokePlan(context.Background(), a)
	if err != nil || p.Len() != 0 {
		t.Errorf("iam revoke plan has %d steps, err %v", p.Len(), err)
	}
}

// A broker with no authorizer refuses every ACL call, so a resource that only
// manages topics must converge without making one.
func TestTopicsOnlyNeverReadsACLs(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	f.NoAuthorizer = true
	e := kafka.New(f)
	a := kafka.Access{
		Principal:     "User:rig",
		Authorization: kafka.AuthorizationACL,
		Topics:        []kafka.Topic{{Name: "book", Manage: true, Partitions: i32(6), ReplicationFactor: 1}},
	}
	apply(t, e, a)
	if got := pending(t, e, a); len(got) != 0 {
		t.Errorf("converged plan = %q, want empty", got)
	}
	if f.TopicMap["book"].Partitions != 6 {
		t.Errorf("topic = %+v, want 6 partitions", f.TopicMap["book"])
	}
	p, err := e.BuildRevokePlan(context.Background(), a)
	if err != nil || p.Len() != 0 {
		t.Errorf("revoke plan has %d steps, err %v", p.Len(), err)
	}
}

func TestRevokeDeletesOnlyDeclaredACLs(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	extra := kafka.ACL{
		Principal: "User:orders",
		Host:      "*",
		Resource:  kafka.ResourceTopic,
		Name:      "audit",
		Operation: "READ",
	}
	f.Granted[extra] = true
	apply(t, e, orders())

	p, err := e.BuildRevokePlan(context.Background(), orders())
	if err != nil {
		t.Fatal(err)
	}
	if p.Len() != 7 {
		t.Errorf("revoke plan has %d steps, want the 7 declared ACLs", p.Len())
	}
	if _, err := p.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.Granted) != 1 || !f.Granted[extra] {
		t.Errorf("ACLs left = %v, want only the undeclared one", f.Granted)
	}
	if _, ok := f.TopicMap["orders"]; !ok {
		t.Error("revoke deleted a topic")
	}
}

func TestSCRAMCreateAndRotate(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	e := kafka.New(f)
	a := kafka.Access{
		Principal:     "User:orders",
		Authorization: kafka.AuthorizationACL,
		SCRAM:         &kafka.SCRAM{Mechanism: "SCRAM-SHA-512", Password: "one", Version: "10"},
	}
	if got := pending(t, e, a); !slices.Equal(got, []string{"CREATE SCRAM-SHA-512 CREDENTIAL FOR orders"}) {
		t.Fatalf("plan = %q", got)
	}
	apply(t, e, a)
	if a.SCRAM.AppliedVersion != "10" || f.Passwords["orders/SCRAM-SHA-512"] != "one" {
		t.Fatalf("applied version %q, password %q", a.SCRAM.AppliedVersion, f.Passwords["orders/SCRAM-SHA-512"])
	}
	// The re-plan after applying must see the credential done.
	if got := pending(t, e, a); len(got) != 0 {
		t.Errorf("re-plan after create = %q", got)
	}

	// The Secret changed.
	a.SCRAM.Password, a.SCRAM.Version = "two", "11"
	if got := pending(t, e, a); !slices.Equal(got, []string{"ROTATE SCRAM-SHA-512 CREDENTIAL FOR orders"}) {
		t.Fatalf("plan = %q", got)
	}
	apply(t, e, a)
	if f.Passwords["orders/SCRAM-SHA-512"] != "two" || f.Upserts != 2 {
		t.Errorf("password %q after %d upserts", f.Passwords["orders/SCRAM-SHA-512"], f.Upserts)
	}
	// The password never reaches plan text.
	p, _ := e.BuildPlan(context.Background(), kafka.Access{
		Principal: "User:orders", Authorization: kafka.AuthorizationACL,
		SCRAM: &kafka.SCRAM{Mechanism: "SCRAM-SHA-256", Password: "s3cret", Version: "1"},
	})
	if strings.Contains(p.Describe(), "s3cret") {
		t.Error("plan text contains the password")
	}
}

func TestFatalACLFailureStopsThePlan(t *testing.T) {
	t.Parallel()
	f := kafkatest.New()
	f.FailACLs = true
	p, err := kafka.New(f).BuildPlan(context.Background(), orders())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Apply(
		context.Background(),
	); err == nil ||
		!strings.Contains(err.Error(), "CLUSTER_AUTHORIZATION_FAILED") {
		t.Errorf("err = %v", err)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mutate func(*kafka.Access)
		want   string
	}{
		"principal type":      {func(a *kafka.Access) { a.Principal = "orders" }, "must be User:<name>"},
		"empty principal":     {func(a *kafka.Access) { a.Principal = "User:" }, "must be User:<name>"},
		"prefixed managed":    {func(a *kafka.Access) { a.Topics[1].Manage, a.Topics[1].Partitions = true, 1 }, "prefixed topic cannot be managed"},
		"managed partitions":  {func(a *kafka.Access) { a.Topics[0].Partitions = 0 }, "needs partitions"},
		"group op":            {func(a *kafka.Access) { a.Groups[0].Operations = []string{"Write"} }, `operation "Write" does not apply`},
		"duplicate topic":     {func(a *kafka.Access) { a.Topics = append(a.Topics, a.Topics[0]) }, "listed twice"},
		"scram under iam":     {func(a *kafka.Access) { a.Authorization = kafka.AuthorizationIAM; a.SCRAM = &kafka.SCRAM{} }, "does not apply under authorization iam"},
		"unknown scram mech":  {func(a *kafka.Access) { a.SCRAM = &kafka.SCRAM{Mechanism: "MD5", Password: "x"} }, "unknown SCRAM mechanism"},
		"unknown authz model": {func(a *kafka.Access) { a.Authorization = "rbac" }, `unknown authorization "rbac"`},
		// A literal * is every resource to a broker and to IAM.
		"wildcard group": {func(a *kafka.Access) { a.Groups[0].Name = "*" }, "read as wildcards"},
		"wildcard txn":   {func(a *kafka.Access) { a.TransactionalIDs[0].Name = "orders-?" }, "read as wildcards"},
	} {
		a := orders()
		tc.mutate(&a)
		if err := kafka.Validate(a); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
	if err := kafka.Validate(orders()); err != nil {
		t.Errorf("valid access rejected: %v", err)
	}
}
