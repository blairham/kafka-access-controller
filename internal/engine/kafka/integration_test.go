// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build integration

// Integration tests run the engine against a real Kafka (make kafka-up): the
// only way to know the admin calls are spelled the way a broker answers them,
// and that what they grant actually authorizes.
package kafka_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blairham/k8s-controller-kit/reconciler"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/blairham/kafka-access-controller/internal/engine/kafka"
)

func bootstrap(t *testing.T) string {
	t.Helper()
	b := os.Getenv("KAFKA_TEST_BOOTSTRAP")
	if b == "" {
		t.Skip("KAFKA_TEST_BOOTSTRAP is unset; run make test-integration")
	}
	return b
}

// scramBootstrap is the SASL/SCRAM listener next to the admin one.
func scramBootstrap(admin string) string {
	host, _, _ := strings.Cut(admin, ":")
	return host + ":19093"
}

func suffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func connect(t *testing.T) *kafka.Engine {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kafka.Connect(ctx, kafka.ConnConfig{Brokers: []string{bootstrap(t)}, Auth: kafka.AuthNone})
	if err != nil {
		t.Fatal(err)
	}
	e := kafka.New(kafka.NewKadmAdmin(cl))
	t.Cleanup(func() { e.Close() }) //nolint:errcheck // test teardown
	return e
}

func plan(t *testing.T, e *kafka.Engine, a kafka.Access) []string {
	t.Helper()
	p, err := e.BuildPlan(context.Background(), a)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return reconciler.PendingOf(p)
}

func applyAll(t *testing.T, e *kafka.Engine, a kafka.Access) {
	t.Helper()
	p, err := e.BuildPlan(context.Background(), a)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	res, err := p.Apply(context.Background())
	if err != nil {
		t.Fatalf("Apply: %v\nplan:\n%s", err, p.Describe())
	}
	if len(res.Warnings) > 0 {
		t.Fatalf("warnings: %v", res.Warnings)
	}
}

// eventuallyConverged re-plans until nothing is pending: topic metadata and
// ACLs propagate through the KRaft metadata log, not synchronously.
func eventuallyConverged(t *testing.T, e *kafka.Engine, a kafka.Access) {
	t.Helper()
	var last []string
	for range 50 {
		if last = plan(t, e, a); len(last) == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("never converged; still pending:\n%s", strings.Join(last, "\n"))
}

func serviceClient(t *testing.T, user, pw string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(scramBootstrap(bootstrap(t))),
		kgo.SASL(scram.Auth{User: user, Pass: pw}.AsSha512Mechanism()),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produce(cl *kgo.Client, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr()
}

func TestIntegrationProvisionConvergeAuthorizeRevoke(t *testing.T) {
	e := connect(t)
	id := suffix(t)
	user := "svc-" + id
	topic := "orders-" + id
	a := kafka.Access{
		Principal:     "User:" + user,
		Authorization: kafka.AuthorizationACL,
		Topics: []kafka.Topic{
			{
				Name: topic, Manage: true, Partitions: 3,
				Config:     map[string]string{"retention.ms": "3600000", "cleanup.policy": "delete"},
				Operations: []string{"Read", "Write", "Describe"},
			},
			{Name: "shared-" + id + ".", Prefixed: true, Operations: []string{"Read"}},
		},
		Groups:           []kafka.Grant{{Name: "g-" + id, Operations: []string{"Read"}}},
		TransactionalIDs: []kafka.Grant{{Name: "tx-" + id, Prefixed: true, Operations: []string{"Write", "Describe"}}},
		IdempotentWrite:  true,
		SCRAM:            &kafka.SCRAM{Mechanism: "SCRAM-SHA-512", Password: "pw-" + id, Version: "1"},
	}

	first := plan(t, e, a)
	if len(first) != 10 || !strings.HasPrefix(first[0], "CREATE SCRAM-SHA-512 CREDENTIAL FOR "+user) {
		t.Fatalf("first plan (%d):\n%s", len(first), strings.Join(first, "\n"))
	}
	applyAll(t, e, a)
	// The round trip that matters: what was written reads back as present,
	// through the broker's own spelling of every enum.
	eventuallyConverged(t, e, a)

	t.Run("the service can use what it was granted", func(t *testing.T) {
		cl := serviceClient(t, user, "pw-"+id)
		if err := produce(cl, topic); err != nil {
			t.Fatalf("produce to granted topic: %v", err)
		}
	})

	t.Run("and only that", func(t *testing.T) {
		cl := serviceClient(t, user, "pw-"+id)
		err := produce(cl, "not-granted-"+id)
		if !errors.Is(err, kerr.TopicAuthorizationFailed) {
			t.Fatalf("produce to ungranted topic: err = %v, want TOPIC_AUTHORIZATION_FAILED", err)
		}
	})

	t.Run("drift is repaired exactly", func(t *testing.T) {
		admin, err := kafka.Connect(
			context.Background(),
			kafka.ConnConfig{Brokers: []string{bootstrap(t)}, Auth: kafka.AuthNone},
		)
		if err != nil {
			t.Fatal(err)
		}
		raw := kafka.NewKadmAdmin(admin)
		defer raw.Close()
		gone := kafka.ACL{
			Principal: a.Principal,
			Host:      "*",
			Resource:  kafka.ResourceGroup,
			Name:      "g-" + id,
			Operation: "READ",
		}
		if err := raw.DeleteACL(context.Background(), gone); err != nil {
			t.Fatal(err)
		}
		if err := raw.SetTopicConfig(context.Background(), topic, map[string]string{"retention.ms": "1000"}); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"ALTER TOPIC " + topic + " SET retention.ms=3600000",
			"CREATE ACL " + gone.String(),
		}
		var got []string
		for range 50 {
			if got = plan(t, e, a); slices.Equal(got, want) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("repair plan =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		applyAll(t, e, a)
		eventuallyConverged(t, e, a)
	})

	t.Run("an operation dropped from the spec is pruned", func(t *testing.T) {
		narrowed := a
		narrowed.Topics = slices.Clone(a.Topics)
		narrowed.Topics[0].Operations = []string{"Read", "Describe"}
		writeKey := kafka.ACL{
			Principal: a.Principal,
			Host:      "*",
			Resource:  kafka.ResourceTopic,
			Name:      topic,
			Operation: "WRITE",
		}.Key()
		steps, err := e.Prune(context.Background(), []string{writeKey})
		if err != nil || len(steps) != 1 {
			t.Fatalf("prune steps = %v, %v", steps, err)
		}
		if err := steps[0].Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		cl := serviceClient(t, user, "pw-"+id)
		var perr error
		for range 25 {
			if perr = produce(cl, topic); errors.Is(perr, kerr.TopicAuthorizationFailed) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !errors.Is(perr, kerr.TopicAuthorizationFailed) {
			t.Fatalf("produce after pruning WRITE: err = %v, want TOPIC_AUTHORIZATION_FAILED", perr)
		}
		eventuallyConverged(t, e, narrowed)
		applyAll(t, e, a) // restore WRITE for the subtests that follow
		eventuallyConverged(t, e, a)
	})

	t.Run("partitions grow and never shrink", func(t *testing.T) {
		grown := a
		grown.Topics = slices.Clone(a.Topics)
		grown.Topics[0].Partitions = 5
		applyAll(t, e, grown)
		eventuallyConverged(t, e, grown)

		shrunk := grown
		shrunk.Topics = slices.Clone(grown.Topics)
		shrunk.Topics[0].Partitions = 2
		if got := plan(t, e, shrunk); len(got) != 1 || !strings.HasPrefix(got[0], "KEEP TOPIC "+topic+" PARTITIONS 5") {
			t.Fatalf("shrink plan = %q", got)
		}
	})

	t.Run("revoke removes exactly the declared ACLs", func(t *testing.T) {
		p, err := e.BuildRevokePlan(context.Background(), a)
		if err != nil {
			t.Fatal(err)
		}
		if p.Len() != 8 {
			t.Fatalf("revoke plan (%d):\n%s", p.Len(), p.Describe())
		}
		if _, err := p.Apply(context.Background()); err != nil {
			t.Fatal(err)
		}
		var left int
		for range 50 {
			p, err := e.BuildRevokePlan(context.Background(), a)
			if err != nil {
				t.Fatal(err)
			}
			if left = p.Len(); left == 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if left != 0 {
			t.Fatalf("%d ACLs survived the revoke", left)
		}
		cl := serviceClient(t, user, "pw-"+id)
		if err := produce(cl, topic); !errors.Is(err, kerr.TopicAuthorizationFailed) {
			t.Fatalf("produce after revoke: err = %v, want TOPIC_AUTHORIZATION_FAILED", err)
		}
	})
}

func TestIntegrationSCRAMRotation(t *testing.T) {
	e := connect(t)
	id := suffix(t)
	user := "rot-" + id
	topic := "rot-" + id
	a := kafka.Access{
		Principal:     "User:" + user,
		Authorization: kafka.AuthorizationACL,
		Topics:        []kafka.Topic{{Name: topic, Manage: true, Partitions: 1, Operations: []string{"Write"}}},
		SCRAM:         &kafka.SCRAM{Mechanism: "SCRAM-SHA-512", Password: "old-" + id, Version: "1"},
	}
	applyAll(t, e, a)
	eventuallyConverged(t, e, a)

	a.SCRAM.Password, a.SCRAM.Version = "new-"+id, "2"
	if got := plan(t, e, a); !slices.Equal(got, []string{"ROTATE SCRAM-SHA-512 CREDENTIAL FOR " + user}) {
		t.Fatalf("rotation plan = %q", got)
	}
	applyAll(t, e, a)

	var err error
	for range 25 {
		if err = produce(serviceClient(t, user, "new-"+id, kgo.RetryTimeout(2*time.Second)), topic); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("the new password does not authenticate: %v", err)
	}
	old := serviceClient(t, user, "old-"+id, kgo.RetryTimeout(2*time.Second))
	if err := produce(old, topic); err == nil {
		t.Fatal("the old password still authenticates after rotation")
	}
}
