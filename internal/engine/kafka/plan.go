// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/blairham/k8s-controller-kit/plan"
)

// errRefused marks a change Kafka cannot make. The step stays in the plan, so
// the resource reports it as pending rather than silently converging.
var errRefused = errors.New("refused")

// ErrTopicExists is what an Admin returns when a topic it was asked to create
// already exists: a concurrent create, tolerated.
var ErrTopicExists = errors.New("topic already exists")

// step is one admin call.
type step struct {
	tolerates  error
	apply      func(ctx context.Context) error
	text       string
	why        string
	key        string // set on a prune step
	bestEffort bool
}

func (s *step) Describe() string                { return s.text }
func (s *step) Rationale() string               { return s.why }
func (s *step) IsBestEffort() bool              { return s.bestEffort }
func (s *step) Apply(ctx context.Context) error { return s.apply(ctx) }
func (s *step) Tolerates(err error) bool        { return s.tolerates != nil && errors.Is(err, s.tolerates) }
func (s *step) Key() string                     { return s.key }

// refusal is a step for a change Kafka cannot make. It always fails, as a
// warning, so status names the gap on every reconcile until the spec changes.
func refusal(text, why string) *step {
	return &step{text: text, why: why, bestEffort: true, apply: func(context.Context) error {
		return fmt.Errorf("%w: %s", errRefused, why)
	}}
}

// Engine builds plans against one cluster.
type Engine struct {
	admin Admin
}

// New returns an engine over admin. Close closes admin.
func New(admin Admin) *Engine { return &Engine{admin: admin} }

// Close releases the admin connection.
func (e *Engine) Close() error {
	e.admin.Close()
	return nil
}

// BuildPlan returns the steps that bring the cluster to a: the SCRAM
// credential, then topics, then ACLs. It is a diff: a converged cluster plans
// nothing. It only adds: an ACL the principal holds but the spec does not
// declare is left alone here, and is pruned (see Prune) only if this resource
// created it.
func (e *Engine) BuildPlan(ctx context.Context, a Access) (*plan.Plan, error) {
	if err := Validate(a); err != nil {
		return nil, err
	}
	var p plan.Plan

	if a.SCRAM != nil {
		s, err := e.scramSteps(ctx, a)
		if err != nil {
			return nil, err
		}
		p.Add(s...)
	}

	steps, err := e.topicSteps(ctx, a.Topics)
	if err != nil {
		return nil, err
	}
	p.Add(steps...)

	// A resource that declares no ACLs must not READ them either. A broker
	// with no authorizer (a dev or test cluster) answers DescribeACLs with
	// SECURITY_DISABLED, so asking anyway fails the whole plan and a
	// topics-only resource could never converge there.
	if desired := DesiredACLs(a); a.Authorization == AuthorizationACL && len(desired) > 0 {
		held, err := e.admin.ACLs(ctx, a.Principal)
		if err != nil {
			return nil, fmt.Errorf("describing ACLs for %s: %w", a.Principal, err)
		}
		have := map[ACL]bool{}
		for _, acl := range held {
			have[acl] = true
		}
		for _, acl := range desired {
			if have[acl] {
				continue
			}
			p.Add(&step{
				text:  "CREATE ACL " + acl.String(),
				apply: func(ctx context.Context) error { return e.admin.CreateACL(ctx, acl) },
			})
		}
	}
	return &p, nil
}

// BuildRevokePlan deletes the ACLs a declares that the cluster holds. Topics
// and credentials are never deleted.
func (e *Engine) BuildRevokePlan(ctx context.Context, a Access) (*plan.Plan, error) {
	var p plan.Plan
	desired := DesiredACLs(a)
	if a.Authorization != AuthorizationACL || len(desired) == 0 {
		return &p, nil
	}
	held, err := e.admin.ACLs(ctx, a.Principal)
	if err != nil {
		return nil, fmt.Errorf("describing ACLs for %s: %w", a.Principal, err)
	}
	have := map[ACL]bool{}
	for _, acl := range held {
		have[acl] = true
	}
	for _, acl := range desired {
		if !have[acl] {
			continue
		}
		p.Add(&step{
			text:  "DELETE ACL " + acl.String(),
			apply: func(ctx context.Context) error { return e.admin.DeleteACL(ctx, acl) },
		})
	}
	return &p, nil
}

func (e *Engine) scramSteps(ctx context.Context, a Access) ([]plan.Step, error) {
	user, sc := a.User(), a.SCRAM
	mechs, err := e.admin.SCRAMMechanisms(ctx, user)
	if err != nil {
		return nil, fmt.Errorf("describing SCRAM credentials for %s: %w", user, err)
	}
	verb := "CREATE"
	switch {
	case !slices.Contains(mechs, sc.Mechanism):
	case sc.Version != sc.AppliedVersion:
		verb = "ROTATE"
	default:
		return nil, nil
	}
	return []plan.Step{&step{
		text: fmt.Sprintf("%s %s CREDENTIAL FOR %s", verb, sc.Mechanism, user),
		why:  "the password comes from the referenced Secret; Kafka cannot report a password, so a change to the Secret is what triggers a rotation",
		apply: func(ctx context.Context) error {
			if err := e.admin.UpsertSCRAM(ctx, user, sc.Mechanism, sc.Password); err != nil {
				return err
			}
			// Shared with the caller through the pointer, so the re-plan
			// after applying sees the rotation done and status records it.
			sc.AppliedVersion = sc.Version
			return nil
		},
	}}, nil
}

func (e *Engine) topicSteps(ctx context.Context, topics []Topic) ([]plan.Step, error) {
	var names []string
	keys := map[string]bool{}
	for _, t := range topics {
		if !t.Manage {
			continue
		}
		names = append(names, t.Name)
		for k := range t.Config {
			keys[k] = true
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	state, err := e.admin.Topics(ctx, names, slices.Sorted(maps.Keys(keys)))
	if err != nil {
		return nil, fmt.Errorf("describing topics: %w", err)
	}

	var steps []plan.Step
	for _, t := range topics {
		if !t.Manage {
			continue
		}
		cur, ok := state[t.Name]
		if !ok {
			steps = append(steps, e.createTopic(t))
			continue
		}
		steps = append(steps, e.alterTopic(t, cur)...)
	}
	return steps, nil
}

func (e *Engine) createTopic(t Topic) plan.Step {
	text := fmt.Sprintf("CREATE TOPIC %s PARTITIONS %d", t.Name, t.Partitions)
	if t.ReplicationFactor > 0 {
		text += fmt.Sprintf(" REPLICATION-FACTOR %d", t.ReplicationFactor)
	}
	if len(t.Config) > 0 {
		text += " CONFIG " + renderConfig(t.Config)
	}
	return &step{
		text:      text,
		tolerates: ErrTopicExists,
		apply: func(ctx context.Context) error {
			return e.admin.CreateTopic(ctx, t.Name, t.Partitions, t.ReplicationFactor, t.Config)
		},
	}
}

func (e *Engine) alterTopic(t Topic, cur TopicState) []plan.Step {
	var steps []plan.Step
	switch {
	case t.Partitions > cur.Partitions:
		steps = append(steps, &step{
			text: fmt.Sprintf("ALTER TOPIC %s PARTITIONS %d -> %d", t.Name, cur.Partitions, t.Partitions),
			why:  "adding partitions changes which partition a key hashes to",
			apply: func(ctx context.Context) error {
				return e.admin.SetPartitions(ctx, t.Name, t.Partitions)
			},
		})
	case t.Partitions < cur.Partitions:
		steps = append(steps, refusal(
			fmt.Sprintf("KEEP TOPIC %s PARTITIONS %d (spec asks for %d)", t.Name, cur.Partitions, t.Partitions),
			"Kafka cannot remove partitions; raise spec.topics[].partitions to match",
		))
	}
	if t.ReplicationFactor > 0 && t.ReplicationFactor != cur.ReplicationFactor {
		steps = append(steps, refusal(
			fmt.Sprintf("KEEP TOPIC %s REPLICATION-FACTOR %d (spec asks for %d)",
				t.Name, cur.ReplicationFactor, t.ReplicationFactor),
			"changing the replication factor needs a partition reassignment, which this controller does not run",
		))
	}
	drift := map[string]string{}
	for k, v := range t.Config {
		if have, ok := cur.Config[k]; !ok || have != v {
			drift[k] = v
		}
	}
	if len(drift) > 0 {
		steps = append(steps, &step{
			text: fmt.Sprintf("ALTER TOPIC %s SET %s", t.Name, renderConfig(drift)),
			apply: func(ctx context.Context) error {
				return e.admin.SetTopicConfig(ctx, t.Name, drift)
			},
		})
	}
	return steps
}

func renderConfig(c map[string]string) string {
	parts := make([]string, 0, len(c))
	for _, k := range slices.Sorted(maps.Keys(c)) {
		parts = append(parts, k+"="+c[k])
	}
	return strings.Join(parts, ",")
}

// DesiredACLs expands a into one allow ACL per resource and operation, in a
// stable order.
func DesiredACLs(a Access) []ACL {
	var out []ACL
	add := func(resource, name string, prefixed bool, ops []string) {
		for _, op := range ops {
			out = append(out, ACL{
				Principal: a.Principal,
				Host:      "*",
				Resource:  resource,
				Name:      name,
				Prefixed:  prefixed,
				Operation: opName(op),
			})
		}
	}
	for _, t := range a.Topics {
		add(ResourceTopic, t.Name, t.Prefixed, t.Operations)
	}
	for _, g := range a.Groups {
		add(ResourceGroup, g.Name, g.Prefixed, g.Operations)
	}
	for _, x := range a.TransactionalIDs {
		add(ResourceTransactionalID, x.Name, x.Prefixed, x.Operations)
	}
	if a.IdempotentWrite {
		add(ResourceCluster, clusterResource, false, []string{"IdempotentWrite"})
	}
	return dedupe(out)
}

// opName converts an API operation (DescribeConfigs) to Kafka's
// (DESCRIBE_CONFIGS).
func opName(op string) string {
	var b strings.Builder
	for i, r := range op {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

func dedupe(acls []ACL) []ACL {
	seen := map[ACL]bool{}
	out := acls[:0]
	for _, a := range acls {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}
