// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafkaaccess

import (
	kafkav1alpha1 "github.com/blairham/kafka-controller/apis/kafka/v1alpha1"
	"github.com/blairham/kafka-controller/internal/engine/kafka"
)

// Access flattens the spec into the engine-neutral shape, applying the CRD's
// defaults so a manifest read from disk (by kactl) maps exactly as an
// admitted one does. The SCRAM password is not resolved here.
func Access(spec kafkav1alpha1.KafkaAccessSpec) kafka.Access {
	a := kafka.Access{
		Principal:       spec.Principal,
		Authorization:   string(spec.Authorization),
		IdempotentWrite: spec.IdempotentWrite,
	}
	if a.Authorization == "" {
		a.Authorization = kafka.AuthorizationACL
	}
	for _, t := range spec.Topics {
		kt := kafka.Topic{
			Name:       t.Name,
			Prefixed:   t.PatternType == kafkav1alpha1.PatternPrefixed,
			Manage:     t.Manage,
			Config:     t.Config,
			Operations: ops(t.Operations),
		}
		if t.Partitions != nil {
			kt.Partitions = *t.Partitions
		}
		if t.ReplicationFactor != nil {
			kt.ReplicationFactor = *t.ReplicationFactor
		}
		a.Topics = append(a.Topics, kt)
	}
	for _, g := range spec.ConsumerGroups {
		a.Groups = append(a.Groups, kafka.Grant{
			Name:       g.Name,
			Prefixed:   g.PatternType == kafkav1alpha1.PatternPrefixed,
			Operations: ops(g.Operations),
		})
	}
	for _, x := range spec.TransactionalIDs {
		a.TransactionalIDs = append(a.TransactionalIDs, kafka.Grant{
			Name:       x.Name,
			Prefixed:   x.PatternType == kafkav1alpha1.PatternPrefixed,
			Operations: ops(x.Operations),
		})
	}
	if c := spec.SCRAMCredential; c != nil {
		mech := c.Mechanism
		if mech == "" {
			mech = "SCRAM-SHA-512"
		}
		a.SCRAM = &kafka.SCRAM{Mechanism: mech}
	}
	return a
}

func ops(in []kafkav1alpha1.Operation) []string {
	out := make([]string, len(in))
	for i, op := range in {
		out[i] = string(op)
	}
	return out
}
