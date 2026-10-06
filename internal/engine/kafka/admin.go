// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
)

// Resource types an ACL can name.
const (
	ResourceTopic           = "TOPIC"
	ResourceGroup           = "GROUP"
	ResourceTransactionalID = "TRANSACTIONAL_ID"
	ResourceCluster         = "CLUSTER"

	// clusterResource is the only name a cluster ACL takes.
	clusterResource = "kafka-cluster"
)

// ACL is one allow entry. Fields are Kafka's own names, upper-cased, so the
// plan text reads like kafka-acls output.
type ACL struct {
	Principal string
	Host      string
	Resource  string // ResourceTopic, ...
	Name      string
	Operation string // READ, WRITE, ...
	Prefixed  bool
}

// Pattern spellings, as kafka-acls prints them.
const (
	patternLiteral  = "LITERAL"
	patternPrefixed = "PREFIXED"
)

func (a ACL) pattern() string {
	if a.Prefixed {
		return patternPrefixed
	}
	return patternLiteral
}

func (a ACL) String() string {
	return fmt.Sprintf("%s ALLOW %s ON %s %s %s HOST %s",
		a.Principal, a.Operation, a.Resource, a.Name, a.pattern(), a.Host)
}

// TopicState is what the cluster holds for one topic.
type TopicState struct {
	// Config holds the effective value of every config the caller asked
	// about, wherever it is set.
	Config            map[string]string
	Partitions        int32
	ReplicationFactor int16
}

// Admin is the slice of the Kafka admin API the engine uses. The production
// implementation wraps kadm; tests use a fake.
type Admin interface {
	// Topics returns the state of the named topics that exist, reading the
	// listed config keys for each. A missing topic is absent from the map.
	Topics(ctx context.Context, names, configKeys []string) (map[string]TopicState, error)

	CreateTopic(ctx context.Context, name string, partitions int32, replicationFactor int16,
		config map[string]string) error
	SetPartitions(ctx context.Context, name string, total int32) error
	SetTopicConfig(ctx context.Context, name string, config map[string]string) error

	// ACLs returns every allow ACL held by principal.
	ACLs(ctx context.Context, principal string) ([]ACL, error)
	CreateACL(ctx context.Context, acl ACL) error
	DeleteACL(ctx context.Context, acl ACL) error

	// SCRAMMechanisms returns the mechanisms user has a password for.
	SCRAMMechanisms(ctx context.Context, user string) ([]string, error)
	UpsertSCRAM(ctx context.Context, user, mechanism, password string) error

	Close()
}
