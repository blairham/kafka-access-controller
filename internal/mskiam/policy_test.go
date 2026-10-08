// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package mskiam_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/blairham/kafka-access-controller/internal/engine/kafka"
	"github.com/blairham/kafka-access-controller/internal/mskiam"
)

const clusterARN = "arn:aws:kafka:us-east-1:123456789012:cluster/prod/0b1c2d3e-aaaa-bbbb-cccc-1234567890ab-7"

func TestBuild(t *testing.T) {
	t.Parallel()
	p, err := mskiam.Build(clusterARN, kafka.Access{
		Authorization: kafka.AuthorizationIAM,
		Topics: []kafka.Topic{
			{Name: "orders", Operations: []string{"Read", "Write"}},
			{Name: "payments.", Prefixed: true, Operations: []string{"Read"}},
			{Name: "audit", Operations: []string{"Read"}},
		},
		Groups:           []kafka.Grant{{Name: "orders-svc", Operations: []string{"Read"}}},
		TransactionalIDs: []kafka.Grant{{Name: "orders-", Prefixed: true, Operations: []string{"Write"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	const base = "arn:aws:kafka:us-east-1:123456789012:"
	const id = "prod/0b1c2d3e-aaaa-bbbb-cccc-1234567890ab-7"
	want := []mskiam.Statement{
		{
			Sid: "Cluster", Effect: "Allow",
			// Transactional writes imply idempotent ones.
			Action:   []string{"kafka-cluster:Connect", "kafka-cluster:WriteDataIdempotently"},
			Resource: []string{clusterARN},
		},
		{
			Sid: "Access0", Effect: "Allow",
			Action:   []string{"kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"},
			Resource: []string{base + "group/" + id + "/orders-svc"},
		},
		{
			Sid: "Access1", Effect: "Allow",
			Action:   []string{"kafka-cluster:AlterTransactionalId", "kafka-cluster:DescribeTransactionalId"},
			Resource: []string{base + "transactional-id/" + id + "/orders-*"},
		},
		{
			Sid: "Access2", Effect: "Allow",
			Action:   []string{"kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"},
			Resource: []string{base + "topic/" + id + "/audit", base + "topic/" + id + "/payments.*"},
		},
		{
			Sid: "Access3", Effect: "Allow",
			Action:   []string{"kafka-cluster:DescribeTopic", "kafka-cluster:ReadData", "kafka-cluster:WriteData"},
			Resource: []string{base + "topic/" + id + "/orders"},
		},
	}
	if !reflect.DeepEqual(p.Statement, want) {
		t.Errorf("statements =\n%+v\nwant\n%+v", p.Statement, want)
	}
}

func TestAllExpandsToEveryAction(t *testing.T) {
	t.Parallel()
	p, err := mskiam.Build(clusterARN, kafka.Access{Topics: []kafka.Topic{{Name: "t", Operations: []string{"All"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(p.Statement[1].Action); got != 8 {
		t.Errorf("All expanded to %d topic actions, want 8: %v", got, p.Statement[1].Action)
	}
}

// IAM reads * and ? in a resource as wildcards; a literal group named *
// would grant every group. Found by FuzzPolicy.
func TestRejectsWildcardNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"*", "orders-?", "a*b"} {
		_, err := mskiam.Render(clusterARN, kafka.Access{Groups: []kafka.Grant{{Name: name, Operations: []string{"Read"}}}})
		if err == nil || !strings.Contains(err.Error(), "read as wildcards") {
			t.Errorf("group %q: err = %v", name, err)
		}
	}
}

func TestRejectsNonClusterARN(t *testing.T) {
	t.Parallel()
	for _, arn := range []string{
		"",
		"arn:aws:kafka:us-east-1:123456789012:topic/prod/uuid/orders",
		"arn:aws:s3:::bucket",
		"arn:aws:kafka:us-east-1:123456789012:cluster/prod",
		"arn:aws:kafka::cluster/0/0", // no region or account: found by FuzzPolicy
		"arn:aws:kafka:us-east-1:123456789012:cluster/prod/*",
		"arn:aws:kafka:us-east-1:123456789012:cluster/prod/\x8f", // invalid UTF-8: found by FuzzPolicy
	} {
		if _, err := mskiam.Render(
			arn,
			kafka.Access{},
		); err == nil ||
			!strings.Contains(err.Error(), "not an MSK cluster ARN") {
			t.Errorf("Render(%q) err = %v", arn, err)
		}
	}
}
