// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package mskiam renders the IAM policy a service's role needs under MSK IAM
// access control, where Kafka ACLs do not apply to IAM-authenticated clients.
package mskiam

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/blairham/kafka-controller/internal/engine/kafka"
)

// Each ACL operation's IAM equivalent, including the Describe action the data
// actions depend on (ReadData without DescribeTopic is refused).
const (
	describeTopic = "DescribeTopic"
	opDescribe    = "Describe"
)

var (
	topicActions = map[string][]string{
		"Read":            {"ReadData", describeTopic},
		"Write":           {"WriteData", describeTopic},
		opDescribe:        {describeTopic},
		"Create":          {"CreateTopic"},
		"Delete":          {"DeleteTopic"},
		"Alter":           {"AlterTopic"},
		"DescribeConfigs": {"DescribeTopicDynamicConfiguration"},
		"AlterConfigs":    {"AlterTopicDynamicConfiguration"},
	}
	groupActions = map[string][]string{
		"Read":     {"AlterGroup", "DescribeGroup"},
		opDescribe: {"DescribeGroup"},
		"Delete":   {"DeleteGroup"},
	}
	txnActions = map[string][]string{
		"Write":    {"AlterTransactionalId", "DescribeTransactionalId"},
		opDescribe: {"DescribeTransactionalId"},
	}
)

// Statement is one IAM policy statement.
type Statement struct {
	Sid      string   `json:"Sid"`
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

// Policy is an IAM policy document.
type Policy struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// clusterARNPattern is the CRD's pattern for spec.cluster.mskClusterARN,
// repeated here because kactl renders policies from manifests that never
// passed admission.
// MSK cluster names and ids are letters, digits and hyphens.
var clusterARNPattern = regexp.MustCompile(
	`^arn:aws[a-z-]*:kafka:[a-z0-9-]+:[0-9]{12}:cluster/[A-Za-z0-9-]+/[A-Za-z0-9-]+$`,
)

// arnParts splits arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>.
func arnParts(clusterARN string) (prefix, nameUUID string, err error) {
	if !clusterARNPattern.MatchString(clusterARN) {
		return "", "", fmt.Errorf(
			"%q is not an MSK cluster ARN (arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>)",
			clusterARN,
		)
	}
	head, rest, _ := strings.Cut(clusterARN, ":cluster/")
	return head, rest, nil
}

// Render returns the policy document for a, granting on the cluster
// clusterARN, as indented JSON.
func Render(clusterARN string, a kafka.Access) (string, error) {
	p, err := Build(clusterARN, a)
	if err != nil {
		return "", err
	}
	out, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// Build returns the policy for a. Statements are grouped by action set, in a
// stable order, so the document only changes when the access does.
func Build(clusterARN string, a kafka.Access) (Policy, error) {
	prefix, nameUUID, err := arnParts(clusterARN)
	if err != nil {
		return Policy{}, err
	}
	// IAM reads * and ? in a resource ARN as wildcards: a literal group
	// named * would grant every group.
	if err := kafka.CheckNames(a); err != nil {
		return Policy{}, err
	}
	arn := func(kind, name string, prefixed bool) string {
		s := fmt.Sprintf("%s:%s/%s/%s", prefix, kind, nameUUID, name)
		if prefixed {
			s += "*"
		}
		return s
	}

	clusterActs := []string{"Connect"}
	if a.IdempotentWrite {
		clusterActs = append(clusterActs, "WriteDataIdempotently")
	}

	byActions := map[string]map[string]bool{} // joined actions -> resources
	add := func(resource string, acts []string) {
		if len(acts) == 0 {
			return
		}
		key := strings.Join(acts, ",")
		if byActions[key] == nil {
			byActions[key] = map[string]bool{}
		}
		byActions[key][resource] = true
	}
	for _, t := range a.Topics {
		add(arn("topic", t.Name, t.Prefixed), expand(t.Operations, topicActions))
	}
	for _, g := range a.Groups {
		add(arn("group", g.Name, g.Prefixed), expand(g.Operations, groupActions))
	}
	for _, x := range a.TransactionalIDs {
		acts := expand(x.Operations, txnActions)
		add(arn("transactional-id", x.Name, x.Prefixed), acts)
		if slices.Contains(acts, "kafka-cluster:AlterTransactionalId") &&
			!slices.Contains(clusterActs, "WriteDataIdempotently") {
			// A transactional producer is an idempotent one.
			clusterActs = append(clusterActs, "WriteDataIdempotently")
		}
	}

	pol := Policy{Version: "2012-10-17"}
	pol.Statement = append(pol.Statement, Statement{
		Sid:      "Cluster",
		Effect:   "Allow",
		Action:   prefixed(clusterActs),
		Resource: []string{clusterARN},
	})
	for i, key := range slices.Sorted(maps.Keys(byActions)) {
		pol.Statement = append(pol.Statement, Statement{
			Sid:      fmt.Sprintf("Access%d", i),
			Effect:   "Allow",
			Action:   strings.Split(key, ","),
			Resource: slices.Sorted(maps.Keys(byActions[key])),
		})
	}
	return pol, nil
}

// expand maps operations to sorted, de-duplicated IAM actions. All is every
// action the resource type has.
func expand(ops []string, table map[string][]string) []string {
	set := map[string]bool{}
	for _, op := range ops {
		if op == "All" {
			for _, acts := range table {
				for _, a := range acts {
					set[a] = true
				}
			}
			continue
		}
		for _, a := range table[op] {
			set[a] = true
		}
	}
	return prefixed(slices.Sorted(maps.Keys(set)))
}

func prefixed(acts []string) []string {
	out := make([]string, len(acts))
	for i, a := range acts {
		out[i] = "kafka-cluster:" + a
	}
	return out
}
