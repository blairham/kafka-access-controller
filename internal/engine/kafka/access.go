// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package kafka plans the data-plane work a service needs on a Kafka cluster
// (topics, ACLs, SCRAM credentials) against the Kafka admin API. It works on
// MSK and on any cluster that speaks that API.
package kafka

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Authorization models, mirroring the API's.
const (
	AuthorizationACL = "acl"
	AuthorizationIAM = "iam"
)

// Access is the engine-neutral description of what a service needs. It is the
// controller's spec, flattened so the engine never imports the API types.
type Access struct {
	// SCRAM, when set, provisions the principal's SCRAM password.
	SCRAM *SCRAM

	// Principal is the Kafka principal ACLs are granted to, such as
	// User:orders.
	Principal string

	// Authorization is AuthorizationACL or AuthorizationIAM. Under IAM no
	// ACLs are planned: MSK ignores them for IAM-authenticated clients.
	Authorization string

	Topics           []Topic
	Groups           []Grant
	TransactionalIDs []Grant

	// IdempotentWrite grants IdempotentWrite on the cluster.
	IdempotentWrite bool
}

// Topic is one topic and the operations granted on it.
type Topic struct {
	Config            map[string]string
	Name              string
	Operations        []string
	Partitions        int32
	ReplicationFactor int16 // 0 uses the broker default
	Prefixed          bool

	// Manage creates the topic and maintains Partitions and Config.
	Manage bool
}

// Grant is a group or transactional id and the operations granted on it.
type Grant struct {
	Name       string
	Operations []string
	Prefixed   bool
}

// SCRAM is a SCRAM credential to provision.
type SCRAM struct {
	Mechanism string // SCRAM-SHA-512 or SCRAM-SHA-256
	Password  string

	// Version identifies the password (the Secret's resourceVersion), and
	// AppliedVersion the one last written. Kafka cannot report a password,
	// so a rotation is detected by the two differing. A successful apply sets
	// AppliedVersion, which the controller then records in status.
	Version        string
	AppliedVersion string
}

// User is the SCRAM user name: the principal without its "User:" type.
func (a Access) User() string {
	_, name, _ := strings.Cut(a.Principal, ":")
	return name
}

// The API's operation names that more than one resource type takes.
const (
	opAll      = "All"
	opRead     = "Read"
	opWrite    = "Write"
	opDelete   = "Delete"
	opDescribe = "Describe"
)

var (
	topicOps = map[string]bool{
		opAll: true, opRead: true, opWrite: true, "Create": true, opDelete: true, "Alter": true,
		opDescribe: true, "DescribeConfigs": true, "AlterConfigs": true,
	}
	groupOps = map[string]bool{opAll: true, opRead: true, opDelete: true, opDescribe: true}
	txnOps   = map[string]bool{opAll: true, opWrite: true, opDescribe: true}
)

// Validate rejects an Access this engine cannot honor, before any connection
// is opened. The CRD enforces most of this at admission; manifests kactl
// reads from disk do not pass through admission.
func Validate(a Access) error {
	errs := validateAuthorization(a)
	errs = append(errs, validateSCRAM(a.SCRAM)...)
	if err := CheckNames(a); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs, validateTopics(a.Topics)...)
	for _, g := range a.Groups {
		errs = append(errs, checkOps("group", g.Name, g.Operations, groupOps)...)
	}
	for _, x := range a.TransactionalIDs {
		errs = append(errs, checkOps("transactional id", x.Name, x.Operations, txnOps)...)
	}
	return errors.Join(errs...)
}

func validateAuthorization(a Access) []error {
	switch a.Authorization {
	case AuthorizationACL:
		if !strings.HasPrefix(a.Principal, "User:") || len(a.Principal) == len("User:") {
			return []error{fmt.Errorf("principal %q must be User:<name> under authorization acl", a.Principal)}
		}
	case AuthorizationIAM:
		if a.SCRAM != nil {
			return []error{errors.New("a SCRAM credential does not apply under authorization iam")}
		}
	default:
		return []error{fmt.Errorf("unknown authorization %q", a.Authorization)}
	}
	return nil
}

func validateSCRAM(sc *SCRAM) []error {
	if sc == nil {
		return nil
	}
	var errs []error
	switch sc.Mechanism {
	case "SCRAM-SHA-512", "SCRAM-SHA-256":
	default:
		errs = append(errs, fmt.Errorf("unknown SCRAM mechanism %q", sc.Mechanism))
	}
	if sc.Password == "" {
		errs = append(errs, errors.New("the SCRAM password is empty"))
	}
	return errs
}

func validateTopics(topics []Topic) []error {
	var errs []error
	seen := map[string]bool{}
	for _, t := range topics {
		key := fmt.Sprintf("%s/%t", t.Name, t.Prefixed)
		if seen[key] {
			errs = append(errs, fmt.Errorf("topic %q is listed twice", t.Name))
		}
		seen[key] = true
		if t.Manage && t.Prefixed {
			errs = append(errs, fmt.Errorf("topic %q: a prefixed topic cannot be managed", t.Name))
		}
		if t.Manage && t.Partitions < 1 {
			errs = append(errs, fmt.Errorf("topic %q: a managed topic needs partitions", t.Name))
		}
		errs = append(errs, checkOps("topic", t.Name, t.Operations, topicOps)...)
	}
	return errs
}

// names returns every resource name a grants on.
func names(a Access) []string {
	out := make([]string, 0, len(a.Topics)+len(a.Groups)+len(a.TransactionalIDs))
	for _, t := range a.Topics {
		out = append(out, t.Name)
	}
	for _, g := range a.Groups {
		out = append(out, g.Name)
	}
	for _, x := range a.TransactionalIDs {
		out = append(out, x.Name)
	}
	return out
}

// CheckNames rejects a name that a broker or IAM would read as a wildcard.
// Validate includes it; the IAM renderer, which runs without Validate, calls
// it directly.
func CheckNames(a Access) error {
	for _, n := range names(a) {
		if strings.ContainsAny(n, "*?") {
			return fmt.Errorf("name %q contains * or ?, which Kafka ACLs and IAM policies read as wildcards", n)
		}
		// JSON would replace the bad bytes, so a policy would name a
		// different resource than the ACL does.
		if !utf8.ValidString(n) {
			return fmt.Errorf("name %q is not valid UTF-8", n)
		}
	}
	return nil
}

func checkOps(kind, name string, ops []string, allowed map[string]bool) []error {
	var errs []error
	for _, op := range ops {
		if !allowed[op] {
			errs = append(errs, fmt.Errorf("%s %q: operation %q does not apply", kind, name, op))
		}
	}
	return errs
}
