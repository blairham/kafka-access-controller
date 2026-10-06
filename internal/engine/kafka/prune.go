// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/blairham/k8s-controller-kit/plan"
)

// aclKeyPrefix marks an ownership key as an ACL. Only ACLs are owned: topics
// and credentials are never deleted, so they never need pruning.
const aclKeyPrefix = "acl/"

// Key is the ACL's ownership key: every field, path-escaped, so a principal
// with spaces or slashes (an mTLS distinguished name) round-trips. Changing
// this format orphans every key already recorded in status.
func (a ACL) Key() string {
	fields := []string{a.Principal, a.Host, a.Resource, a.pattern(), a.Name, a.Operation}
	for i, f := range fields {
		fields[i] = url.PathEscape(f)
	}
	return aclKeyPrefix + strings.Join(fields, "/")
}

// ParseACLKey reverses ACL.Key.
func ParseACLKey(key string) (ACL, error) {
	rest, ok := strings.CutPrefix(key, aclKeyPrefix)
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 6 {
		return ACL{}, fmt.Errorf("%q is not an ACL key", key)
	}
	for i, p := range parts {
		v, err := url.PathUnescape(p)
		if err != nil {
			return ACL{}, fmt.Errorf("%q is not an ACL key: %w", key, err)
		}
		parts[i] = v
	}
	if parts[3] != patternLiteral && parts[3] != patternPrefixed {
		return ACL{}, fmt.Errorf("%q has pattern %q", key, parts[3])
	}
	return ACL{
		Principal: parts[0],
		Host:      parts[1],
		Resource:  parts[2],
		Prefixed:  parts[3] == patternPrefixed,
		Name:      parts[4],
		Operation: parts[5],
	}, nil
}

// OwnedKeys returns the keys of the ACLs a declares. Under IAM it declares
// none, so switching a resource from acl to iam prunes its ACLs.
func OwnedKeys(a Access) []string {
	if a.Authorization != AuthorizationACL {
		return nil
	}
	acls := DesiredACLs(a)
	out := make([]string, len(acls))
	for i, acl := range acls {
		out[i] = acl.Key()
	}
	return out
}

// PruneStep deletes the thing its Key names.
type PruneStep interface {
	plan.Step
	Key() string
}

// Prune returns a DELETE ACL step for each key the cluster still holds. Keys
// may name a principal the spec no longer uses; that is the point. Keys that
// do not parse are skipped, so an unknown key cannot block the rest.
func (e *Engine) Prune(ctx context.Context, keys []string) ([]PruneStep, error) {
	held := map[string]map[ACL]bool{} // by principal
	var out []PruneStep
	for _, key := range keys {
		acl, err := ParseACLKey(key)
		if err != nil {
			continue
		}
		if held[acl.Principal] == nil {
			list, err := e.admin.ACLs(ctx, acl.Principal)
			if err != nil {
				return nil, fmt.Errorf("describing ACLs for %s: %w", acl.Principal, err)
			}
			held[acl.Principal] = map[ACL]bool{}
			for _, h := range list {
				held[acl.Principal][h] = true
			}
		}
		if !held[acl.Principal][acl] {
			continue
		}
		out = append(out, &step{
			text:       "DELETE ACL " + acl.String(),
			why:        "this resource created it and its spec no longer declares it",
			bestEffort: true,
			key:        key,
			apply:      func(ctx context.Context) error { return e.admin.DeleteACL(ctx, acl) },
		})
	}
	return out, nil
}
