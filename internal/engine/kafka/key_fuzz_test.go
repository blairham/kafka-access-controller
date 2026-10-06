// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka_test

import (
	"testing"

	"github.com/blairham/kafka-controller/internal/engine/kafka"
)

// FuzzACLKey covers ownership keys, which come back from status -- anything
// a status writer put there. Whatever the input:
//
//   - ParseACLKey never panics, and a key it accepts names a known resource
//     type and operation spelling only if those were in the key: parsing
//     invents nothing, and Key of the result parses back to the same ACL;
//   - for any ACL built from arbitrary fields, Key parses back to exactly
//     that ACL, so a principal with slashes, spaces or escapes cannot shift
//     a field into its neighbor.
func FuzzACLKey(f *testing.F) {
	f.Add("acl/User:orders/*/TOPIC/LITERAL/orders/READ", "User:a", "*", "TOPIC", "t", "READ", false)
	f.Add(
		"acl/User:CN=x%2Fy%20z/*/GROUP/PREFIXED/g%2F/DESCRIBE",
		"User:CN=a/b c,O=d",
		"10.0.0.1",
		"GROUP",
		"g/1 2",
		"DESCRIBE",
		true,
	)
	f.Add("acl/%zz/a/b/c/d/e", "", "", "", "", "", false)
	f.Add("acl//////", "%2F", "%", "/", "%%", "", true)
	f.Fuzz(func(t *testing.T, key, principal, host, resource, name, op string, prefixed bool) {
		if acl, err := kafka.ParseACLKey(key); err == nil {
			back, err := kafka.ParseACLKey(acl.Key())
			if err != nil || back != acl {
				t.Fatalf("accepted %q as %+v, whose key %q parses to %+v, %v", key, acl, acl.Key(), back, err)
			}
		}

		acl := kafka.ACL{Principal: principal, Host: host, Resource: resource, Name: name, Prefixed: prefixed, Operation: op}
		got, err := kafka.ParseACLKey(acl.Key())
		if err != nil || got != acl {
			t.Fatalf("Key(%+v) = %q parses to %+v, %v", acl, acl.Key(), got, err)
		}
	})
}
