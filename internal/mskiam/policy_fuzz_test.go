// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package mskiam_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/blairham/kafka-access-controller/internal/engine/kafka"
	"github.com/blairham/kafka-access-controller/internal/mskiam"
)

// FuzzPolicy feeds an ARN and resource names as a spec could carry them.
// Whatever they are, Render never panics, and a policy it renders:
//
//   - is valid JSON with one Allow statement on exactly the cluster ARN;
//   - grants every other statement only on resources of that same cluster
//     (its region, account, name and uuid), never on a bare wildcard, and
//     a trailing * only for a prefixed name;
//   - contains no action outside kafka-cluster:.
func FuzzPolicy(f *testing.F) {
	const arn = "arn:aws:kafka:us-east-1:123456789012:cluster/prod/0b1c2d3e-aaaa-bbbb-cccc-1234567890ab-7"
	f.Add(arn, "orders", "g", "tx-", false, true)
	f.Add(arn, "*", "*", "*", true, true)
	f.Add("arn:aws:kafka:us-east-1:1:cluster/a/b/c", "x", "y", "z", false, false)
	f.Add("arn:aws:kafka::cluster/x/y", "", "", "", true, false)
	f.Add(arn, "a\"b", "c/d", "e:f", true, false)
	f.Fuzz(func(t *testing.T, clusterARN, topic, group, txn string, prefixed, all bool) {
		ops := []string{"Read", "Write"}
		if all {
			ops = []string{"All"}
		}
		a := kafka.Access{
			Authorization:    kafka.AuthorizationIAM,
			Topics:           []kafka.Topic{{Name: topic, Prefixed: prefixed, Operations: ops}},
			Groups:           []kafka.Grant{{Name: group, Prefixed: prefixed, Operations: []string{"Read"}}},
			TransactionalIDs: []kafka.Grant{{Name: txn, Prefixed: prefixed, Operations: []string{"Write"}}},
		}
		doc, err := mskiam.Render(clusterARN, a)
		if err != nil {
			return
		}
		var p mskiam.Policy
		if err := json.Unmarshal([]byte(doc), &p); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, doc)
		}
		if len(p.Statement) == 0 || p.Statement[0].Sid != "Cluster" ||
			len(p.Statement[0].Resource) != 1 || p.Statement[0].Resource[0] != clusterARN {
			t.Fatalf("first statement is not the cluster: %+v", p.Statement)
		}
		head, nameUUID, _ := strings.Cut(clusterARN, ":cluster/")
		for _, st := range p.Statement {
			if st.Effect != "Allow" {
				t.Fatalf("effect %q", st.Effect)
			}
			for _, act := range st.Action {
				if !strings.HasPrefix(act, "kafka-cluster:") || strings.Contains(act, "*") {
					t.Fatalf("action %q", act)
				}
			}
			if st.Sid == "Cluster" {
				continue
			}
			for _, res := range st.Resource {
				ok := false
				for _, kind := range []string{"topic", "group", "transactional-id"} {
					want := head + ":" + kind + "/" + nameUUID + "/"
					if strings.HasPrefix(res, want) {
						ok = true
						rest := strings.TrimPrefix(res, want)
						if strings.ContainsAny(strings.TrimSuffix(rest, "*"), "*?") ||
							(strings.HasSuffix(rest, "*") && !prefixed) {
							t.Fatalf("wildcard beyond a prefixed name's trailing *: %q", res)
						}
					}
				}
				if !ok {
					t.Fatalf("resource %q is not in cluster %q", res, clusterARN)
				}
			}
		}
	})
}
