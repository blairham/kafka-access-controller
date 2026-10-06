// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build envtest

package kafkaaccess

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	kafkav1alpha1 "github.com/blairham/kafka-controller/apis/kafka/v1alpha1"
)

const testARN = "arn:aws:kafka:us-east-1:123456789012:cluster/prod/0b1c2d3e-aaaa-bbbb-cccc-1234567890ab-7"

func validSpec() kafkav1alpha1.KafkaAccessSpec {
	return kafkav1alpha1.KafkaAccessSpec{
		Cluster: kafkav1alpha1.ClusterRef{
			BootstrapServers: []string{"b-1.example:9098"},
			Auth:             &kafkav1alpha1.ClusterAuth{Region: "us-east-1"},
		},
		Principal: "User:orders",
		Topics: []kafkav1alpha1.Topic{{
			Name: "orders", Manage: true, Partitions: ptr.To[int32](6),
			Operations: []kafkav1alpha1.Operation{"Read", "Write"},
		}},
	}
}

// Every CEL rule and pattern in the CRD, each with the case it exists to
// reject and, where the rule is conditional, the case it must still admit.
func TestAdmission(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		mutate func(*kafkav1alpha1.KafkaAccessSpec)
		want   string // substring of the rejection; empty means admitted
	}{
		{"valid", func(*kafkav1alpha1.KafkaAccessSpec) {}, ""},
		{
			"msk-iam needs region", func(s *kafkav1alpha1.KafkaAccessSpec) { s.Cluster.Auth.Region = "" },
			"cluster.auth.region is required for msk-iam",
		},
		{
			"msk-iam by default needs region",
			func(s *kafkav1alpha1.KafkaAccessSpec) { s.Cluster.Auth = &kafkav1alpha1.ClusterAuth{} },
			"cluster.auth.region is required for msk-iam",
		},
		{"scram needs secretRef", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Cluster.Auth = &kafkav1alpha1.ClusterAuth{Method: kafkav1alpha1.AuthSCRAMSHA512}
		}, "cluster.auth.secretRef is required"},
		{"scram with secretRef", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Cluster.Auth = &kafkav1alpha1.ClusterAuth{
				Method: kafkav1alpha1.AuthSCRAMSHA512, SecretRef: &kafkav1alpha1.LocalObjectReference{Name: "admin"},
			}
		}, ""},
		{"none needs nothing", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Cluster.Auth = &kafkav1alpha1.ClusterAuth{Method: kafkav1alpha1.AuthNone}
		}, ""},
		{
			"acl needs principal", func(s *kafkav1alpha1.KafkaAccessSpec) { s.Principal = "" },
			"principal is required for authorization acl",
		},
		{
			"principal needs a type", func(s *kafkav1alpha1.KafkaAccessSpec) { s.Principal = "orders" },
			"spec.principal",
		},
		{"iam needs no principal", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Principal = ""
			s.Authorization = kafkav1alpha1.AuthorizationIAM
			s.Cluster.MSKClusterARN = testARN
		}, ""},
		{"iam needs the cluster ARN", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Authorization = kafkav1alpha1.AuthorizationIAM
		}, "cluster.mskClusterARN is required for authorization iam"},
		{"cluster ARN shape", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Cluster.MSKClusterARN = "arn:aws:kafka:us-east-1:123456789012:topic/prod/x/orders"
		}, "spec.cluster.mskClusterARN"},
		{"scram credential not under iam", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Authorization = kafkav1alpha1.AuthorizationIAM
			s.Cluster.MSKClusterARN = testARN
			s.SCRAMCredential = &kafkav1alpha1.SCRAMCredential{
				PasswordSecretRef: kafkav1alpha1.LocalObjectReference{Name: "pw"},
			}
		}, "scramCredential does not apply under authorization iam"},
		{
			"managed topic needs partitions", func(s *kafkav1alpha1.KafkaAccessSpec) { s.Topics[0].Partitions = nil },
			"a managed topic needs partitions",
		},
		{"prefixed topic cannot be managed", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Topics[0].PatternType = kafkav1alpha1.PatternPrefixed
		}, "a Prefixed topic cannot be managed"},
		{"prefixed unmanaged topic", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.Topics[0].PatternType = kafkav1alpha1.PatternPrefixed
			s.Topics[0].Manage = false
		}, ""},
		{
			"topic name charset", func(s *kafkav1alpha1.KafkaAccessSpec) { s.Topics[0].Name = "orders/v2" },
			"spec.topics[0].name",
		},
		{"group operation enum", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.ConsumerGroups = []kafkav1alpha1.ConsumerGroup{{Name: "g", Operations: []kafkav1alpha1.Operation{"Write"}}}
		}, "a consumer group takes only All, Read, Delete and Describe"},
		{"transactional id operation", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.TransactionalIDs = []kafkav1alpha1.TransactionalID{{Name: "x", Operations: []kafkav1alpha1.Operation{"Read"}}}
		}, "a transactional id takes only All, Write and Describe"},
		{"wildcard group name", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.ConsumerGroups = []kafkav1alpha1.ConsumerGroup{{Name: "*", Operations: []kafkav1alpha1.Operation{"Read"}}}
		}, "spec.consumerGroups[0].name"},
		{"wildcard transactional id", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.TransactionalIDs = []kafkav1alpha1.TransactionalID{{Name: "tx-?", Operations: []kafkav1alpha1.Operation{"Write"}}}
		}, "spec.transactionalIDs[0].name"},
		{"group operation accepted", func(s *kafkav1alpha1.KafkaAccessSpec) {
			s.ConsumerGroups = []kafkav1alpha1.ConsumerGroup{
				{Name: "g", Operations: []kafkav1alpha1.Operation{"Read", "Describe"}},
			}
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			tc.mutate(&spec)
			ka := &kafkav1alpha1.KafkaAccess{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "adm-", Namespace: "default"},
				Spec:       spec,
			}
			err := k8sClient.Create(ctx, ka)
			if err == nil {
				defer k8sClient.Delete(ctx, ka) //nolint:errcheck // test cleanup
			}
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("rejected: %v", err)
			case tc.want != "" && err == nil:
				t.Fatalf("admitted; want a rejection containing %q", tc.want)
			case tc.want != "" && !strings.Contains(err.Error(), tc.want):
				t.Fatalf("rejected with %v; want %q", err, tc.want)
			}
		})
	}
}

// The defaults the engine mapping relies on are applied at admission.
func TestDefaults(t *testing.T) {
	ctx := context.Background()
	spec := validSpec()
	spec.Cluster.Auth = &kafkav1alpha1.ClusterAuth{Region: "us-east-1"}
	ka := &kafkav1alpha1.KafkaAccess{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "def-", Namespace: "default"},
		Spec:       spec,
	}
	if err := k8sClient.Create(ctx, ka); err != nil {
		t.Fatal(err)
	}
	defer k8sClient.Delete(ctx, ka) //nolint:errcheck // test cleanup
	s := ka.Spec
	if s.Authorization != kafkav1alpha1.AuthorizationACL || s.Mode != kafkav1alpha1.ModeEnforce ||
		s.Cluster.Auth.Method != kafkav1alpha1.AuthMSKIAM || s.Topics[0].PatternType != kafkav1alpha1.PatternLiteral {
		t.Errorf("defaults = authz %q mode %q auth %q pattern %q",
			s.Authorization, s.Mode, s.Cluster.Auth.Method, s.Topics[0].PatternType)
	}
}

// Every example is admitted: they are copied by hand into real manifests.
func TestExamplesAreAdmitted(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no examples found (%v)", err)
	}
	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			var ka kafkav1alpha1.KafkaAccess
			if err := yaml.UnmarshalStrict(raw, &ka); err != nil {
				t.Fatal(err)
			}
			ka.Namespace = "default"
			ka.Name = "example-" + strings.TrimSuffix(filepath.Base(p), ".yaml")
			if err := k8sClient.Create(context.Background(), &ka); err != nil {
				t.Fatalf("rejected: %v", err)
			}
			_ = k8sClient.Delete(context.Background(), &ka)
		})
	}
}
