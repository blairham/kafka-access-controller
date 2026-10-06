// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafkaaccess

import (
	"context"
	"fmt"

	"github.com/blairham/k8s-controller-kit/plan"
	"github.com/blairham/k8s-controller-kit/secret"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafkav1alpha1 "github.com/blairham/kafka-controller/apis/kafka/v1alpha1"
	"github.com/blairham/kafka-controller/internal/engine/kafka"
)

// Engine is what the reconciler needs from an engine; envtest fakes it.
type Engine interface {
	BuildPlan(ctx context.Context, a kafka.Access) (*plan.Plan, error)
	BuildRevokePlan(ctx context.Context, a kafka.Access) (*plan.Plan, error)
	Prune(ctx context.Context, keys []string) ([]kafka.PruneStep, error)
	Close() error
}

// EngineFactory opens an engine for one resource. It takes the whole object
// because credential Secrets are resolved in the resource's namespace.
type EngineFactory func(ctx context.Context, ka *kafkav1alpha1.KafkaAccess) (Engine, error)

// NewEngineFactory returns the production factory. The client only reads
// Secrets, and only from the resource's own namespace.
func NewEngineFactory(c client.Reader) EngineFactory {
	return func(ctx context.Context, ka *kafkav1alpha1.KafkaAccess) (Engine, error) {
		lookup := func(name, key string) ([]byte, error) {
			return secret.Read(ctx, c, ka.Namespace, name, key)
		}
		cfg, err := ConnConfig(ka, lookup)
		if err != nil {
			return nil, err
		}
		cl, err := kafka.Connect(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return kafka.New(kafka.NewKadmAdmin(cl)), nil
	}
}

// Lookup returns one key of a credential Secret the spec names. The
// controller reads the Secret in the resource's namespace; kactl reads the
// environment instead.
type Lookup func(secretName, key string) ([]byte, error)

// ConnConfig resolves the admin connection for ka. Exported so kactl connects
// exactly as the controller does.
func ConnConfig(ka *kafkav1alpha1.KafkaAccess, lookup Lookup) (kafka.ConnConfig, error) {
	cl := ka.Spec.Cluster
	cfg := kafka.ConnConfig{
		Brokers:  cl.BootstrapServers,
		TLS:      true,
		Auth:     kafka.AuthMSKIAM,
		ClientID: "kafka-controller/" + ka.Namespace + "/" + ka.Name,
	}
	if t := cl.TLS; t != nil {
		if t.Enabled != nil {
			cfg.TLS = *t.Enabled
		}
		cfg.ServerName = t.ServerName
		if t.CASecretRef != nil {
			ca, err := lookup(t.CASecretRef.Name, "ca.crt")
			if err != nil {
				return cfg, err
			}
			cfg.CA = ca
		}
	}

	auth := cl.Auth
	if auth == nil {
		auth = &kafkav1alpha1.ClusterAuth{}
	}
	if auth.Method != "" {
		cfg.Auth = string(auth.Method)
	}
	cfg.Region = auth.Region

	err := readCredentials(&cfg, auth, lookup)
	return cfg, err
}

// readCredentials fills in the admin credentials the auth method needs.
func readCredentials(cfg *kafka.ConnConfig, auth *kafkav1alpha1.ClusterAuth, lookup Lookup) error {
	read := func(key string) ([]byte, error) {
		if auth.SecretRef == nil {
			return nil, fmt.Errorf("cluster.auth.method %s needs cluster.auth.secretRef", cfg.Auth)
		}
		return lookup(auth.SecretRef.Name, key)
	}
	switch cfg.Auth {
	case kafka.AuthSCRAMSHA512, kafka.AuthSCRAMSHA256, kafka.AuthPlain:
		user, err := read("username")
		if err != nil {
			return err
		}
		pw, err := read("password")
		if err != nil {
			return err
		}
		cfg.Username, cfg.Password = string(user), string(pw)
	case kafka.AuthMTLS:
		crt, err := read("tls.crt")
		if err != nil {
			return err
		}
		key, err := read("tls.key")
		if err != nil {
			return err
		}
		cfg.CertPEM, cfg.KeyPEM = crt, key
	}
	return nil
}

// resolveAccess maps the spec and reads the service's SCRAM password.
func resolveAccess(ctx context.Context, c client.Reader, ka *kafkav1alpha1.KafkaAccess) (kafka.Access, error) {
	a := Access(ka.Spec)
	if a.SCRAM == nil {
		return a, nil
	}
	ref := ka.Spec.SCRAMCredential.PasswordSecretRef.Name
	pw, version, err := secret.Get(ctx, c, ka.Namespace, ref, "password")
	if err != nil {
		return a, err
	}
	a.SCRAM.Password = string(pw)
	a.SCRAM.Version = version
	a.SCRAM.AppliedVersion = ka.Status.SCRAMSecretVersion
	return a, nil
}
