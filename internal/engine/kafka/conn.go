// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Auth methods, mirroring the API's.
const (
	AuthMSKIAM      = "msk-iam"
	AuthSCRAMSHA512 = "scram-sha-512"
	AuthSCRAMSHA256 = "scram-sha-256"
	AuthMTLS        = "mtls"
	AuthPlain       = "plain"
	AuthNone        = "none"
)

// ConnConfig is everything needed to open the admin connection.
type ConnConfig struct {
	ServerName string
	Auth       string
	Region     string // msk-iam
	Username   string // scram, plain
	Password   string // scram, plain

	// ClientID names the connection in broker logs.
	ClientID string

	Brokers []string
	CA      []byte // PEM, trusted in addition to the system roots
	CertPEM []byte // mtls
	KeyPEM  []byte // mtls
	TLS     bool
}

// Connect opens a client and checks that the cluster answers, so a bad
// endpoint or credential fails as ConnectionFailed rather than at planning.
func Connect(ctx context.Context, c ConnConfig) (*kgo.Client, error) {
	opts, err := clientOptions(ctx, c)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("connecting to %v: %w", c.Brokers, err)
	}
	return cl, nil
}

func clientOptions(ctx context.Context, c ConnConfig) ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("no bootstrap servers")
	}
	clientID := c.ClientID
	if clientID == "" {
		clientID = "kafka-controller"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(clientID),
		kgo.RequestTimeoutOverhead(10 * time.Second),
	}

	if c.TLS {
		tlsCfg, err := tlsConfig(c)
		if err != nil {
			return nil, err
		}
		dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tlsCfg}
		opts = append(opts, kgo.Dialer(dialer.DialContext))
	} else {
		switch c.Auth {
		case AuthMTLS:
			return nil, errors.New("mtls needs TLS enabled")
		case AuthPlain:
			// SASL/PLAIN sends the password as written.
			return nil, errors.New("plain auth needs TLS enabled")
		}
	}

	switch c.Auth {
	case AuthMTLS, AuthNone:
		// No SASL: mTLS authenticates in the handshake.
	default:
		mech, err := mechanism(ctx, c)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

// tlsConfig builds the client TLS configuration; called only with TLS on.
func tlsConfig(c ConnConfig) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName}
	if len(c.CA) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(c.CA) {
			return nil, errors.New("the CA bundle holds no PEM certificate")
		}
		cfg.RootCAs = pool
	}
	if c.Auth == AuthMTLS {
		cert, err := tls.X509KeyPair(c.CertPEM, c.KeyPEM)
		if err != nil {
			return nil, fmt.Errorf("loading the client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func mechanism(ctx context.Context, c ConnConfig) (sasl.Mechanism, error) {
	switch c.Auth {
	case AuthMSKIAM:
		if c.Region == "" {
			return nil, errors.New("msk-iam needs a region")
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region))
		if err != nil {
			return nil, fmt.Errorf("loading AWS credentials: %w", err)
		}
		// OAUTHBEARER with AWS's MSK signer, not franz-go's AWS_MSK_IAM: that
		// mechanism has no region input. It reads the region from an
		// *.amazonaws.com broker host and otherwise only from AWS_REGION, so
		// spec.cluster.auth.region never reached the signature and a broker
		// behind any other name (a PrivateLink alias, a rig's IAM proxy) failed
		// every handshake with "cannot determine the region". MSK serves both
		// mechanisms on its IAM listener.
		//
		// A token is minted per connection, so a rotated Pod Identity or IRSA
		// credential is picked up without a restart.
		region, creds := c.Region, awsCfg.Credentials
		return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
			token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(ctx, region, creds)
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("signing the MSK IAM token: %w", err)
			}
			return oauth.Auth{Token: token}, nil
		}), nil
	case AuthSCRAMSHA512:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism(), nil
	case AuthSCRAMSHA256:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism(), nil
	case AuthPlain:
		return plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism(), nil
	default:
		return nil, fmt.Errorf("unknown auth method %q", c.Auth)
	}
}
