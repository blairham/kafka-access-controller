// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
)

// The region in the spec must reach the signature, whatever the broker is
// called. franz-go's AWS_MSK_IAM read it only from an *.amazonaws.com host or
// AWS_REGION, so a broker behind any other name failed every handshake.
func TestMSKIAMSignsWithTheSpecRegion(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")

	mech, err := mechanism(context.Background(), ConnConfig{Auth: AuthMSKIAM, Region: "us-west-2"})
	if err != nil {
		t.Fatal(err)
	}
	if mech.Name() != "OAUTHBEARER" {
		t.Fatalf("mechanism = %s, want OAUTHBEARER", mech.Name())
	}
	_, msg, err := mech.Authenticate(context.Background(), "kafka.kafka.svc.cluster.local:9094")
	if err != nil {
		t.Fatalf("authenticate against a non-AWS host: %v", err)
	}
	// OAUTHBEARER's first message is "n,,\x01auth=Bearer <token>\x01\x01".
	_, tok, ok := strings.Cut(string(msg), "auth=Bearer ")
	if !ok {
		t.Fatalf("no bearer token in %q", msg)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(tok, "\x01"))
	if err != nil {
		t.Fatalf("token is not base64url: %v", err)
	}
	u, err := url.Parse(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Query().Get("X-Amz-Credential"); !strings.Contains(got, "/us-west-2/kafka-cluster/") {
		t.Errorf("credential scope = %q, want the spec's us-west-2", got)
	}
	if u.Host != "kafka.us-west-2.amazonaws.com" {
		t.Errorf("signed host = %q", u.Host)
	}
}
