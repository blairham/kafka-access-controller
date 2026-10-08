// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/blairham/k8s-controller-kit/plan"
	"sigs.k8s.io/yaml"

	kafkav1alpha1 "github.com/blairham/kafka-access-controller/apis/kafka/v1alpha1"
	"github.com/blairham/kafka-access-controller/internal/controller/kafkaaccess"
	"github.com/blairham/kafka-access-controller/internal/engine/kafka"
	"github.com/blairham/kafka-access-controller/internal/mskiam"
)

// envHelp documents where kactl reads what the controller reads from Secrets.
const envHelp = `
Credentials come from the environment rather than Secrets:

  KAFKA_USERNAME, KAFKA_PASSWORD   cluster.auth for scram and plain
  KAFKA_CERT_FILE, KAFKA_KEY_FILE  cluster.auth for mtls (PEM files)
  KAFKA_CA_FILE                    cluster.tls.caSecretRef (PEM file)
  KAFKA_SCRAM_PASSWORD             spec.scramCredential; needed by apply only

msk-iam uses the ambient AWS credentials, as the controller does.`

// loadAccess reads a KafkaAccess manifest from a file or stdin.
func loadAccess(path string) (*kafkav1alpha1.KafkaAccess, error) {
	var (
		raw []byte
		err error
	)
	if path == "-" || path == "" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(path) //nolint:gosec // the operator names the file
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var ka kafkav1alpha1.KafkaAccess
	if err := yaml.UnmarshalStrict(raw, &ka); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &ka, nil
}

// envLookup maps the Secret keys the controller would read onto environment
// variables.
func envLookup(_, key string) ([]byte, error) {
	vars := map[string]string{
		"username": "KAFKA_USERNAME",
		"password": "KAFKA_PASSWORD",
	}
	files := map[string]string{
		"tls.crt": "KAFKA_CERT_FILE",
		"tls.key": "KAFKA_KEY_FILE",
		"ca.crt":  "KAFKA_CA_FILE",
	}
	if v, ok := vars[key]; ok {
		if s := os.Getenv(v); s != "" {
			return []byte(s), nil
		}
		return nil, fmt.Errorf("set %s", v)
	}
	if v, ok := files[key]; ok {
		path := os.Getenv(v)
		if path == "" {
			return nil, fmt.Errorf("set %s to a PEM file", v)
		}
		return os.ReadFile(path) //nolint:gosec // the operator names the file
	}
	return nil, fmt.Errorf("no environment mapping for secret key %q", key)
}

// access maps the manifest. The SCRAM password comes from the environment;
// with none set, only a missing credential is planned, since kactl cannot
// know which Secret version the controller last wrote.
func access(ka *kafkav1alpha1.KafkaAccess, needPassword bool) (kafka.Access, error) {
	a := kafkaaccess.Access(ka.Spec)
	if a.SCRAM != nil {
		a.SCRAM.Password = os.Getenv("KAFKA_SCRAM_PASSWORD")
		if a.SCRAM.Password == "" {
			if needPassword {
				return a, errors.New("spec.scramCredential is set; set KAFKA_SCRAM_PASSWORD")
			}
			a.SCRAM.Password = "unused-by-plan"
		}
	}
	return a, kafka.Validate(a)
}

func buildPlan(
	ctx context.Context,
	ka *kafkav1alpha1.KafkaAccess,
	needPassword bool,
) (*plan.Plan, *kafka.Engine, error) {
	a, err := access(ka, needPassword)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := kafkaaccess.ConnConfig(ka, envLookup)
	if err != nil {
		return nil, nil, err
	}
	cfg.ClientID = "kactl"
	cl, err := kafka.Connect(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	eng := kafka.New(kafka.NewKadmAdmin(cl))
	p, err := eng.BuildPlan(ctx, a)
	if err != nil {
		eng.Close() //nolint:errcheck // already failing
		return nil, nil, err
	}
	return p, eng, nil
}

type planCommand struct{}

func (*planCommand) Synopsis() string {
	return "Print the operations the controller would run, without running them"
}

func (*planCommand) Help() string {
	return strings.TrimSpace(`
Usage: kactl plan -f <manifest>

  Connects to the cluster named in a KafkaAccess manifest, plans against its
  current state, and prints the operations the controller would run.

  Nothing is written. The plan is a diff: an operation is printed only when
  what it would create or change is missing. "0 operation(s)" means the
  cluster already matches the manifest.

Options:

  -f <path>   KafkaAccess manifest, or - for stdin.
` + envHelp)
}

func (*planCommand) Run(args []string) int {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	path := fs.String("f", "-", "KafkaAccess manifest, or - for stdin")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	ka, err := loadAccess(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	p, eng, err := buildPlan(context.Background(), ka, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer eng.Close() //nolint:errcheck // read-only path

	fmt.Print(p.Describe())
	fmt.Fprintf(os.Stderr, "\n%d operation(s), plan %s\n", p.Len(), p.Hash())
	return 0
}

type applyCommand struct{}

func (*applyCommand) Synopsis() string {
	return "Apply a KafkaAccess manifest directly, outside the controller"
}

func (*applyCommand) Help() string {
	return strings.TrimSpace(`
Usage: kactl apply -f <manifest> [-yes]

  Applies the same plan the controller would apply. Intended for a cluster the
  controller does not yet manage, or for recovering one by hand.

  Prints the plan and asks for confirmation unless -yes is given.

Options:

  -f <path>   KafkaAccess manifest, or - for stdin.
  -yes        Skip the confirmation prompt.
` + envHelp)
}

func (*applyCommand) Run(args []string) int {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	path := fs.String("f", "-", "KafkaAccess manifest, or - for stdin")
	assumeYes := fs.Bool("yes", false, "skip the confirmation prompt")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	ctx := context.Background()
	ka, err := loadAccess(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	p, eng, err := buildPlan(ctx, ka, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer eng.Close() //nolint:errcheck // best-effort teardown

	fmt.Print(p.Describe())
	fmt.Fprintf(os.Stderr, "\n%d operation(s) against %s\n",
		p.Len(), strings.Join(ka.Spec.Cluster.BootstrapServers, ","))
	if !*assumeYes {
		fmt.Fprint(os.Stderr, "apply? [y/N] ")
		var answer string
		fmt.Scanln(&answer) //nolint:errcheck // empty answer means no
		if !strings.EqualFold(strings.TrimSpace(answer), "y") {
			fmt.Fprintln(os.Stderr, "aborted")
			return 1
		}
	}
	res, err := p.Apply(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "apply failed after %d operation(s): %v\n", res.Applied, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "applied %d operation(s)\n", res.Applied)
	for _, w := range res.Warnings {
		fmt.Fprintf(os.Stderr, "warning: %s\n", w)
	}
	return 0
}

type iamPolicyCommand struct{}

func (*iamPolicyCommand) Synopsis() string {
	return "Print the IAM policy a manifest under authorization iam needs"
}

func (*iamPolicyCommand) Help() string {
	return strings.TrimSpace(`
Usage: kactl iam-policy -f <manifest>

  Renders the IAM policy document the service's role needs under MSK IAM
  access control: the same document the controller writes to
  status.iamPolicy. Needs no connection.

Options:

  -f <path>   KafkaAccess manifest, or - for stdin.
`)
}

func (*iamPolicyCommand) Run(args []string) int {
	fs := flag.NewFlagSet("iam-policy", flag.ContinueOnError)
	path := fs.String("f", "-", "KafkaAccess manifest, or - for stdin")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	ka, err := loadAccess(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	pol, err := mskiam.Render(ka.Spec.Cluster.MSKClusterARN, kafkaaccess.Access(ka.Spec))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(pol)
	return 0
}
