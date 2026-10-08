# AGENTS.md — kafka-access-controller

Guidance for AI coding agents working in this repo. `CLAUDE.md` imports it, and
other tools read this file directly.

## Project Overview

A Kubernetes controller that provisions the **data plane** of a Kafka cluster
for a service: its topics, the ACLs on them, and its SCRAM credential. It works
on MSK (IAM, SCRAM or mTLS) and on any cluster that speaks the Kafka admin API.
The sibling of `database-access-controller`; both are built on `k8s-controller-kit`.

- Module: `github.com/blairham/kafka-access-controller`
- Go 1.26, `controller-runtime` v0.25, `franz-go` (kgo/kadm), `aws-sdk-go-v2`,
  `hashicorp/cli`, `github.com/blairham/k8s-controller-kit`

## Quick Reference

```sh
make test               # unit tests, no Kafka or cluster needed
make test-envtest       # controller against a real kube-apiserver, no cluster
make test-integration   # the engine against a real Kafka (starts kafkatest)
make kafka-down         # stop the integration Kafka -- do not leave it running
make generate           # deepcopy, CRDs, RBAC + sync them into the chart
make helm-lint          # lint and render the chart with the toggles flipped
make check-generated    # fail if the generated files are stale
make build              # bin/manager, bin/kactl
make docker-build       # image from source
make fmt                # gofumpt
```

## Project Structure

```
apis/kafka/v1alpha1/             KafkaAccess API + generated deepcopy
internal/engine/kafka/           Access, the plan (topics, ACLs, SCRAM), the kadm
                                 adapter and the connection (TLS, SASL, MSK IAM)
internal/engine/kafka/kafkatest/ an in-memory Admin for unit and envtest tests
internal/mskiam/                 the IAM policy a service needs under MSK IAM
internal/controller/kafkaaccess/ wiring onto k8s-controller-kit's reconciler
cmd/manager/                     the controller binary (k8s-controller-kit/manager)
cmd/kactl/                       plan, apply and iam-policy from a terminal
config/crd/, config/rbac/        generated manifests -- never hand-edit
charts/kafka-access-controller/         the Helm chart; the install path
hack/kafka-test/                 the integration broker
docs/design/                     why the design is shaped this way
```

## Built on k8s-controller-kit

The reconcile loop, the plan type, conditions, metrics, ownership and pruning
come from [k8s-controller-kit](https://github.com/blairham/k8s-controller-kit),
pinned in `go.mod` (first at `v0.0.0`). A change to any of those belongs in
the kit: land it there, tag it, then bump the version here. While working on
both at once, a temporary
`go mod edit -replace github.com/blairham/k8s-controller-kit=../k8s-controller-kit`
is fine locally, but never commit it: CI and the release build fetch modules
from the network and cannot see a sibling directory.

## Repository, CI and releases

Public at `github.com/blairham/kafka-access-controller` since 2026-10-06 (Apache-2.0
with a CLA). `main` is guarded by repository ruleset 24595901, as
database-access-controller's is: squash-only PRs, signed commits, linear history,
stale reviews dismissed, resolved review threads, and every check below
required on a branch up to date with `main`. Nobody bypasses it, admins
included; a release's CHANGELOG and chart bump land as a PR too.

- `.github/workflows/ci.yml` -- the required checks: **Pre-commit**,
  **Detect changed files** (skips code jobs for prose-only PRs without
  leaving a check pending), **Build and test** (build, `check-generated`, vet
  with all build tags, `go test -race`, envtest), **Kafka integration**
  (`make test-integration`: the same broker script as local), **Build image**
  and **Helm chart**. `codeql.yml` (security-extended) and `scorecard.yml` run
  alongside. Every action is pinned by commit SHA with a `# vX.Y.Z` comment;
  Dependabot moves the pins, the Go modules (aws-sdk, Kubernetes and
  franz-go as groups) and the Dockerfile bases.
- A **`v*` tag is what publishes**: `goreleaser.yml` runs GoReleaser, which
  pushes `ghcr.io/blairham/kafka-access-controller:<version>` (amd64 and arm64) and
  a `kactl` archive per platform, signed with keyless cosign, with SLSA
  provenance for the archives and the image. The notes are the tag's
  `CHANGELOG.md` section. The release refuses to publish when `Chart.yaml`'s
  `appVersion` does not match the tag, or the CHANGELOG has no section for
  it. Read `.claude/commands/release-tag.md` before cutting one; the first
  tag is `v0.0.0`.
- `osv-scanner.toml` ignores one advisory, with its reason; re-check it when
  the module graph changes.
- `SECURITY.md` states what the controller can do with its admin principal
  and what counts as a vulnerability -- including that status write access
  can trigger ACL deletion. Keep it true when behavior changes.

## Code Conventions

- Formatting and lint run as **pre-commit hooks**, never by hand (see the
  workspace `AGENTS.md`). golangci-lint v2 is pinned in go.mod's `tool` block
  and in `.pre-commit-config.yaml`; move the two together.
- Every `.go` file starts with the two-line SPDX header
  (`hack/boilerplate.go.txt`); `check-license-headers` enforces it.
- `.tool-versions` and go.mod's `go` directive must match.
- **Comments are short and explain why, not what.** The longer story belongs
  in `docs/design/kafka-access.md` or the commit message.
- **Never hand-edit generated files**: `zz_generated.deepcopy.go`,
  `config/crd/`, `config/rbac/`, and the chart's `templates/crds.yaml` and
  `templates/rbac.yaml` come from `make generate`. API field comments become
  CRD descriptions, so a change to them needs `make generate` too.
- **CEL rules need bounded inputs.** Every list in the spec has a `MaxItems`;
  without them the API server rejects the CRD for exceeding the CEL cost
  budget. A per-field `items:Enum` is silently overridden by the item type's
  own enum, which is why group and transactional-id operations are a CEL rule.
- The plan text never contains a secret. A test pins this for SCRAM.
- American English spelling.

## Testing

- **Unit** tests build plans against `kafkatest.Admin`.
- **envtest** runs the reconciler against a real kube-apiserver: every CEL rule
  and pattern (admit and reject), defaults, Enforce/Observe, the SCRAM version
  round trip through status, the IAM policy in status, revoke on delete, and
  pruning (a dropped topic, a switch to iam). Each
  test's manager watches only its own namespace.
- **Integration** (`make test-integration`) runs the engine against Apache
  Kafka 3.9.1 in KRaft mode with the standard authorizer: plan, apply,
  re-plan to zero (the proof that what is written reads back with the broker's
  spelling of every enum), drift repair, pruning, partition growth, revoke, and a SCRAM
  client proving the ACLs authorize what they should and nothing else.

- **Fuzz** targets on the input a user controls: `FuzzACLKey`
  (`internal/engine/kafka/key_fuzz_test.go`, ownership keys read back from
  status) and `FuzzPolicy` (`internal/mskiam/policy_fuzz_test.go`, the ARN and
  names an IAM policy is rendered from). `FuzzPolicy` found two bugs before
  the first release -- a literal `*` group granting every group under IAM,
  and invalid UTF-8 renaming the cluster in the policy -- and their inputs
  stay in `testdata/fuzz/` as seeds.

Every bug in plan construction gets a unit test; every bug about what a broker
accepts gets an integration test.

## Documentation

`docs/design/kafka-access.md` — the design decisions and what is out of scope.
Read it before changing the engine.
