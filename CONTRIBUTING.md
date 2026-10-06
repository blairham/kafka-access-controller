# Contributing

Thank you for considering a contribution. Please read the two rules below
before opening a pull request -- both are non-negotiable.

## 1. Nothing secret reaches plan text, status, events or logs

The controller holds an administrative principal on someone's Kafka cluster
and reads their SCRAM passwords, client keys and CA bundles. Plan steps are
printed by `kactl plan`, recorded in `status.pending` and `status.warnings`,
and logged. A step's text names a user and a mechanism, never a password; a
new step or status field that carries a credential will not be merged. A
test pins this for SCRAM.

The same goes for deletion: the controller deletes only ACLs recorded in
`status.ownedACLs` and never topics or credentials. A change that widens what
it deletes needs a design discussion in an issue first.

## 2. The Contributor License Agreement

Contributions require a signed CLA; the text is in [`CLA.md`](CLA.md).

**Why.** The project may need to offer different licensing terms in future.
That is only possible if one party can license the whole work, and copyright
in a contribution stays with its author unless licensed onward.

The CLA does **not** take your copyright. You keep it; you grant a license
broad enough to include sublicensing, and you affirm the work is your own --
including that no employer holds rights to it.

## Practical

- Open an issue before a large change, so the design can be agreed first.
  Read [`docs/design/kafka-access.md`](docs/design/kafka-access.md) before
  changing the engine. Reconcile-loop changes belong in
  [k8s-controller-kit](https://github.com/blairham/k8s-controller-kit).
- Work on a branch and open a pull request against `main`.
- Commit messages explain the **why**, not a restatement of the diff.
  Conventional-commit prefixes (`feat:`, `fix:`, `docs:`, ...).
- Commits must be signed.
- Every `.go` file carries the two-line SPDX header; the pre-commit hook fails
  without it. Generated files get it from `hack/boilerplate.go.txt`.
- Never hand-edit generated files. A change under `apis/` or to a
  kubebuilder marker needs `make generate`, committed with it; CI fails on
  stale output.
- Every list in the spec carries a `MaxItems`: without one the API server
  rejects the CRD for exceeding the CEL cost budget.
- Every user-visible change gets a line under `[Unreleased]` in
  [`CHANGELOG.md`](CHANGELOG.md); a release's notes are that section.
- `pre-commit install` once per checkout. The hooks format, lint, scan for
  secrets and check for vulnerable dependencies on every commit; never bypass
  them with `--no-verify`.
- `go test -race ./...` must pass. **New functionality comes with tests in
  the same pull request**, and a pull request that adds behavior without them
  will not be merged. A bug in plan construction gets a unit test; a bug
  about what a broker accepts gets an integration test
  (`make test-integration`, needs Docker); a bug in the reconcile contract
  or the CRD schema gets an envtest (`make test-envtest`). Input a user
  controls that is parsed -- ownership keys, ARNs -- has a fuzz target.

Please also read the [Code of Conduct](CODE_OF_CONDUCT.md). Security issues go
through [SECURITY.md](SECURITY.md), never a public issue.
