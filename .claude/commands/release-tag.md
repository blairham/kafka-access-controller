---
description: Cut a v0.0.x release tag (the tag publishes the image, kactl archives and signed evidence).
argument-hint: "<version, e.g. v0.0.1>"
allowed-tools: Bash(make:*), Bash(go test:*), Bash(go vet:*), Bash(go build:*), Read, Edit, Glob, Grep
---

Cut a kafka-access-controller release. Pushing a `v*` tag runs
`.github/workflows/release.yml` (blairham/.github's `go-release.yml`), which
publishes the multi-arch image to ghcr.io, the `kactl` archives, a cosign-signed `checksums.txt`, a signed
image and SLSA provenance for both, with the tag's CHANGELOG section as the
notes. It fails if that section is missing, or if the chart's `appVersion`
does not match the tag. Target version: `$1` (must be `v0.0.x`, pre-stable;
the first tag is `v0.0.0`).

Steps:
1. Confirm the working tree is clean and we're on `main`. If dirty, stop and
   report.
2. Check CI on the commit you will tag (`gh run list --branch main -L 3`) --
   the tag must point at a green commit. Do not run `make check` locally; CI
   ran it. If CI is red, stop.
3. Verify `$1` is a valid, monotonically-increasing `v0.0.x` tag -- check
   `git tag --list 'v0.0.*'` and `git describe --tags --abbrev=0`.
4. In one change: move CHANGELOG.md's Unreleased entries under
   `## [X.Y.Z] - YYYY-MM-DD` (no `v`; the workflow matches that exact shape)
   and leave a fresh Unreleased section; set `version` and `appVersion` in
   `charts/kafka-access-controller/Chart.yaml` to `X.Y.Z`.
5. STOP and show the user the diff and the exact commands before anything
   that mutates the remote. `main` is protected, so the change lands as a
   `Release $1` PR (squash-merged once CI is green); then
   `git tag -s $1 -m "$1: <summary>" <merge commit>` and
   `git push origin $1`. Do not push tags without explicit confirmation.
6. Watch the Release workflow, and check the release carries the `kactl_*`
   archives, `checksums.txt`, `checksums.txt.sigstore.json` and
   `kafka-access-controller-$1.intoto.jsonl`, and that
   `ghcr.io/blairham/kafka-access-controller:X.Y.Z` is signed (SECURITY.md).
7. Watch the Publish chart workflow (`chart.yml`, same tag) and check that
   `ghcr.io/blairham/charts/kafka-access-controller:X.Y.Z` exists and is
   signed (SECURITY.md). If it failed, rerun it alone with
   `gh workflow run chart.yml -f tag=$1`.
