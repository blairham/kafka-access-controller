# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-stable releases (`v0.x.y`) make no API-stability promise -- breaking changes can land in any `v0.x` bump.

## [Unreleased]

### Changed

- Releases, the image and the chart are now built, signed and attested by
  the shared workflows in [blairham/.github](https://github.com/blairham/.github)
  (`go-release.yml`, `go-chart.yml`), so the keyless signing identity is that
  workflow rather than this repository's own; SECURITY.md has the new verify
  commands and how to verify 0.0.3 and earlier.

## [0.0.3] - 2026-10-08

The first complete release under the `kafka-access-controller` name; 0.0.2
published only its image.

### Added

- The Helm chart is published to `oci://ghcr.io/blairham/charts` on every
  release, signed by digest with keyless cosign like the image, so a pinned
  version installs without cloning the repository.

### Fixed

- The release workflow creates the GitHub release again. It named the
  release's commit, which GitHub refuses to `GITHUB_TOKEN` when the release
  range changes workflow files; the tag already exists, so it is no longer
  named.

## [0.0.2] - 2026-10-08

Published as the image `ghcr.io/blairham/kafka-access-controller:0.0.2`
only: creating the GitHub release failed, so there are no `kactl` archives
or release-attached provenance for this version. Use 0.0.3.

### Changed

- **Breaking:** renamed to `kafka-access-controller`, after the one thing it
  reconciles. The API group is now `kafka-access-controller.io`, the Go
  module `github.com/blairham/kafka-access-controller`, the image and Helm
  chart `ghcr.io/blairham/kafka-access-controller`, the metrics prefix
  `kafka_access_controller_`, and the revoke annotation
  `kafka-access-controller.io/revoke-on-delete`. Resources created under
  `kafka-controller.io` are not migrated; re-apply them under the new group.

## [0.0.1] - 2026-10-07

### Fixed

- A `KafkaAccess` that declares no ACLs plans without reading the
  principal's ACLs, so topics-only resources converge on a broker with no
  authorizer instead of failing with `SECURITY_DISABLED`.
- MSK IAM signs for `cluster.auth.region` whatever the broker is called. It
  now authenticates over SASL/OAUTHBEARER with AWS's MSK signer; AWS_MSK_IAM
  read the region only from an `*.amazonaws.com` host or `AWS_REGION`, so a
  PrivateLink alias or proxy failed every handshake.
- A reconcile that fails no longer leaves a stale `Converged` condition,
  pending list or warnings in status (k8s-controller-kit v0.0.1).

## [0.0.0] - 2026-10-07

### Added

- The `KafkaAccess` API (`kafka-controller.io/v1alpha1`): one resource per
  service declaring its cluster, principal, topics, consumer groups,
  transactional ids, idempotent write and SCRAM credential, with
  admission-time validation of every field.
- Topics: a managed topic is created, its partitions grown and the listed
  configs kept; fewer partitions or a new replication factor are reported in
  `status.warnings`, never attempted.
- ACLs under `authorization: acl`, recorded in `status.ownedACLs`; an ACL the
  spec stops declaring is deleted, and one it never declared is never
  touched.
- MSK IAM: `authorization: iam` plans no ACLs and renders the service's IAM
  policy into `status.iamPolicy`.
- SCRAM credentials on self-managed clusters, rotated when the password
  Secret changes.
- Names containing `*` or `?` are refused at admission and by `kactl`:
  Kafka ACLs and IAM policies read them as wildcards, so a literal group
  named `*` would have granted every group.
- Admin connections over MSK IAM, SCRAM-SHA-512/256, mTLS, SASL/PLAIN (TLS
  only) or none, with TLS verified by default.
- `Enforce` and `Observe` modes, `Ready` and `Converged` conditions, hourly
  drift correction, `revokeOnDelete`, and per-resource Prometheus metrics
  with alerts in the chart.
- `kactl plan`, `kactl apply` and `kactl iam-policy`.
- The Helm chart, and a signed multi-arch image on ghcr.io.
