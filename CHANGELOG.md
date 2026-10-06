# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-stable releases (`v0.x.y`) make no API-stability promise -- breaking changes can land in any `v0.x` bump.

## [Unreleased]

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
