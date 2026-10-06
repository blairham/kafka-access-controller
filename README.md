# kafka-controller

[![CI](https://github.com/blairham/kafka-controller/actions/workflows/ci.yml/badge.svg)](https://github.com/blairham/kafka-controller/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/tag/blairham/kafka-controller?sort=semver&label=release)](https://github.com/blairham/kafka-controller/releases)
[![CodeQL](https://github.com/blairham/kafka-controller/actions/workflows/codeql.yml/badge.svg)](https://github.com/blairham/kafka-controller/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/blairham/kafka-controller/badge)](https://scorecard.dev/viewer/?uri=github.com/blairham/kafka-controller)
[![Go version](https://img.shields.io/github/go-mod/go-version/blairham/kafka-controller)](go.mod)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Kubernetes controller that provisions what a service needs **inside** a Kafka
cluster: its topics, the ACLs on them, and its SCRAM credential. One
`KafkaAccess` per service, on MSK (IAM, SCRAM or mTLS) or any cluster that
speaks the Kafka admin API.

```yaml
apiVersion: kafka-controller.io/v1alpha1
kind: KafkaAccess
metadata:
  name: orders
spec:
  cluster:
    bootstrapServers: [b-1.prod.abc123.c2.kafka.us-east-1.amazonaws.com:9098]
    auth: {method: msk-iam, region: us-east-1}
  principal: User:orders
  topics:
    - name: orders.events
      manage: true            # create it, grow it, keep these configs
      partitions: 12
      config: {retention.ms: "604800000"}
      operations: [Read, Write]
  consumerGroups:
    - name: orders
      operations: [Read]
```

- **A plan, not a script.** Every reconcile reads the cluster and plans only
  what is missing; a converged cluster plans nothing, and drift is repaired by
  exactly the operation that undoes it.
- **Observe before Enforce.** `mode: Observe` records the plan in status and
  writes nothing.
- **Honest about what Kafka cannot do.** Fewer partitions or a new replication
  factor are reported in `status.warnings`, never silently ignored.
- **MSK IAM aware.** Under `authorization: iam` no ACLs are planned (MSK
  ignores them for IAM clients); the IAM policy the service needs is written
  to `status.iamPolicy`.
- **Prunes only what it owns.** An ACL dropped from the spec is deleted if this
  resource created it (`status.ownedACLs`); anything else is never touched.
  Topics and credentials are never deleted.

`kactl plan -f manifest.yaml` prints what the controller would do against the
real cluster; `kactl iam-policy` renders the IAM policy offline.

See `docs/design/kafka-access.md` for the design and `examples/` for MSK IAM,
MSK SCRAM and self-managed manifests. Built on
[k8s-controller-kit](https://github.com/blairham/k8s-controller-kit), shared
with [database-controller](https://github.com/blairham/database-controller).

## Install

```sh
helm install kafka-controller charts/kafka-controller \
  --namespace kafka-controller-system --create-namespace
```

Read [`SECURITY.md`](SECURITY.md) before granting anyone `KafkaAccess`
create rights: the controller acts with an administrative principal.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Contributions need a signed
[CLA](CLA.md).

Licensed under the Apache License, Version 2.0.
