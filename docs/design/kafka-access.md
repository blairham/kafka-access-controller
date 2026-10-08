# Design: KafkaAccess

**Status:** implemented for MSK (IAM, SCRAM, mTLS) and any Kafka admin API;
local only, not yet run against a live MSK cluster
**Code:** `internal/engine/kafka/`, `internal/mskiam/`,
`internal/controller/kafkaaccess/`; the reconcile loop is `k8s-controller-kit`

## Purpose

Provision what a service needs **inside** a Kafka cluster -- its topics, the
ACLs on them, its SCRAM credential -- with a controller, the way
`database-access-controller` does for PostgreSQL. Creating the cluster is
control-plane work that ACK or Crossplane already covers; this does not do it.

Strimzi's `KafkaTopic` and `KafkaUser` cover the same ground only for clusters
Strimzi operates. MSK and other external clusters have no equivalent, which is
the gap this fills.

## Shape

One `KafkaAccess` per service, mirroring `DatabaseAccess`: the cluster, the
principal, and everything the principal needs. Separate topic and ACL
resources would let two services fight over one topic's config; one resource
per service makes ownership explicit (`manage: true` on the topic the service
owns, bare grants on topics it only uses).

## The plan

Built with `k8s-controller-kit/plan`, so `kactl plan` and the controller share one
code path. The plan is a diff, read first and then written:

- **SCRAM credential**, then **topics**, then **ACLs** -- a credential and a
  topic exist before anything grants on them.
- A **managed topic** is created when missing, grown when it has fewer
  partitions than the spec, and has the spec's configs set (incrementally:
  configs not listed are left alone).
- Kafka **cannot remove partitions**, and changing the **replication factor**
  needs a reassignment. Both are planned as a refusal: a best-effort step that
  always fails, so `status.warnings` and `status.pending` name the gap on every
  reconcile until the spec changes. Converging silently would hide a spec the
  cluster does not match.
- **ACLs are pruned by ownership.** `status.ownedACLs` is the inventory of
  ACLs this resource has declared (k8s-controller-kit's ownership mechanism).
  An ACL in it that the spec no longer declares -- a dropped topic or
  operation, a renamed principal, a switch to `authorization: iam` -- is
  deleted in a cleanup phase after the main plan. An ACL the resource never
  declared is never touched, even for the same principal. Kafka ACLs cannot be
  tagged, so an ACL the spec declares is adopted even if someone created it
  first, and two resources declaring the same ACL both own it: removing it
  from one deletes it until the other's next reconcile restores it. The first
  reconcile of a resource with an empty inventory prunes nothing.
  `revokeOnDelete` removes everything the resource declares or still owns.
- **Topics and credentials are never deleted**, on any path.

ACL operations are spelled the broker's way in the plan
(`User:orders ALLOW READ ON TOPIC orders LITERAL HOST *`), and the kadm
adapter maps every enum through explicit tables in both directions. The
integration suite's "apply, then re-plan to zero" is what proves the mapping
round-trips; a mutant inverting literal/prefixed fails it.

## Authorization: acl or iam

`spec.authorization` is about the **service's** principal, and is independent
of `cluster.auth`, which is how the **controller** connects.

- **acl** (default): Kafka ACLs, for SCRAM, mTLS and PLAIN principals -- on MSK
  or anywhere else.
- **iam**: MSK IAM access control ignores Kafka ACLs for IAM-authenticated
  clients, so no ACLs are planned. Topics are still managed. The controller
  renders the IAM policy the service's role needs into `status.iamPolicy`
  (`kactl iam-policy` prints the same document offline). It does **not**
  attach the policy: that is IAM control-plane work, and the role belongs to
  whoever owns the service. Each ACL operation maps to its IAM action plus the
  Describe action it depends on; a transactional producer also gets
  `WriteDataIdempotently`.

## SCRAM credentials

On a self-managed cluster, `scramCredential` writes the principal's password
with `AlterUserScramCredentials`. Kafka cannot report a password, so rotation
is detected by the password Secret's `resourceVersion` differing from
`status.scramSecretVersion`, which records the version last written. The
engine marks the rotation done on the shared `SCRAM` value when the upsert
succeeds, so the re-plan after applying converges and status records it.

Nothing watches the Secret: a rotation is applied at the next reconcile, at
worst the hourly drift pass. If a later step in the same plan fails fatally,
the version is not recorded and the next reconcile rewrites the same password
-- harmless.

**MSK refuses this API.** An MSK SCRAM user is a Secrets Manager secret
associated with the cluster (`BatchAssociateScramSecret`): control-plane work,
so not here. ACLs for an MSK SCRAM user are data plane and work normally.

## Connection

`cluster.auth.method`: `msk-iam` (default; MSK IAM over SASL/OAUTHBEARER,
signed for `auth.region` with the pod's ambient credentials and refreshed per
connection — not AWS_MSK_IAM, whose signer reads the region only from an
`*.amazonaws.com` broker host or `AWS_REGION`),
`scram-sha-512`/`scram-sha-256`, `mtls`, `plain` (refused without TLS) or
`none`. TLS defaults on; `tls.caSecretRef` adds a private CA. Credentials are
read from Secrets in the resource's own namespace only (`k8s-controller-kit/secret`).

## Modes and deletion

As `database-access-controller`: `mode: Observe` plans and reports without writing,
adding the finalizer, or revoking on delete. `revokeOnDelete` defaults to false
and the finalizer exists only while it is true.

## Not yet done

- Run against a live MSK cluster (IAM admin auth and `authorization: iam`).
  The SASL mechanism is franz-go's; the policy renderer is unit-tested against
  the ARN formats AWS documents, not against a cluster.
- Watching password Secrets for immediate rotation.
- Quotas, and delegation tokens.
- Moving `database-access-controller` onto k8s-controller-kit (`v0.0.0` is
  published, so nothing blocks it now).
