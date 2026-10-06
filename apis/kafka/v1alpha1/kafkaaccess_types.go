// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AuthMethod is how the controller authenticates to the cluster.
// +kubebuilder:validation:Enum=msk-iam;scram-sha-512;scram-sha-256;mtls;plain;none
type AuthMethod string

const (
	// AuthMSKIAM signs the AWS_MSK_IAM SASL handshake with the ambient AWS
	// credentials. The production path on MSK, and the default.
	AuthMSKIAM AuthMethod = "msk-iam"

	// AuthSCRAMSHA512 reads a username and password from a Secret. MSK's
	// SCRAM listener accepts only SHA-512.
	AuthSCRAMSHA512 AuthMethod = "scram-sha-512"

	// AuthSCRAMSHA256 is SCRAM with SHA-256, for a self-managed cluster.
	AuthSCRAMSHA256 AuthMethod = "scram-sha-256"

	// AuthMTLS presents a client certificate from a Secret.
	AuthMTLS AuthMethod = "mtls"

	// AuthPlain is SASL/PLAIN, for a self-managed cluster. Only over TLS.
	AuthPlain AuthMethod = "plain"

	// AuthNone connects without authenticating, for a local cluster.
	AuthNone AuthMethod = "none"
)

// ClusterAuth selects how the controller's admin connection authenticates.
//
// +kubebuilder:validation:XValidation:rule="!has(self.method) || !(self.method in ['scram-sha-512','scram-sha-256','plain','mtls']) || has(self.secretRef)",message="cluster.auth.secretRef is required for scram, plain and mtls"
// +kubebuilder:validation:XValidation:rule="!has(self.method) || self.method != 'msk-iam' || (has(self.region) && size(self.region) > 0)",message="cluster.auth.region is required for msk-iam"
type ClusterAuth struct {
	// Method is msk-iam, scram-sha-512, scram-sha-256, mtls, plain or none.
	// +kubebuilder:default=msk-iam
	// +optional
	Method AuthMethod `json:"method,omitempty"`

	// Region is the AWS region the MSK IAM handshake is signed for. Required
	// for msk-iam.
	// +optional
	Region string `json:"region,omitempty"`

	// SecretRef names the Secret, in this resource's namespace, holding the
	// admin credentials: keys username and password for scram and plain;
	// tls.crt and tls.key for mtls.
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty"`
}

// TLSConfig configures transport security for the admin connection.
type TLSConfig struct {
	// Enabled turns TLS on. MSK's IAM, SCRAM and mTLS listeners all require it.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// CASecretRef names a Secret whose ca.crt is trusted in addition to the
	// system roots, for a private CA. MSK's certificates chain to public
	// roots and need none.
	// +optional
	CASecretRef *LocalObjectReference `json:"caSecretRef,omitempty"`

	// ServerName overrides the name verified on the broker certificates.
	// +optional
	ServerName string `json:"serverName,omitempty"`
}

// ClusterRef identifies the Kafka cluster to act on: MSK, or any cluster that
// speaks the Kafka admin API (Apache Kafka, Confluent Platform, Redpanda).
type ClusterRef struct {
	// BootstrapServers are host:port seeds for the admin connection.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	BootstrapServers []string `json:"bootstrapServers"`

	// TLS configures transport security. Defaults to enabled.
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// Auth is how the controller authenticates. Unrelated to
	// spec.authorization, which concerns the service's principal.
	// +optional
	Auth *ClusterAuth `json:"auth,omitempty"`

	// MSKClusterARN is the cluster's ARN. Required for authorization iam,
	// where the controller renders the IAM policy the service needs.
	// +kubebuilder:validation:Pattern=`^arn:aws[a-z-]*:kafka:[a-z0-9-]+:[0-9]{12}:cluster/[A-Za-z0-9-]+/[A-Za-z0-9-]+$`
	// +optional
	MSKClusterARN string `json:"mskClusterARN,omitempty"`
}

// Operation is a Kafka ACL operation.
// +kubebuilder:validation:Enum=All;Read;Write;Create;Delete;Alter;Describe;DescribeConfigs;AlterConfigs
type Operation string

// PatternType is how a resource name in an ACL matches.
// +kubebuilder:validation:Enum=Literal;Prefixed
type PatternType string

const (
	// PatternLiteral matches exactly one resource.
	PatternLiteral PatternType = "Literal"

	// PatternPrefixed matches every resource whose name starts with Name.
	PatternPrefixed PatternType = "Prefixed"
)

// Topic is one topic the service uses.
//
// +kubebuilder:validation:XValidation:rule="!has(self.manage) || !self.manage || !has(self.patternType) || self.patternType == 'Literal'",message="a Prefixed topic cannot be managed; manage one topic at a time"
// +kubebuilder:validation:XValidation:rule="!has(self.manage) || !self.manage || has(self.partitions)",message="a managed topic needs partitions"
type Topic struct {
	// Name is the topic name, or the prefix when patternType is Prefixed.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=249
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9._-]+$`
	Name string `json:"name"`

	// PatternType is Literal (the default) or Prefixed. It applies to the
	// ACLs; only a Literal topic can be managed.
	// +kubebuilder:default=Literal
	// +optional
	PatternType PatternType `json:"patternType,omitempty"`

	// Manage makes the controller create the topic if it is missing, grow its
	// partitions and set the configs listed here. Without it the topic is
	// only granted on. A managed topic is never deleted.
	// +optional
	Manage bool `json:"manage,omitempty"`

	// Partitions is the partition count of a managed topic. Kafka can only
	// add partitions, so a smaller value than the topic has is reported, not
	// applied.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Partitions *int32 `json:"partitions,omitempty"`

	// ReplicationFactor is used when a managed topic is created. Unset uses
	// the broker default. A change to an existing topic is reported, not
	// applied: it needs a partition reassignment.
	// +kubebuilder:validation:Minimum=1
	// +optional
	ReplicationFactor *int16 `json:"replicationFactor,omitempty"`

	// Config holds topic configs to set on a managed topic, such as
	// retention.ms. Keys not listed are left alone.
	// +kubebuilder:validation:MaxProperties=64
	// +optional
	Config map[string]string `json:"config,omitempty"`

	// Operations the principal is granted on the topic, under authorization
	// acl, or the matching IAM actions under iam.
	// +kubebuilder:validation:MaxItems=9
	// +optional
	Operations []Operation `json:"operations,omitempty"`
}

// ConsumerGroup is one consumer group the service joins.
type ConsumerGroup struct {
	// Name is the group id, or the prefix when patternType is Prefixed.
	// * and ? are refused: Kafka ACLs and IAM policies read them as wildcards.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^*?]+$`
	Name string `json:"name"`

	// PatternType is Literal (the default) or Prefixed.
	// +kubebuilder:default=Literal
	// +optional
	PatternType PatternType `json:"patternType,omitempty"`

	// Operations the principal is granted on the group. Read is what a
	// consumer needs to join and commit.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=4
	// +kubebuilder:validation:XValidation:rule="self.all(op, op in ['All', 'Read', 'Delete', 'Describe'])",message="a consumer group takes only All, Read, Delete and Describe"
	Operations []Operation `json:"operations"`
}

// TransactionalID is a transactional.id the service produces with.
type TransactionalID struct {
	// Name is the transactional id, or the prefix when patternType is
	// Prefixed. * and ? are refused: Kafka ACLs and IAM policies read them as
	// wildcards.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[^*?]+$`
	Name string `json:"name"`

	// PatternType is Literal (the default) or Prefixed.
	// +kubebuilder:default=Literal
	// +optional
	PatternType PatternType `json:"patternType,omitempty"`

	// Operations the principal is granted. A transactional producer needs
	// Write and Describe.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:XValidation:rule="self.all(op, op in ['All', 'Write', 'Describe'])",message="a transactional id takes only All, Write and Describe"
	Operations []Operation `json:"operations"`
}

// Authorization is how the cluster decides what the service's principal may
// do.
// +kubebuilder:validation:Enum=acl;iam
type Authorization string

const (
	// AuthorizationACL grants with Kafka ACLs. For SCRAM, mTLS and PLAIN
	// principals, on MSK or anywhere else.
	AuthorizationACL Authorization = "acl"

	// AuthorizationIAM is MSK IAM access control, which ignores Kafka ACLs
	// for IAM-authenticated clients. The controller manages topics and
	// renders the IAM policy the service's role needs into status.iamPolicy;
	// attaching it is the role owner's job.
	AuthorizationIAM Authorization = "iam"
)

// SCRAMCredential provisions the service's SCRAM user on a self-managed
// cluster. MSK refuses this API: there, a SCRAM user is a Secrets Manager
// secret associated with the cluster, which is control-plane work.
type SCRAMCredential struct {
	// Mechanism is SCRAM-SHA-512 (the default) or SCRAM-SHA-256.
	// +kubebuilder:validation:Enum=SCRAM-SHA-512;SCRAM-SHA-256
	// +kubebuilder:default=SCRAM-SHA-512
	// +optional
	Mechanism string `json:"mechanism,omitempty"`

	// PasswordSecretRef names the Secret, in this resource's namespace, whose
	// password key is the service's password.
	PasswordSecretRef LocalObjectReference `json:"passwordSecretRef"`
}

// LocalObjectReference names an object in this resource's namespace.
type LocalObjectReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// Mode selects whether the controller changes the cluster or only reports
// what it would change.
// +kubebuilder:validation:Enum=Observe;Enforce
type Mode string

const (
	// ModeEnforce applies the plan on every reconcile.
	ModeEnforce Mode = "Enforce"

	// ModeObserve builds the same plan and records it in status without
	// executing it.
	ModeObserve Mode = "Observe"
)

// KafkaAccessSpec declares the desired cluster state for one service.
//
// +kubebuilder:validation:XValidation:rule="!has(self.authorization) || self.authorization != 'acl' || (has(self.principal) && size(self.principal) > 0)",message="principal is required for authorization acl"
// +kubebuilder:validation:XValidation:rule="!has(self.authorization) || self.authorization != 'iam' || (has(self.cluster.mskClusterARN) && size(self.cluster.mskClusterARN) > 0)",message="cluster.mskClusterARN is required for authorization iam"
// +kubebuilder:validation:XValidation:rule="!has(self.authorization) || self.authorization != 'iam' || !has(self.scramCredential)",message="scramCredential does not apply under authorization iam"
type KafkaAccessSpec struct {
	// Cluster is the Kafka cluster to provision on.
	Cluster ClusterRef `json:"cluster"`

	// Authorization is acl (Kafka ACLs, the default) or iam (MSK IAM access
	// control).
	// +kubebuilder:default=acl
	// +optional
	Authorization Authorization `json:"authorization,omitempty"`

	// Principal is the Kafka principal granted to under authorization acl,
	// such as User:orders, or User:CN=orders for an mTLS client.
	// +kubebuilder:validation:Pattern=`^[A-Za-z]+:.+$`
	// +optional
	Principal string `json:"principal,omitempty"`

	// Topics the service uses.
	// +kubebuilder:validation:MaxItems=256
	// +optional
	Topics []Topic `json:"topics,omitempty"`

	// ConsumerGroups the service joins.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	ConsumerGroups []ConsumerGroup `json:"consumerGroups,omitempty"`

	// TransactionalIDs the service produces with.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	TransactionalIDs []TransactionalID `json:"transactionalIDs,omitempty"`

	// IdempotentWrite grants IdempotentWrite on the cluster, which an
	// idempotent producer needs on brokers before Kafka 3.0.
	// +optional
	IdempotentWrite bool `json:"idempotentWrite,omitempty"`

	// SCRAMCredential provisions the principal's SCRAM password on a
	// self-managed cluster. The user is never deleted.
	// +optional
	SCRAMCredential *SCRAMCredential `json:"scramCredential,omitempty"`

	// RevokeOnDelete deletes the ACLs this resource declares when it is
	// deleted. Topics and credentials are never deleted.
	// +kubebuilder:default=false
	// +optional
	RevokeOnDelete bool `json:"revokeOnDelete,omitempty"`

	// Mode is Enforce (apply the plan) or Observe (report in status what would
	// change, and change nothing). Observe also never revokes on delete.
	// +kubebuilder:default=Enforce
	// +optional
	Mode Mode `json:"mode,omitempty"`
}

// KafkaAccessStatus reports what the controller actually applied.
type KafkaAccessStatus struct {
	// Conditions are Ready (the last reconcile planned, and in Enforce applied,
	// without a fatal error) and Converged (the cluster already matches the
	// spec).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the .metadata.generation this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// AppliedPlanHash fingerprints the operations applied on the last
	// successful reconcile.
	// +optional
	AppliedPlanHash string `json:"appliedPlanHash,omitempty"`

	// LastAppliedTime is when that plan was applied.
	// +optional
	LastAppliedTime *metav1.Time `json:"lastAppliedTime,omitempty"`

	// OperationsApplied counts the operations in the last applied plan.
	// +optional
	OperationsApplied int `json:"operationsApplied,omitempty"`

	// Warnings holds the best-effort operations that failed on the last
	// apply, such as a partition decrease Kafka cannot make.
	// +optional
	Warnings []string `json:"warnings,omitempty"`

	// PendingOperations counts the operations the cluster still needs to
	// match the spec. In Enforce it comes from a re-plan after applying.
	// +optional
	PendingOperations int `json:"pendingOperations"`

	// Pending lists those operations, capped at the first 50.
	// +optional
	Pending []string `json:"pending,omitempty"`

	// LastPlannedTime is when the cluster was last planned against, in either
	// mode.
	// +optional
	LastPlannedTime *metav1.Time `json:"lastPlannedTime,omitempty"`

	// IAMPolicy is the IAM policy document the service's role needs, under
	// authorization iam. The controller does not attach it.
	// +optional
	IAMPolicy string `json:"iamPolicy,omitempty"`

	// SCRAMSecretVersion is the resourceVersion of the password Secret last
	// written to the cluster. Kafka cannot report a password, so a Secret
	// whose resourceVersion differs is what triggers a rotation.
	// +optional
	SCRAMSecretVersion string `json:"scramSecretVersion,omitempty"`

	// OwnedACLs is the inventory of ACLs this resource has declared, as
	// ownership keys. An ACL in it that the spec no longer declares is
	// deleted; an ACL not in it is never touched.
	// +optional
	OwnedACLs []string `json:"ownedACLs,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ka
// +kubebuilder:printcolumn:name="Principal",type=string,JSONPath=`.spec.principal`
// +kubebuilder:printcolumn:name="Authz",type=string,JSONPath=`.spec.authorization`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.pendingOperations`
// +kubebuilder:printcolumn:name="Applied",type=date,JSONPath=`.status.lastAppliedTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KafkaAccess is the topics, ACLs and credentials one service needs on a Kafka
// cluster.
type KafkaAccess struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaAccessSpec   `json:"spec,omitempty"`
	Status KafkaAccessStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KafkaAccessList is a list of KafkaAccess resources.
type KafkaAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaAccess `json:"items"`
}

func init() {
	register(&KafkaAccess{}, &KafkaAccessList{})
}
