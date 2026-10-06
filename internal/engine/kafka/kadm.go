// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// scramIterations is the work factor for new SCRAM passwords; Kafka accepts
// 4096 to 16384.
const scramIterations = 8192

// The enum tables are explicit in both directions rather than leaning on
// kmsg's String and Parse, so the plan text and the cluster's answer can
// never disagree over a spelling.
var (
	resourceTypes = map[string]kmsg.ACLResourceType{
		ResourceTopic:           kmsg.ACLResourceTypeTopic,
		ResourceGroup:           kmsg.ACLResourceTypeGroup,
		ResourceTransactionalID: kmsg.ACLResourceTypeTransactionalId,
		ResourceCluster:         kmsg.ACLResourceTypeCluster,
	}
	operations = map[string]kmsg.ACLOperation{
		"ALL":              kmsg.ACLOperationAll,
		"READ":             kmsg.ACLOperationRead,
		"WRITE":            kmsg.ACLOperationWrite,
		"CREATE":           kmsg.ACLOperationCreate,
		"DELETE":           kmsg.ACLOperationDelete,
		"ALTER":            kmsg.ACLOperationAlter,
		"DESCRIBE":         kmsg.ACLOperationDescribe,
		"DESCRIBE_CONFIGS": kmsg.ACLOperationDescribeConfigs,
		"ALTER_CONFIGS":    kmsg.ACLOperationAlterConfigs,
		"IDEMPOTENT_WRITE": kmsg.ACLOperationIdempotentWrite,
	}
	mechanisms = map[string]kadm.ScramMechanism{
		"SCRAM-SHA-256": kadm.ScramSha256,
		"SCRAM-SHA-512": kadm.ScramSha512,
	}
)

func reverse[K, V comparable](m map[K]V) map[V]K {
	out := make(map[V]K, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

var (
	resourceNames  = reverse(resourceTypes)
	operationNames = reverse(operations)
	mechanismNames = reverse(mechanisms)
)

// KadmAdmin implements Admin over franz-go's admin client.
type KadmAdmin struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// NewKadmAdmin wraps a connected client. Close closes it.
func NewKadmAdmin(cl *kgo.Client) *KadmAdmin {
	return &KadmAdmin{cl: cl, adm: kadm.NewClient(cl)}
}

// Close closes the underlying client.
func (k *KadmAdmin) Close() { k.cl.Close() }

// Topics implements Admin.
func (k *KadmAdmin) Topics(ctx context.Context, names, configKeys []string) (map[string]TopicState, error) {
	details, err := k.adm.ListTopics(ctx, names...)
	if err != nil {
		return nil, err
	}
	out := map[string]TopicState{}
	var existing []string
	for _, name := range names {
		d, ok := details[name]
		if !ok || errors.Is(d.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		if d.Err != nil {
			return nil, fmt.Errorf("topic %s: %w", name, d.Err)
		}
		st := TopicState{
			Partitions: int32(len(d.Partitions)),
			Config:     map[string]string{},
		} //nolint:gosec // partition counts fit
		if len(d.Partitions) > 0 {
			st.ReplicationFactor = int16(len(d.Partitions[0].Replicas)) //nolint:gosec // replica counts fit
		}
		out[name] = st
		existing = append(existing, name)
	}
	if len(existing) == 0 || len(configKeys) == 0 {
		return out, nil
	}
	rcs, err := k.adm.DescribeTopicConfigs(ctx, existing...)
	if err != nil {
		return nil, err
	}
	for _, rc := range rcs {
		if rc.Err != nil {
			return nil, fmt.Errorf("topic %s configs: %w", rc.Name, rc.Err)
		}
		st := out[rc.Name]
		for _, c := range rc.Configs {
			if slices.Contains(configKeys, c.Key) {
				st.Config[c.Key] = c.MaybeValue()
			}
		}
	}
	return out, nil
}

// CreateTopic implements Admin.
func (k *KadmAdmin) CreateTopic(ctx context.Context, name string, partitions int32, rf int16,
	config map[string]string,
) error {
	if rf == 0 {
		rf = -1 // broker default
	}
	var cfg map[string]*string
	if len(config) > 0 {
		cfg = map[string]*string{}
		for _, key := range slices.Sorted(maps.Keys(config)) {
			cfg[key] = kadm.StringPtr(config[key])
		}
	}
	_, err := k.adm.CreateTopic(ctx, partitions, rf, cfg, name)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("%w: %w", ErrTopicExists, err)
	}
	return err
}

// SetPartitions implements Admin.
func (k *KadmAdmin) SetPartitions(ctx context.Context, name string, total int32) error {
	rs, err := k.adm.UpdatePartitions(ctx, int(total), name)
	if err != nil {
		return err
	}
	return rs.Error()
}

// SetTopicConfig implements Admin. It is incremental: keys not named are
// left as they are.
func (k *KadmAdmin) SetTopicConfig(ctx context.Context, name string, config map[string]string) error {
	alters := make([]kadm.AlterConfig, 0, len(config))
	for _, key := range slices.Sorted(maps.Keys(config)) {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: kadm.StringPtr(config[key])})
	}
	rs, err := k.adm.AlterTopicConfigs(ctx, alters, name)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Err != nil {
			return fmt.Errorf("topic %s: %w %s", r.Name, r.Err, r.ErrMessage)
		}
	}
	return nil
}

// ACLs implements Admin. Entries this engine never writes (deny entries,
// resource types it has no field for) are dropped.
func (k *KadmAdmin) ACLs(ctx context.Context, principal string) ([]ACL, error) {
	b := kadm.NewACLs().
		Allow(principal).AllowHosts().
		AnyResource().
		ResourcePatternType(kadm.ACLPatternAny).
		Operations(kadm.OpAny)
	rs, err := k.adm.DescribeACLs(ctx, b)
	if err != nil {
		return nil, err
	}
	var out []ACL
	for _, r := range rs {
		if r.Err != nil {
			return nil, fmt.Errorf("%w %s", r.Err, r.ErrMessage)
		}
		for _, d := range r.Described {
			acl, ok := fromDescribed(d)
			if ok {
				out = append(out, acl)
			}
		}
	}
	return out, nil
}

func fromDescribed(d kadm.DescribedACL) (ACL, bool) {
	res, okRes := resourceNames[d.Type]
	op, okOp := operationNames[d.Operation]
	literal := d.Pattern == kadm.ACLPatternLiteral
	if !okRes || !okOp || d.Permission != kmsg.ACLPermissionTypeAllow ||
		(!literal && d.Pattern != kadm.ACLPatternPrefixed) {
		return ACL{}, false
	}
	return ACL{
		Principal: d.Principal,
		Host:      d.Host,
		Resource:  res,
		Name:      d.Name,
		Prefixed:  !literal,
		Operation: op,
	}, true
}

func builderFor(a ACL) (*kadm.ACLBuilder, error) {
	op, ok := operations[a.Operation]
	if !ok {
		return nil, fmt.Errorf("unknown ACL operation %q", a.Operation)
	}
	b := kadm.NewACLs().Allow(a.Principal).AllowHosts(a.Host).Operations(op)
	switch a.Resource {
	case ResourceTopic:
		b.Topics(a.Name)
	case ResourceGroup:
		b.Groups(a.Name)
	case ResourceTransactionalID:
		b.TransactionalIDs(a.Name)
	case ResourceCluster:
		b.Clusters()
	default:
		return nil, fmt.Errorf("unknown ACL resource %q", a.Resource)
	}
	if a.Prefixed {
		b.ResourcePatternType(kadm.ACLPatternPrefixed)
	} else {
		b.ResourcePatternType(kadm.ACLPatternLiteral)
	}
	return b, nil
}

// CreateACL implements Admin.
func (k *KadmAdmin) CreateACL(ctx context.Context, a ACL) error {
	b, err := builderFor(a)
	if err != nil {
		return err
	}
	rs, err := k.adm.CreateACLs(ctx, b)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Err != nil {
			return fmt.Errorf("%w %s", r.Err, r.ErrMessage)
		}
	}
	return nil
}

// DeleteACL implements Admin.
func (k *KadmAdmin) DeleteACL(ctx context.Context, a ACL) error {
	b, err := builderFor(a)
	if err != nil {
		return err
	}
	rs, err := k.adm.DeleteACLs(ctx, b)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Err != nil {
			return fmt.Errorf("%w %s", r.Err, r.ErrMessage)
		}
	}
	return nil
}

// SCRAMMechanisms implements Admin.
func (k *KadmAdmin) SCRAMMechanisms(ctx context.Context, user string) ([]string, error) {
	ds, err := k.adm.DescribeUserSCRAMs(ctx, user)
	if err != nil {
		return nil, err
	}
	d, ok := ds[user]
	if !ok || errors.Is(d.Err, kerr.ResourceNotFound) {
		return nil, nil
	}
	if d.Err != nil {
		return nil, fmt.Errorf("%w %s", d.Err, d.ErrMessage)
	}
	var out []string
	for _, ci := range d.CredInfos {
		if name, ok := mechanismNames[ci.Mechanism]; ok {
			out = append(out, name)
		}
	}
	return out, nil
}

// UpsertSCRAM implements Admin.
func (k *KadmAdmin) UpsertSCRAM(ctx context.Context, user, mechanism, password string) error {
	m, ok := mechanisms[mechanism]
	if !ok {
		return fmt.Errorf("unknown SCRAM mechanism %q", mechanism)
	}
	rs, err := k.adm.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User: user, Mechanism: m, Iterations: scramIterations, Password: password,
	}})
	if err != nil {
		return err
	}
	return rs.Error()
}
