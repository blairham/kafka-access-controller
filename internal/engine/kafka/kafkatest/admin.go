// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package kafkatest provides an in-memory Kafka admin for tests.
package kafkatest

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/blairham/kafka-controller/internal/engine/kafka"
)

// Admin is an in-memory cluster implementing kafka.Admin. Fields are
// exported so a test can arrange drift and inspect what was written.
type Admin struct {
	TopicMap  map[string]kafka.TopicState
	Granted   map[kafka.ACL]bool
	SCRAM     map[string][]string // user -> mechanisms
	Passwords map[string]string   // user/mechanism -> password
	Upserts   int
	mu        sync.Mutex
	FailACLs  bool
	// NoAuthorizer makes every ACL call fail as a broker with no authorizer
	// does (SECURITY_DISABLED), so a test can prove a plan never asks.
	NoAuthorizer bool
}

// New returns an empty cluster.
func New() *Admin {
	return &Admin{
		TopicMap:  map[string]kafka.TopicState{},
		Granted:   map[kafka.ACL]bool{},
		SCRAM:     map[string][]string{},
		Passwords: map[string]string{},
	}
}

// Topics implements kafka.Admin.
func (f *Admin) Topics(_ context.Context, names, keys []string) (map[string]kafka.TopicState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]kafka.TopicState{}
	for _, n := range names {
		st, ok := f.TopicMap[n]
		if !ok {
			continue
		}
		cfg := map[string]string{}
		for _, k := range keys {
			if v, ok := st.Config[k]; ok {
				cfg[k] = v
			}
		}
		st.Config = cfg
		out[n] = st
	}
	return out, nil
}

// CreateTopic implements kafka.Admin.
func (f *Admin) CreateTopic(_ context.Context, name string, p int32, rf int16, cfg map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.TopicMap[name]; ok {
		return kafka.ErrTopicExists
	}
	if rf == 0 {
		rf = 3
	}
	f.TopicMap[name] = kafka.TopicState{Partitions: p, ReplicationFactor: rf, Config: maps.Clone(cfg)}
	return nil
}

// SetPartitions implements kafka.Admin.
func (f *Admin) SetPartitions(_ context.Context, name string, total int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.TopicMap[name]
	st.Partitions = total
	f.TopicMap[name] = st
	return nil
}

// SetTopicConfig implements kafka.Admin.
func (f *Admin) SetTopicConfig(_ context.Context, name string, cfg map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.TopicMap[name]
	if st.Config == nil {
		st.Config = map[string]string{}
	}
	maps.Copy(st.Config, cfg)
	f.TopicMap[name] = st
	return nil
}

// ACLs implements kafka.Admin.
func (f *Admin) ACLs(_ context.Context, principal string) ([]kafka.ACL, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.NoAuthorizer {
		return nil, errors.New("SECURITY_DISABLED: No Authorizer is configured on the broker")
	}
	var out []kafka.ACL
	for a := range f.Granted {
		if a.Principal == principal {
			out = append(out, a)
		}
	}
	return out, nil
}

// CreateACL implements kafka.Admin.
func (f *Admin) CreateACL(_ context.Context, a kafka.ACL) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailACLs {
		return errors.New("CLUSTER_AUTHORIZATION_FAILED")
	}
	f.Granted[a] = true
	return nil
}

// DeleteACL implements kafka.Admin.
func (f *Admin) DeleteACL(_ context.Context, a kafka.ACL) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.Granted, a)
	return nil
}

// SCRAMMechanisms implements kafka.Admin.
func (f *Admin) SCRAMMechanisms(_ context.Context, user string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.SCRAM[user], nil
}

// UpsertSCRAM implements kafka.Admin.
func (f *Admin) UpsertSCRAM(_ context.Context, user, mech, pw string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Upserts++
	if !slices.Contains(f.SCRAM[user], mech) {
		f.SCRAM[user] = append(f.SCRAM[user], mech)
	}
	f.Passwords[user+"/"+mech] = pw
	return nil
}

// Close implements kafka.Admin.
func (*Admin) Close() {}

// Do runs fn with the cluster locked, for a test inspecting it while a
// controller is running.
func (f *Admin) Do(fn func(*Admin)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}
