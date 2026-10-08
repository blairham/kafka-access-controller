// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package v1alpha1 contains the KafkaAccess API, which declares the topics,
// ACLs and credentials a service needs on a Kafka cluster.
//
// +kubebuilder:object:generate=true
// +groupName=kafka-access-controller.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group and version for this API.
	GroupVersion = schema.GroupVersion{Group: "kafka-access-controller.io", Version: "v1alpha1"}

	// SchemeBuilder registers this API's types with a runtime.Scheme. It is
	// apimachinery's builder, not controller-runtime's: an API package should
	// depend on as little as possible, which is why that one is retired.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds this API's types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme

	knownTypes []runtime.Object
)

// register records types for addKnownTypes; each type file calls it from init.
func register(objs ...runtime.Object) {
	knownTypes = append(knownTypes, objs...)
}

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, knownTypes...)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
