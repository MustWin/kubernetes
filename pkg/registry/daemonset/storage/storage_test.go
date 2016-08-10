/*
Copyright 2015 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"testing"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/registry/generic"
	"k8s.io/kubernetes/pkg/registry/registrytest"
	"k8s.io/kubernetes/pkg/runtime"
	storagefactory "k8s.io/kubernetes/pkg/storage/storagebackend/factory/testing"
)

var factory storagefactory.TestServerFactory

func TestMain(m *testing.M) {
	storagefactory.RunTestsForStorageFactories(func(fac storagefactory.TestServerFactory) int {
		factory = fac
		return m.Run()
	})
}

func newStorage(t *testing.T) (*REST, *StatusREST, storagefactory.TestClientServer) {
	internalStorage, server := registrytest.NewStorage(t, factory, extensions.GroupName)
	restOptions := generic.RESTOptions{Storage: internalStorage, Decorator: generic.UndecoratedStorage, DeleteCollectionWorkers: 1}
	daemonSetStorage, statusStorage := NewREST(restOptions)
	return daemonSetStorage, statusStorage, server
}

func newValidDaemonSet() *extensions.DaemonSet {
	return &extensions.DaemonSet{
		ObjectMeta: api.ObjectMeta{
			Name:      "foo",
			Namespace: api.NamespaceDefault,
		},
		Spec: extensions.DaemonSetSpec{
			Selector: &unversioned.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
			Template: api.PodTemplateSpec{
				ObjectMeta: api.ObjectMeta{
					Labels: map[string]string{"a": "b"},
				},
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:            "test",
							Image:           "test_image",
							ImagePullPolicy: api.PullIfNotPresent,
						},
					},
					RestartPolicy: api.RestartPolicyAlways,
					DNSPolicy:     api.DNSClusterFirst,
				},
			},
		},
	}
}

var validDaemonSet = newValidDaemonSet()

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Store)
	ds := newValidDaemonSet()
	ds.ObjectMeta = api.ObjectMeta{}
	test.TestCreate(
		// valid
		ds,
		// invalid (invalid selector)
		&extensions.DaemonSet{
			Spec: extensions.DaemonSetSpec{
				Selector: &unversioned.LabelSelector{MatchLabels: map[string]string{}},
				Template: validDaemonSet.Spec.Template,
			},
		},
		// invalid update strategy
		&extensions.DaemonSet{
			Spec: extensions.DaemonSetSpec{
				Selector: validDaemonSet.Spec.Selector,
				Template: validDaemonSet.Spec.Template,
			},
		},
	)
}

func TestUpdate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Store)
	test.TestUpdate(
		// valid
		newValidDaemonSet(),
		// updateFunc
		func(obj runtime.Object) runtime.Object {
			object := obj.(*extensions.DaemonSet)
			object.Spec.Template.Spec.NodeSelector = map[string]string{"c": "d"}
			object.Spec.Template.Spec.DNSPolicy = api.DNSDefault
			return object
		},
		// invalid updateFunc
		func(obj runtime.Object) runtime.Object {
			object := obj.(*extensions.DaemonSet)
			object.Name = ""
			return object
		},
		func(obj runtime.Object) runtime.Object {
			object := obj.(*extensions.DaemonSet)
			object.Spec.Template.Spec.RestartPolicy = api.RestartPolicyOnFailure
			return object
		},
	)
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Store)
	test.TestDelete(newValidDaemonSet())
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Store)
	test.TestGet(newValidDaemonSet())
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Store)
	test.TestList(newValidDaemonSet())
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	test := registrytest.New(t, storage.Store)
	test.TestWatch(
		validDaemonSet,
		// matching labels
		[]labels.Set{
			{"a": "b"},
		},
		// not matching labels
		[]labels.Set{
			{"a": "c"},
			{"foo": "bar"},
		},
		// matching fields
		[]fields.Set{
			{"metadata.name": "foo"},
		},
		// notmatching fields
		[]fields.Set{
			{"metadata.name": "bar"},
			{"name": "foo"},
		},
	)
}

// TODO TestUpdateStatus
