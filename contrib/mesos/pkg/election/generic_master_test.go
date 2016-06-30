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

package election

import (
	"testing"

	//etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"

	storagetesting "k8s.io/kubernetes/pkg/storage/testing/factory"
	"k8s.io/kubernetes/pkg/watch"
)

var factory storagetesting.TestServerFactory

func TestMain(m *testing.M) {
	storagetesting.RunTestsForStorageFactories(func(fac storagetesting.TestServerFactory) int {
		factory = fac
		return m.Run()
	})
}

func TestMasterOther(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)

	path := "foo"
	rawStorage := server.NewRawStorage()
	if err := rawStorage.Create(context.TODO(), path, []byte("baz"), nil, 0); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	master := NewGenericMasterElector(rawStorage)
	w := master.Elect(path, "bar")
	result := <-w.ResultChan()
	if result.Type != watch.Modified || result.Object.(Master) != "baz" {
		t.Errorf("unexpected event: %#v", result)
	}
	w.Stop()
}

func TestMasterNoOther(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)

	path := "foo"
	master := NewGenericMasterElector(server.NewRawStorage())
	w := master.Elect(path, "bar")
	result := <-w.ResultChan()
	if result.Type != watch.Modified || result.Object.(Master) != "bar" {
		t.Errorf("unexpected event: %#v", result)
	}
	w.Stop()
}

func TestMasterNoOtherThenConflict(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)

	path := "foo"
	rawStorage := server.NewRawStorage()
	master := NewGenericMasterElector(rawStorage)
	leader := NewGenericMasterElector(rawStorage)

	w_ldr := leader.Elect(path, "baz")
	result := <-w_ldr.ResultChan()
	w := master.Elect(path, "bar")
	result = <-w.ResultChan()
	if result.Type != watch.Modified || result.Object.(Master) != "baz" {
		t.Errorf("unexpected event: %#v", result)
	}
	w.Stop()
	w_ldr.Stop()
}
