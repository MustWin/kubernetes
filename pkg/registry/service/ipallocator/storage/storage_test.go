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
	"net"
	"strings"
	"testing"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/registry/registrytest"
	"k8s.io/kubernetes/pkg/registry/service/allocator"
	allocatorstg "k8s.io/kubernetes/pkg/registry/service/allocator/storage"
	"k8s.io/kubernetes/pkg/registry/service/ipallocator"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	storagefactory "k8s.io/kubernetes/pkg/storage/storagebackend/factory/testing"

	"golang.org/x/net/context"
)

var factory storagefactory.TestServerFactory

func TestMain(m *testing.M) {
	storagefactory.RunTestsForStorageFactories(func(fac storagefactory.TestServerFactory) int {
		factory = fac
		return m.Run()
	})
}

func newStorage(t *testing.T) (storagefactory.TestClientServer, ipallocator.Interface, allocator.Interface, storage.Interface) {
	internalStorage, server := registrytest.NewStorage(t, factory, "")
	_, cidr, err := net.ParseCIDR("192.168.1.0/24")
	if err != nil {
		t.Fatal(err)
	}

	var backing allocator.Interface
	storage := ipallocator.NewAllocatorCIDRRange(cidr, func(max int, rangeSpec string) allocator.Interface {
		mem := allocator.NewAllocationMap(max, rangeSpec)
		backing = mem
		storage := allocatorstg.NewStorage(mem, "/ranges/serviceips", api.Resource("serviceipallocations"), internalStorage)
		return storage
	})

	return server, storage, backing, internalStorage
}

func validNewRangeAllocation() *api.RangeAllocation {
	_, cidr, _ := net.ParseCIDR("192.168.1.0/24")
	return &api.RangeAllocation{
		Range: cidr.String(),
	}
}

func key() string {
	s := "/ranges/serviceips"
	return etcdtest.AddPrefix(s)
}

func TestEmpty(t *testing.T) {
	server, storage, _, _ := newStorage(t)
	defer server.Terminate(t)
	if err := storage.Allocate(net.ParseIP("192.168.1.2")); !strings.Contains(err.Error(), "cannot allocate resources of type serviceipallocations at this time") {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	server, storage, _, _ := newStorage(t)
	defer server.Terminate(t)
	if err := storage.Allocate(net.ParseIP("192.168.0.0")); err != ipallocator.ErrNotInRange {
		t.Fatal(err)
	}
}

func TestStore(t *testing.T) {
	server, storage, backing, si := newStorage(t)
	defer server.Terminate(t)
	if err := si.Create(context.TODO(), key(), validNewRangeAllocation(), nil, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := storage.Allocate(net.ParseIP("192.168.1.2")); err != nil {
		t.Fatal(err)
	}
	ok, err := backing.Allocate(1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Expected allocation to fail")
	}
	if err := storage.Allocate(net.ParseIP("192.168.1.2")); err != ipallocator.ErrAllocated {
		t.Fatal(err)
	}
}
