/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package testing

import (
	"fmt"
	"os"
	"strings"

	"k8s.io/kubernetes/pkg/storage/storagebackend"
)

func RunTestsForStorageFactories(iterFn func(TestServerFactory) int) {
	FactoryTypesList := os.Getenv("KUBE_STORAGE")
	if FactoryTypesList == "" {
		//FactoryTypesList = "etcd2,etcd3,consul"
		FactoryTypesList = "etcd2,consul"
	}
	types := strings.Split(FactoryTypesList, ",")
	retCodes := make([]int, len(types))
	for index, FactoryType := range types {
		factory, err := GetFactory(FactoryType)
		if err != nil {
			fmt.Printf("Skipping tests for %s because %v", FactoryType, err)
			retCodes[index] = 1
			continue 
		}
		fmt.Printf("Running tests in %s mode\n", factory.GetName())
		retCodes[index] = iterFn(factory)
	}
	for _, code := range retCodes {
		if code > 0 {
			os.Exit(code)
		}
	}
	os.Exit(0)
}

// Create creates a storage backend based on given config.
func GetFactory(Type string) (TestServerFactory, error) {
	switch Type {
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD2:
		return NewETCD2TestServerFactory()
	//case storagebackend.StorageTypeETCD3:
		// TODO: We have the following features to implement:
		// - Support secure connection by using key, cert, and CA files.
		// - Honor "https" scheme to support secure connection in gRPC.
		// - Support non-quorum read.
		//return newETCD3Storage(c, codec)
	case storagebackend.StorageTypeConsul:
		return NewConsulSharedTestServerFactory()
	default:
		return nil, fmt.Errorf("unknown storage type: %s", Type)
	}
}

