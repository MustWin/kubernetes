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
	"errors"
	"os"
	"testing"
	
	consultest	"k8s.io/kubernetes/pkg/storage/consul/testing"
)

type ConsulSharedTestServerFactory struct {
	filePath string
}

func NewConsulSharedTestServerFactory() (TestServerFactory, error) {
	consul_path := os.Getenv("CONSUL_EXEC_FILEPATH")
	if consul_path == "" {
		return nil, errors.New("No path to consul executable found in 'CONSUL_EXEC_FILEPATH'")
	}
	return &ConsulSharedTestServerFactory{
		filePath: consul_path,
	}, nil
}

func (f *ConsulSharedTestServerFactory) NewTestClientServer(t *testing.T) (TestClientServer, error) {
	return consultest.NewTestClientServer(t, f.filePath)
}

func (f *ConsulSharedTestServerFactory) GetName() string {
	return "consul"
}
