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
	"testing"
	
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	etcdtesting	"k8s.io/kubernetes/pkg/storage/etcd/testing"
)

type Etcd2TestSvrFactory struct {}

func (f Etcd2TestSvrFactory) NewTestClientServer(t *testing.T) (TestClientServer, error) {
	return Etcd2TestSvr{
		s: etcdtesting.NewEtcdTestClientServer(t),
	}, nil
}

func (f Etcd2TestSvrFactory) GetName() string {
	return "etcd2"
}

type Etcd2TestSvr struct {
	s *etcdtesting.EtcdTestServer
}

func (s Etcd2TestSvr)	Terminate(t *testing.T) {
	s.s.Terminate(t)
}

func (s Etcd2TestSvr)	CreateStorage(t *testing.T, codec runtime.Codec) (storage.Interface, error) {
	return etcd.NewEtcdStorage(s.s.Client, codec, etcdtest.PathPrefix(), false, etcdtest.DeserializationCacheSize), nil
}

func NewETCD2TestServerFactory() (TestServerFactory, error) {
	return Etcd2TestSvrFactory{}, nil
}
