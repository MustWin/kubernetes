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

package main

import (
	"flag"
	"fmt"
	consulApi "github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	"os"
	"path"
	"testing"
)

type fakeConsulAgent struct {
	// TODO: Convert this to real fs to better simulate consul behavior.
	writes map[string]string
}

func (fa *fakeConsulAgent) ServiceRegister(service *consulApi.AgentServiceRegistration) error {
	key := fmt.Sprintf("%s", service.ID)
	value := fmt.Sprintf("%s:%d", service.Address, service.Port)
	fa.writes[key] = value
	return nil
}

func (fa *fakeConsulAgent) ServiceDeregister(serviceID string) error {
	delete(fa.writes, serviceID)
	return nil
}

// FIXME: instead of duplicating, extract these methods from kube2sky tests
func getConsulPathForA(name, namespace, subDomain string) string {
	return path.Join(basePath, subDomain, namespace, name)
}

type hostPort struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func getHostPort(service *kapi.Service) *hostPort {
	return &hostPort{
		Host: service.Spec.ClusterIP,
		Port: int(service.Spec.Ports[0].Port),
	}
}

func assertDnsServiceEntryInConsulAgent(t *testing.T, eca *fakeConsulAgent, serviceName, namespace string, expectedHostPort *hostPort) {
	key := getConsulPathForA(serviceName, namespace, serviceSubDomain)
	value := eca.writes[key]

	require.True(t, len(value) > 0, "entry not found.")

	actualHostPort := value
	host := expectedHostPort.Host
	port := expectedHostPort.Port

	expectedValue := fmt.Sprintf("%s:%d", host, port)
	assert.Equal(t, expectedValue, actualHostPort)
}

func newService(namespace, serviceName, clusterIP, portName string, portNumber int) kapi.Service {
	service := kapi.Service{
		ObjectMeta: kapi.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
		},
		Spec: kapi.ServiceSpec{
			ClusterIP: clusterIP,
			Ports: []kapi.ServicePort{
				{Port: int32(portNumber), Name: portName, Protocol: "TCP"},
			},
		},
	}
	return service
}

const (
	testDomain       = "cluster_local"
	basePath         = "cluster_local"
	serviceSubDomain = "svc"
	podSubDomain     = "pod"
)

type fakeConsulKV struct {
	writes map[string]*consulApi.KVPair
}

func (fk *fakeConsulKV) Get(key string, q *consulApi.QueryOptions) (*consulApi.KVPair, *consulApi.QueryMeta, error) {
	pair := fk.writes[key]
	return pair, nil, nil
}

func (fk *fakeConsulKV) Put(p *consulApi.KVPair, q *consulApi.WriteOptions) (*consulApi.WriteMeta, error) {
	key := p.Key
	fk.writes[key] = p
	return nil, nil
}

func (fk *fakeConsulKV) Delete(key string, w *consulApi.WriteOptions) (*consulApi.WriteMeta, error) {
	delete(fk.writes, key)
	return nil, nil
}

func newKube2Consul(ca *fakeConsulAgent, ck *fakeConsulKV) *kube2consul {
	return &kube2consul{
		consulAgent:    ca,
		consulKV:       ck,
		domain:         testDomain,
		endpointsStore: cache.NewStore(cache.MetaNamespaceKeyFunc),
		servicesStore:  cache.NewStore(cache.MetaNamespaceKeyFunc),
	}
}
func newPod(namespace, podName, podIP string) kapi.Pod {
	pod := kapi.Pod{
		ObjectMeta: kapi.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Status: kapi.PodStatus{
			PodIP: podIP,
		},
	}

	return pod
}

func TestMain(m *testing.M) {
	flag.Set("v", "3")
	os.Exit(m.Run())
}

func TestAddSinglePortService(t *testing.T) {
	const (
		testService   = "testservice"
		testNamespace = "default"
	)
	fck := &fakeConsulKV{make(map[string]*consulApi.KVPair)}
	fca := &fakeConsulAgent{make(map[string]string)}
	k2c := newKube2Consul(fca, fck)
	service := newService(testNamespace, testService, "1.2.3.4", "", 0)
	k2c.newService(&service)
	hostPort := getHostPort(&service)
	assert.Equal(t, 1, len(fca.writes))

	assertDnsServiceEntryInConsulAgent(t, fca, testService, testNamespace, hostPort)
}

func TestUpdateSinglePortService(t *testing.T) {
	const (
		testService   = "testservice"
		testNamespace = "default"
	)
	fck := &fakeConsulKV{make(map[string]*consulApi.KVPair)}
	fca := &fakeConsulAgent{make(map[string]string)}
	k2c := newKube2Consul(fca, fck)
	service := newService(testNamespace, testService, "1.2.3.4", "", 0)
	k2c.newService(&service)
	assert.Len(t, fca.writes, 1)
	newService := service
	newService.Spec.ClusterIP = "0.0.0.0"
	k2c.updateService(&service, &newService)
	hostPort := getHostPort(&newService)
	assertDnsServiceEntryInConsulAgent(t, fca, testService, testNamespace, hostPort)
}

func TestDeleteSinglePortService(t *testing.T) {
	const (
		testService   = "testservice"
		testNamespace = "default"
	)
	fck := &fakeConsulKV{make(map[string]*consulApi.KVPair)}
	fca := &fakeConsulAgent{make(map[string]string)}
	k2c := newKube2Consul(fca, fck)
	service := newService(testNamespace, testService, "1.2.3.4", "", 0)
	k2c.newService(&service)
	assert.Len(t, fca.writes, 1)
	k2c.removeService(&service)
	assert.Empty(t, fca.writes)
}

func newPod(namespace, podName, podIP string) kapi.Pod {
	pod := kapi.Pod{
		ObjectMeta: kapi.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Status: kapi.PodStatus{
			PodIP: podIP,
		},
	}

	return pod
}

func TestPodDns(t *testing.T) {
	const (
		testPodIP      = "1.2.3.4"
		sanitizedPodIP = "1-2-3-4"
		testNamespace  = "default"
		testPodName    = "testPod"
	)
	fck := &fakeConsulKV{make(map[string]*consulApi.KVPair)}
	fca := &fakeConsulAgent{make(map[string]string)}
	k2c := newKube2Consul(fca, fck)

	// create pod without ip address yet
	pod := newPod(testNamespace, testPodName, "")
	k2c.handlePodCreate(&pod)
	assert.Empty(t, fck.writes)

	// create pod
	pod = newPod(testNamespace, testPodName, testPodIP)
	k2c.handlePodCreate(&pod)
	// assertDnsPodEntryInConsulClient(t, fck, sanitizedPodIP, testNamespace)

	// update pod with same ip
	newPod := pod
	newPod.Status.PodIP = testPodIP
	k2c.handlePodUpdate(&pod, &newPod)
	// assertDnsPodEntryInConsulClient(t, fck, sanitizedPodIP, testNamespace)

	// update pod with different ip's
	newPod = pod
	newPod.Status.PodIP = "4.3.2.1"
	k2c.handlePodUpdate(&pod, &newPod)
	// assertDnsPodEntryInConsulClient(t, fck, sanitizedPodIP, testNamespace)
	// assertDnsPodEntryNotInConsulClient(t, fck, "1-2-3-4", testNamespace)

	// Delete the pod
	// k2c.handlePodDelete(&newPod)
	//assert.Empty(t, fck.writes)
}
