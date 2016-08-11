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

package testing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"syscall"
	"testing"
	"time"

	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	consulstg "k8s.io/kubernetes/pkg/storage/consul"
	etcdtesting "k8s.io/kubernetes/pkg/storage/etcd/testing"
	"k8s.io/kubernetes/pkg/storage/storagebackend"
	utilnet "k8s.io/kubernetes/pkg/util/net"

	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"
)

// Consul implementation

type ConsulSharedTestServer struct {
	cmdLeave        *exec.Cmd
	config          storagebackend.Config
	client          *consulapi.Client
	clientConfig    *consulapi.Config
	CertificatesDir string
	ConfigFile      string
	Prefix          string
}

type ConsulServerPorts struct {
	dns    int
	http   int
	https  int
	rpc    int
	serf_l int
	serf_w int
	server int
}

var SHARED_CONSUL_SERVER_CONFIG = ConsulServerPorts{
	dns:    -1,
	https:  -1,
	http:   8505,
	rpc:    8506,
	serf_l: 8507,
	serf_w: 8508,
	server: 8509,
}

const SHARED_CONSUL_REFCOUNT_KEY = "k8s-test-server/refcount"

func consulServerConfigFromPorts(portConfig *ConsulServerPorts) (*ConsulSharedTestServer, error) {
	server := &ConsulSharedTestServer{
		config: storagebackend.Config{
			Type: storagebackend.StorageTypeConsul,
		},
	}
	config := map[string]interface{}{
		"datacenter": "k8s-testing",
		"log_level":  "INFO",
		"node_name":  "test_svr",
		"server":     true,
		"bind_addr":  "127.0.0.1",
	}

	addresses := make(map[string]string, 0)
	ports := make(map[string]int, 0)

	var err error
	valid := false

	server.CertificatesDir, err = ioutil.TempDir(os.TempDir(), "etcd_certificates")
	if err != nil {
		return nil, err
	}

	if portConfig.dns > 0 {
		addresses["dns"] = "127.0.0.1"
		ports["dns"] = portConfig.dns
	} else {
		ports["dns"] = -1
	}

	if portConfig.http > 0 || (portConfig.http == 0 && portConfig.https <= 0) {
		addresses["http"] = "127.0.0.1"
		ports["http"] = portConfig.http
		if portConfig.http > 0 {
			server.config.ServerList = []string{"http://127.0.0.1:" + strconv.Itoa(portConfig.http)}
		} else {
			server.config.ServerList = []string{"http://127.0.0.1"}
		}
		valid = true
	} else {
		ports["http"] = -1
	}

	if portConfig.https > 0 {
		server.config.CertFile = path.Join(server.CertificatesDir, "etcdcert.pem")
		if err = ioutil.WriteFile(server.config.CertFile, []byte(etcdtesting.CertFileContent), 0644); err != nil {
			return nil, err
		}
		server.config.KeyFile = path.Join(server.CertificatesDir, "etcdkey.pem")
		if err = ioutil.WriteFile(server.config.KeyFile, []byte(etcdtesting.KeyFileContent), 0644); err != nil {
			return nil, err
		}
		server.config.CAFile = path.Join(server.CertificatesDir, "ca.pem")
		if err = ioutil.WriteFile(server.config.CAFile, []byte(etcdtesting.CAFileContent), 0644); err != nil {
			return nil, err
		}
		config["cert_file"] = server.config.CertFile
		config["key_file"] = server.config.KeyFile
		config["ca_file"] = server.config.CAFile
		if portConfig.http <= 0 {
			config["verify_incoming"] = true
		}
		config["verify_outgoing"] = true
		addresses["https"] = "127.0.0.1"
		ports["https"] = portConfig.https
		server.config.ServerList = []string{"https://127.0.0.1:" + strconv.Itoa(portConfig.https)}
		valid = true
	} else {
		ports["https"] = -1
	}

	if !valid {
		return nil, errors.New("Invalid consul configuration specified with no client port")
	}

	if portConfig.serf_l > 0 {
		ports["serf_lan"] = portConfig.serf_l
	}

	if portConfig.serf_w > 0 {
		ports["serf_wan"] = portConfig.serf_w
	}

	if portConfig.server > 0 {
		ports["server"] = portConfig.server
	}

	if len(addresses) > 0 {
		config["addresses"] = addresses
	}
	if len(ports) > 0 {
		config["ports"] = ports
	}

	server.ConfigFile = path.Join(server.CertificatesDir, "consul.conf")
	configFile, err := os.Create(server.ConfigFile)
	if err != nil {
		return nil, err
	}
	encoder := json.NewEncoder(configFile)

	err = encoder.Encode(config)
	if err != nil {
		return nil, err
	}

	clientConfig := consulapi.DefaultConfig()
	if portConfig.https > 0 {
		clientConfig.Address = fmt.Sprintf("127.0.0.1:%d", portConfig.https)
		transport, err := newTransportForConsul(clientConfig.Address, server.config.CertFile, server.config.KeyFile, server.config.CAFile)
		if err != nil {
			panic(err)
		}
		clientConfig.HttpClient.Transport = transport
	} else {
		clientConfig.Address = fmt.Sprintf("127.0.0.1:%d", portConfig.http)
		customTransport := http.DefaultTransport
		customTransport.(*http.Transport).MaxIdleConnsPerHost = 100
		clientConfig.HttpClient.Transport = customTransport
	}
	server.client, err = consulapi.NewClient(clientConfig)
	server.clientConfig = clientConfig
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newTransportForConsul(address, certFile, keyFile, caFile string) (*http.Transport, error) {
	tlsConfig := &consulapi.TLSConfig{
		Address:  address,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}
	cfg, err := consulapi.SetupTLSConfig(tlsConfig)
	if err != nil {
		return nil, err
	}
	// Copied from etcd.DefaultTransport declaration.
	// TODO: Determine if transport needs optimization
	tr := utilnet.SetTransportDefaults(&http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: 500,
		TLSClientConfig:     cfg,
	})
	return tr, nil
}

func connectSharedConsulServer(t *testing.T, filePath string) (*ConsulSharedTestServer, uint64, error) {
	server, err := consulServerConfigFromPorts(&SHARED_CONSUL_SERVER_CONFIG)
	if err != nil {
		// TODO: cleanup files
		return nil, 0, err
	}
	launchesAttempted := 0
	for {
		// if the server is already up
		if isUp(server.client, t) {
			// try to secure a reference to it
			count, index, status := refModify(server.client, SHARED_CONSUL_REFCOUNT_KEY, 1, false)
			glog.Infof("refModify(%d) returned (%d, %d, %s)", 1, count, index, status)
			if status == REF_ACQUIRED {
				// we now have a reference on the shared server :)
				return server, index, nil
			}

			// server is either starting up or shutting down... let's spin
			// for a moment and hope it reaches either state soon
			<-time.After(100 * time.Millisecond)
			continue
		} else {
			// if the server is not started, then we start it
			nullFile, err := os.Open(os.DevNull)
			if err != nil {
				return nil, 0, err
			}
			var sysproc = &syscall.SysProcAttr{}
			var attr = os.ProcAttr{
				Env: os.Environ(),
				Files: []*os.File{
					nullFile,
					nullFile,
					nullFile,
				},
				Sys: sysproc,
			}
			process, err := os.StartProcess(filePath, []string{filePath, "agent", "-dev", "-config-file", server.ConfigFile}, &attr)
			if err != nil {
				// TODO: cleanup files
				return nil, 0, err
			}
			endedChan := make(chan int)
			launchesAttempted += 1
			go func(proc *os.Process, ended chan int) {
				_, err := proc.Wait()
				if err == nil {
					// exited normally
					ended <- 0
				}
				switch err.(type) {
				case *exec.ExitError:
					// exited with error
					ended <- 404 // status code not found - way to go golang
				default:
					// something else happened... oh well
					ended <- -1
				}
			}(process, endedChan)
			// spin until server is up or server has exitted
			exited := false
			start := time.Now()
			for time.Since(start) < 25*time.Second {
				timeout := time.After(100 * time.Millisecond)
				select {
				case <-endedChan:
					// our attempt to start a server has failed... if this happens
					// too many times, we should consider the possibility that it
					// will never succeed... :(
					exited = true
					break
				case <-timeout:
					if isUp(server.client, t) {
						process.Release()
						_, index, status := refModify(server.client, SHARED_CONSUL_REFCOUNT_KEY, 1, true)
						if status == REF_ACQUIRED {
							return server, index, nil
						}
						break
					}
				}
			}
			if !exited || launchesAttempted > 3 {
				// we didn't spin up nor exit within 25 seconds... something has gone very bad
				return nil, 0, errors.New("Unable to launch a shared consul server")
			}
		}
	}
	panic("ConsulRef OMG WTF NO")
}

func NewTestClientServer(t *testing.T, filePath string) (*ConsulSharedTestServer, error) {
	server, _, err := connectSharedConsulServer(t, filePath)
	if err != nil {
		t.Errorf("Unexpected failure starting consul server %#v", err)
		//return nil, err
	}

	// TODO: specify rcp-addr
	server.cmdLeave = exec.Command(filePath, "leave")

	err = server.waitUntilUp(t)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	server.Prefix = fmt.Sprintf("/registry")

	return server, err
}

type refStatus string

const (
	REF_ACQUIRED             = "REF_ACQUIRED"
	REF_FAILED_NOT_STARTED   = "REF_FAILED_NOT_STARTED"
	REF_FAILED_SHUTTING_DOWN = "REF_FAILED_SHUTTING_DOWN"
	REF_RELEASED             = "REF_RELEASED"
	REF_RELEASED_SHUTDOWN    = "REF_RELEASED_SHUTDOWN"
)

func refModify(client *consulapi.Client, key string, countDelta int, createIfNotFound bool) (int, uint64, refStatus) {
	KV := client.KV()
	for {
		kv, _, err := KV.Get(key, nil)
		var refCount int
		var version uint64
		if kv == nil {
			if err == nil && createIfNotFound {
				refCount = 0
				version = 0
			} else {
				if err != nil {
					glog.Infof("ConsulRef Failed to change ref-count by %d because %v returning %s", countDelta, err, REF_FAILED_NOT_STARTED)
				} else {
					glog.Infof("ConsulRef Failed to change ref-count by %d. returning %s", countDelta, REF_FAILED_NOT_STARTED)
				}
				return 0, 0, REF_FAILED_NOT_STARTED
			}
		} else {
			refCount, err = strconv.Atoi(string(kv.Value))
			if err != nil {
				glog.Errorf("Failed to parse refCount from %v got %d", kv.Value, refCount)
			}
			if refCount == 0 {
				glog.Infof("ConsulRef Failed to change ref-count by %d. returning %s", countDelta, REF_RELEASED_SHUTDOWN)
				return 0, 0, REF_FAILED_SHUTTING_DOWN
			}
			version = kv.ModifyIndex
		}
		refCount += countDelta
		var statusSuccess refStatus
		if countDelta > 0 {
			statusSuccess = REF_ACQUIRED
		} else {
			if refCount == 0 {
				statusSuccess = REF_RELEASED_SHUTDOWN
			} else {
				statusSuccess = REF_RELEASED
			}
		}
		kvNew := &consulapi.KVPair{
			Key:         key,
			Value:       []byte(strconv.Itoa(refCount)),
			ModifyIndex: version,
		}
		success, _, err := KV.CAS(kvNew, nil)
		if success {
			return refCount, version, statusSuccess
		}
		if err != nil {
			<-time.After(100 * time.Millisecond)
		}
	}
}

func isUp(client *consulapi.Client, t *testing.T) bool {
	_, _, err := client.KV().Get("until/consul/started", nil)
	if err == nil {
		return true
	}
	return false
}

// waitForEtcd wait until consul is propagated correctly
func (s *ConsulSharedTestServer) waitUntilUp(t *testing.T) error {
	for start := time.Now(); time.Since(start) < 25*time.Second; time.Sleep(100 * time.Millisecond) {
		if isUp(s.client, t) {
			return nil
		}
	}
	return fmt.Errorf("timeout on waiting for consul cluster")
}

func (s *ConsulSharedTestServer) Terminate(t *testing.T) {
	count, _, status := refModify(s.client, SHARED_CONSUL_REFCOUNT_KEY, -1, false)
	if status == REF_RELEASED_SHUTDOWN {
		err := s.cmdLeave.Run()
		if err != nil {
			// well damn... what do we do now?
			t.Errorf("unexpected error while stopping consul: %v", err)
		}
		fmt.Println("Shutting down dereferenced consul server")
	} else if status != REF_RELEASED {
		fmt.Println("ConsulRef Failed to release ref for unknown reason")
	} else {
		fmt.Println("ConsulRef Dereferenced consul serve... now has %d references remaining.", count)
	}
	// TODO: figure out some way to safely clean up the config directory
	//if err := os.RemoveAll(s.CertificatesDir); err != nil {
	//	t.Fatal(err)
	//}
}

func (s *ConsulSharedTestServer) CreateStorage(t *testing.T, codec runtime.Codec) (storage.Interface, error) {
	return consulstg.NewConsulStorage(*s.client, codec, s.Prefix, *s.clientConfig), nil
}
