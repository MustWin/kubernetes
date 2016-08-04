/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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
	//storagetesting "k8s.io/kubernetes/pkg/storage/testing"
)

type TestResource struct {
	unversioned.TypeMeta `json:",inline"`
	api.ObjectMeta       `json:"metadata"`
	Value                int `json:"value"`
}

func (obj *TestResource) GetObjectKind() unversioned.ObjectKind { return &obj.TypeMeta }

func TestObjectVersioner(t *testing.T) {
	v := APIObjectVersioner{}
	if ver, err := v.ObjectResourceVersion(&TestResource{ObjectMeta: api.ObjectMeta{ResourceVersion: "5"}}); err != nil || ver != 5 {
		t.Errorf("unexpected version: %d %v", ver, err)
	}
	if ver, err := v.ObjectResourceVersion(&TestResource{ObjectMeta: api.ObjectMeta{ResourceVersion: "a"}}); err == nil || ver != 0 {
		t.Errorf("unexpected version: %d %v", ver, err)
	}
	obj := &TestResource{ObjectMeta: api.ObjectMeta{ResourceVersion: "a"}}
	if err := v.UpdateObject(obj, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obj.ResourceVersion != "5" || obj.DeletionTimestamp != nil {
		t.Errorf("unexpected resource version: %#v", obj)
	}
}

func TestCompareResourceVersion(t *testing.T) {
	five := &TestResource{ObjectMeta: api.ObjectMeta{ResourceVersion: "5"}}
	six := &TestResource{ObjectMeta: api.ObjectMeta{ResourceVersion: "6"}}

	versioner := APIObjectVersioner{}

	if e, a := -1, versioner.CompareResourceVersion(five, six); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
	if e, a := 1, versioner.CompareResourceVersion(six, five); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
	if e, a := 0, versioner.CompareResourceVersion(six, six); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
}
