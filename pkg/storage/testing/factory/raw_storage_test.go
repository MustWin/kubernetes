package testing

import (
	"bytes"
	"testing"

	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	"k8s.io/kubernetes/pkg/storage/generic"

	"golang.org/x/net/context"
)

var factory TestServerFactory
func TestMain(m *testing.M) {
	RunTestsForStorageFactories(func(fac TestServerFactory) int {
		factory = fac
		return m.Run()
	})
}

func RawObjEqual(lhs generic.RawObject, rhs generic.RawObject) bool {
	if lhs.Version != rhs.Version {
		return false
	}
	
	ttlDiff := lhs.TTL - rhs.TTL
	if ttlDiff > 1 || ttlDiff < -1 {
		return false
	}
	
	return bytes.Equal(lhs.Data, rhs.Data)
}

func TestNotFound(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	var raw generic.RawObject
	
	err := stg.Get(context.TODO(), "/does/not/exist", &raw)
	if err == nil {
		t.Error("Expected error NotFound, got success")
	} else if !storage.IsNotFound(err) {
		t.Errorf("Expected error NotFound, got error %#v", err)
	}
}

func TestCreate(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	var created generic.RawObject
	var read generic.RawObject
	var failed generic.RawObject
	
	err := stg.Create(context.TODO(), "/colide", []byte("succeed"), &created, 0)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
	}
	
	if created.Version == 0 {
		t.Error("Expected a non-zero Version number")
	}
	if !bytes.Equal(created.Data, []byte("succeed")) {
		t.Errorf("Expected Data==[]byte('succeed'), got '%v'", created.Data) 
	} 
	
	err = stg.Get(context.TODO(), "/colide", &read)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
	}
	if !RawObjEqual(created, read) {
		t.Errorf("Expected %#v, got %#v", created, read)
	}
	
	err = stg.Create(context.TODO(), "/colide", []byte("fail"), &failed, 0)
	if err == nil {
		t.Error("Expected error NodeExist, got success")
	} else if !storage.IsNodeExist(err) {
		t.Errorf("Expected error NodeExist, got error %#v", err)
	}
}

func TestDelete(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	var created generic.RawObject
	var deleted generic.RawObject
	
	alwaysTrue := func(raw *generic.RawObject) (bool, error) {
		return true, nil
	}
	alwaysFalse := func(raw *generic.RawObject) (bool, error) {
		return false, nil
	}
	alwaysError := func(raw *generic.RawObject) (bool, error) {
		return false, storage.NewInvalidObjError("/fake/key", "fake error")
	}
	
	err := stg.Delete(context.TODO(), "/does/not/exist", &deleted, alwaysFalse)
	if !storage.IsNotFound(err) {
		if err != nil {
			t.Errorf("Expected Error NotFound, got %#v", err)
		} else {
			t.Error("Expected Error NotFound, got success")
		}
	}
	
	err = stg.Create(context.TODO(), "/to/be/deleted", []byte("DeleteMe"), &created, 0)
	if err != nil {
		t.Errorf("Unexpected Error %#v", err)
	}
	
	err = stg.Delete(context.TODO(), "/to/be/deleted", &deleted, alwaysFalse)
	if !storage.IsTestFailed(err) {
		if err != nil {
			t.Errorf("Expected Error TestFailed, got %#v", err)
		} else {
			t.Error("Expected Error TestFailed, got success")
		}
	}
	
	err = stg.Create(context.TODO(), "/to/be/deleted", []byte("DeleteMe"), &created, 0)
	if err == nil || !storage.IsNodeExist(err) {
		if err == nil {
			t.Error("Expected Error NodeExist, got success... Delete ignored precondition(alwaysFalse)")
		} else {
			t.Errorf("Expected Error NodeExist, got %v#", err)
		}
	}
	
	err = stg.Delete(context.TODO(), "/to/be/deleted", &deleted, alwaysError)
	if err == nil || !storage.IsInvalidObj(err) {
		if err == nil {
			t.Error("Expected Error InvalidObj, got success... Delete dropped error from precondition(alwaysError)")
		} else {
			t.Error("Expected Error InvalidObj, got %#v... Delete translated error from precondition(alwaysError)", err)
		}
	}
	
	err = stg.Create(context.TODO(), "/to/be/deleted", []byte("DeleteMe"), &created, 0)
	if err == nil || !storage.IsNodeExist(err) {
		if err == nil {
			t.Error("Expected Error NodeExist, got success... Delete ignored precondition(alwaysFalse)")
		} else {
			t.Errorf("Expected Error NodeExist, got %v#", err)
		}
	}

	err = stg.Delete(context.TODO(), "/to/be/deleted", &deleted, alwaysTrue)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
	}
	
	if !RawObjEqual(created, deleted) {
		t.Error("Delete returned an object not equal to created object")
	}
	
	var got generic.RawObject
	err = stg.Get(context.TODO(), "/to/be/deleted", &got)
	if err == nil || !storage.IsNotFound(err) {
		if err == nil {
			t.Error("Expected Get to fail after Delete succeeded")
		} else {
			t.Errorf("Expected Error NotFound, got %#v", err)
		}
	}
	
	err = stg.Create(context.TODO(), "/to/be/deleted", []byte("DeleteMe"), &created, 0)
	if err != nil {
		t.Errorf("Failed to Create after Delete %v#", err)
	}
}

// used to copy data in case a misbehaving implementation is not respecting
// the constantcy of Data
func CloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func TestSet(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	var created generic.RawObject
	err := stg.Create(context.TODO(), "/to/be/overwritten", []byte("oldValue"), &created, 0)
	if err != nil {
		t.Errorf("Unexpected Error %#v", err)
	}
	
	overwritten := generic.RawObject{
		Data:		[]byte("newValue"),
		Version:	created.Version,
		TTL:		0,
		UID:		CloneBytes(created.UID),
	}
	success, err := stg.Set(context.TODO(), "/to/be/overwritten", &overwritten)
	if err != nil {
		t.Errorf("Unexpected Error %#v", err)
	}
	if !success {
		t.Error("Failed to overwrite created value")
	}
	
	failed := generic.RawObject{
		Data:		[]byte("neverWritten"),
		Version:	created.Version, // intentionally wrong version
		TTL:		0,
		UID:		CloneBytes(overwritten.UID),
	}
	success, err = stg.Set(context.TODO(), "/to/be/overwritten", &failed)
	if err != nil {
		t.Errorf("Unexpected Error %#v", err)
	}
	if success {
		t.Error("Set reports success for a request with an invalid version")
	}
	
	var gotten generic.RawObject
	err = stg.Get(context.TODO(), "/to/be/overwritten", &gotten)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
	}
	if !bytes.Equal(gotten.Data, []byte("newValue")) {
		t.Errorf("Expected Data == []byte('newValue'), got %#v", gotten)
	}
}

func TestList(t *testing.T) {
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	type KeyValSource struct{
		Key		string
		Value	[]byte
	}
	type ListTestReadMethod int
	const (
		Use_List	= 1 + iota
		Use_GetToList
		Use_Both
	)
	type ListTest struct{
		Name 		string
		Reason		string
		Source		[]KeyValSource
		Result		[][]byte
		KeyToRead	string
		ReadMethod	ListTestReadMethod
	}
	
	test_sources := []ListTest{
		ListTest{
			Name:		"Simple Usage",
			Reason:		"This test fails if even the simplest usage doesn't work",
			Source:		[]KeyValSource{
				{
					Key:	"/some/key/bar",
					Value:	[]byte("bar"),
				},
				{
					Key:	"/some/key/baz",
					Value:	[]byte("baz"),
				},
				{
					Key:	"/some/key/foo",
					Value:	[]byte("foo"),
				},
			},
			Result:		[][]byte{
				[]byte("bar"),
				[]byte("baz"),
				[]byte("foo"),
			},
			KeyToRead:	"/some/key",
			ReadMethod:	Use_Both,
		},
		ListTest{
			Name:		"Ensure List Enumerates Properly",
			Reason:		"This fails if List method doesn't recurse into directories or exclude self properly",
			Source:		[]KeyValSource{
				{
					Key:	"/some/key/directory1/baz",
					Value:	[]byte("baz"),
				},
				{
					Key:	"/some/key/directory1/foo",
					Value:	[]byte("foo"),
				},
				{
					Key:	"/some/key/directory2/bar",
					Value:	[]byte("bar"),
				},
				{
					Key:	"/some/foreign/path/wtf",
					Value:	[]byte("fail:foreign path included"),
				},
				{
					Key:	"/some/key",
					Value:	[]byte("fail:self included"),
				},
			},
			Result:		[][]byte{
				[]byte("baz"),
				[]byte("foo"),
				[]byte("bar"),
			},
			KeyToRead:	"/some/key",
			ReadMethod:	Use_List,
		},
		ListTest{
			Name:		"Ensure GetToList Enumerates Properly",
			Reason:		"This test fails if GetToList doesn't exclude subdirectories or self properly",
			Source:		[]KeyValSource{
				{
					Key:	"/some/key/bar",
					Value:	[]byte("bar"),
				},
				{
					Key:	"/some/key/baz",
					Value:	[]byte("baz"),
				},
				{
					Key:	"/some/key/hidden/fail",
					Value:	[]byte("fail: GetToList recursed into directory"),
				},
				{
					Key:	"/some/foreign/key/fail",
					Value:	[]byte("fail: GetToList enumerated a foreign directory"),
				},
				{
					Key:	"/some/key",
					Value:	[]byte("fail: GetToList enumerated self"),
				},
			},
			Result:		[][]byte{
				[]byte("bar"),
				[]byte("baz"),
			},
			KeyToRead:	"/some/key",
			ReadMethod:	Use_GetToList,
		},
	}
	
	doTest := func(t *testing.T, stg generic.InterfaceRaw, test *ListTest) {
		t.Logf("---Starting sub-test %s", test.Name)
		// setup initial data
		for _, source := range test.Source {
			err := stg.Create(context.TODO(), source.Key, source.Value, nil, 0)
			if err != nil && !storage.IsNodeExist(err) {
				t.Errorf("Unexpected Error %#v", err)
			}
		}
		// defer tear-down
		defer func() {
			for _, source := range test.Source {
				// reverse enumeration
				//source := test.Source[len(test.Source)-i-1]
				var raw generic.RawObject
				err := stg.Get(context.TODO(), source.Key, &raw)
				if err != nil {
					// that sucks... but we do what we can
					t.Logf("Failed to Get(%s) in order to delete it %#v", source.Key, err)
					continue
				}
				err = stg.Delete(context.TODO(), source.Key, &raw, func(raw *generic.RawObject) (bool, error) {
						return true, nil
					})
				if err != nil {
					t.Logf("Failed to Delete(%s, %v) error %#v", source.Key, raw, err)
				}
			}
		}()
		inspectResults := func(t *testing.T, test *ListTest, actual []generic.RawObject) {
			var expectedIndex int
			var failed bool
			for _, a := range actual {
				if expectedIndex < len(test.Result) {
					e := test.Result[expectedIndex]
					if bytes.Equal(e, a.Data) {
						expectedIndex += 1
					} else {
						failed = true  
					}
				} else {
					failed = true
				}
			}
			if expectedIndex < len(test.Result) {
				failed = true
				t.Logf("Missing %d expected results", len(test.Result) - expectedIndex)
			}
			if failed {
				t.Log(test.Reason)
				t.Fail()
			}
		}
		
		if test.ReadMethod == Use_List || test.ReadMethod == Use_Both {
			t.Log("testing List method")
			actual := make([]generic.RawObject,0)
			_, err := stg.List(context.TODO(), test.KeyToRead, "", &actual)
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}
			inspectResults(t, test, actual)
		}
		
		if test.ReadMethod == Use_GetToList || test.ReadMethod == Use_Both {
			t.Log("testing GetToList method")
			actual := make([]generic.RawObject,0)
			_, err := stg.GetToList(context.TODO(), test.KeyToRead, &actual)
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}
			inspectResults(t, test, actual)
		}
	}
	
	for _, test := range test_sources {
		doTest(t, stg, &test)
	}
}
