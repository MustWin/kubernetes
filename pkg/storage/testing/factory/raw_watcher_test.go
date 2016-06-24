package testing

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	//"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	"k8s.io/kubernetes/pkg/storage/generic"
	"k8s.io/kubernetes/pkg/watch"

	"golang.org/x/net/context"
)

type WatcherTestActionType int
const(
	TestActionCreate WatcherTestActionType = 1 + iota
	TestActionSet
	TestActionDelete
	TestActionRememberVersion	// records version of last operation for watchFrom value
	TestActionStartWatchValue
	TestActionStartWatchList
	TestActionStopWatch
)
type WatcherTestExpect struct {
	Type		watch.EventType
	ValueCur	[]byte
	ValuePrev	[]byte
}
type WatcherTestAction struct {
	Type		WatcherTestActionType
	Key			string
	Value		[]byte
	Expect		int
}

type WatcherTest struct {
	Actions			[]WatcherTestAction
	Expectations	[]WatcherTestExpect
}

func(test *WatcherTest) RunTest(t *testing.T, stg generic.InterfaceRaw) {
	var w generic.InterfaceRawWatch
	var rchan <-chan generic.RawEvent
	var versionUse uint64 = 0
	var versionLast uint64 = 0
	var currentlyExpecting int = 0
	var watcherStarted bool
	versions := make(map[string]uint64)
	
	for _, action := range test.Actions {
		var raw generic.RawObject
		switch {
		case action.Type == TestActionCreate:
			err := stg.Create(context.TODO(), action.Key, action.Value, &raw, 0)
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}
			versionLast = raw.Version
			versions[action.Key] = raw.Version
			
		case action.Type == TestActionSet:
			raw.Version = versions[action.Key]
			raw.Data = action.Value
			raw.TTL = 0
			succeed, err := stg.Set(context.TODO(), action.Key, &raw)
			if !succeed {
				panic("Unexpectedly failed to set a value for watcher tests")
			}
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}
			versionLast = raw.Version
			versions[action.Key] = raw.Version
		
		case action.Type == TestActionDelete:
			err := stg.Delete(context.TODO(), action.Key, nil, func(*generic.RawObject) (bool, error){ return true, nil })
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}

		case action.Type == TestActionRememberVersion:
			versionUse = versionLast

		case action.Type == TestActionStartWatchValue:
			watcher, err := stg.Watch(context.TODO(), action.Key, strconv.FormatUint(versionUse, 10))
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}
			w = watcher
			rchan = w.ResultChan()
			watcherStarted = true
			
		case action.Type == TestActionStartWatchList:
			watcher, err := stg.WatchList(context.TODO(), action.Key, strconv.FormatUint(versionUse, 10))
			if err != nil {
				t.Errorf("Unexpected Error %#v", err)
			}
			w = watcher
			rchan = w.ResultChan()
			watcherStarted = true
			
		case action.Type == TestActionStopWatch:
			w.Stop()
			// explicitly do NOT clear the watcherStarted flag... we test to see
			// that Stop() has actually stopped the watcher this way 
		}

		if !watcherStarted {
			// explicitly skip polling the watcher
			continue
		}
		
		if action.Expect > 0 {
			for expectCount := action.Expect; expectCount > 0; expectCount -= 1 {
				expect := &test.Expectations[currentlyExpecting]
				select {
				case got := <- rchan:
					// translate got into a comparable type for display purposes
					actual := WatcherTestExpect{
						Type: 		got.Type,
						ValueCur:	got.Current.Data,
						ValuePrev:	got.Previous.Data,
					}
					if currentlyExpecting >= len(test.Expectations) {
						panic("Bad Test: Expectations set has fewer items that expected")
					}
					if expect.Type != actual.Type || !bytes.Equal(expect.ValueCur, actual.ValueCur) || !bytes.Equal(expect.ValuePrev, actual.ValuePrev) {
						t.Logf("While performing %v", action)
						t.Errorf("Expected %v, got %v", expect, actual) 
					}
					
				case <-time.After(10000 * time.Millisecond):
					t.Logf("While performing %v", action)
					t.Errorf("Watcher failed to emit expected event or took too long to do so... expected %v", expect)
				}
				currentlyExpecting += 1
			}
		} else {
			select {
			case got := <- rchan:
				t.Logf("While performing %v", action)
				t.Errorf("Watch emitted unexpected event %v", got)

			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func TestWatchListInitialCreate(t *testing.T) {
	test := &WatcherTest{
		Actions: []WatcherTestAction{
			{
				Type:		TestActionCreate,
				Key:		"/some/key/bar",
				Value:		[]byte("bar"),
			},
			/*{
				Type:		TestActionCreate,
				Key:		"/some/key/baz",
				Value:		[]byte("baz"),
			},*/
			{
				Type:		TestActionCreate,
				Key:		"/some/foreign/key",
				Value:		[]byte("fail"),
			},
			{
				Type:		TestActionStartWatchList,
				Key:		"/some/key",
				//Expect:	2, // batches may come in arbitrary order... until the test is able to handle this, no batches
				Expect:		1,
			},
			{
				Type:		TestActionStopWatch,
			},
		},
		Expectations: []WatcherTestExpect{
			{
				Type:		watch.Added,
				ValueCur:	[]byte("bar"),
			},
			/*{
				Type:		watch.Added,
				ValueCur:	[]byte("baz"),
			},*/
		},
	}
	
	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)
	
	test.RunTest(t, stg)
}