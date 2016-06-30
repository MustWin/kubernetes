package testing

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	//"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	"k8s.io/kubernetes/pkg/storage/generic"
	"k8s.io/kubernetes/pkg/watch"

	"golang.org/x/net/context"
)

type WatcherTestActionType int

const (
	TestActionCreate WatcherTestActionType = 1 + iota
	TestActionSet
	TestActionDelete
	TestActionRememberVersion // records version of last operation for watchFrom value
	TestActionStartWatchValue
	TestActionStartWatchList
	TestActionStopWatch
)

type WatcherTestExpect struct {
	Type      watch.EventType
	ValueCur  []byte
	ValuePrev []byte
}

type WatcherTestAction struct {
	Type     WatcherTestActionType
	Key      string
	Value    []byte
	Expect   int
	ExpectOf int
}

type WatcherTest struct {
	Actions      []WatcherTestAction
	Expectations []WatcherTestExpect
}

type SortedExpectations []*WatcherTestExpect

func (expectations SortedExpectations) Len() int { return len(expectations) }
func (expectations SortedExpectations) Swap(i, j int) {
	expectations[i], expectations[j] = expectations[j], expectations[i]
}
func CompareExpectations(l, r *WatcherTestExpect) int {
	compResult := strings.Compare(string(l.Type), string(r.Type))
	if compResult != 0 {
		return compResult
	}

	compResult = bytes.Compare(l.ValueCur, r.ValueCur)
	if compResult != 0 {
		return compResult
	}

	return bytes.Compare(l.ValuePrev, r.ValuePrev)
}

func (expectations SortedExpectations) Less(i, j int) bool {
	return CompareExpectations(expectations[i], expectations[j]) < 0
}

func (expectations SortedExpectations) String() string {
	if len(expectations) == 0 {
		return "{}"
	}

	output := fmt.Sprintf("{%v", *expectations[0])

	for i := 1; i < len(expectations); i += 1 {
		if expectations[i] == nil {
			output += ", nil"
		} else {
			output += fmt.Sprintf(", %v", *expectations[i])
		}
	}

	return output + "}"
}

func (test *WatcherTest) RunTest(t *testing.T, stg generic.InterfaceRaw) {
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
			err := stg.Delete(context.TODO(), action.Key, nil, func(*generic.RawObject) (bool, error) { return true, nil })
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
			expectOf := action.ExpectOf
			if action.Expect > expectOf {
				expectOf = action.Expect
			}
			if currentlyExpecting+expectOf > len(test.Expectations) {
				panic("Bad Test: Expectations set has fewer items that expected")
			}
			expectations := make(SortedExpectations, expectOf)
			for i := 0; i < expectOf; i += 1 {
				expectations[i] = &test.Expectations[currentlyExpecting+i]
			}
			currentlyExpecting += expectOf
			sort.Sort(expectations)
			remaining := expectations
			received := make(SortedExpectations, 0)
			for expectCount := action.Expect; expectCount > 0; expectCount -= 1 {
				select {
				case got, ok := <-rchan:
					if ok {
						// translate got into a comparable type for display purposes
						actual := &WatcherTestExpect{
							Type:      got.Type,
							ValueCur:  got.Current.Data,
							ValuePrev: got.Previous.Data,
						}
						received = append(received, actual)
						// try to find actual in expectations
						x := sort.Search(len(remaining), func(i int) bool { return CompareExpectations(actual, remaining[i]) <= 0 })

						if x >= len(remaining) || CompareExpectations(actual, remaining[x]) != 0 {
							t.Logf("While performing %v", action)
							t.Errorf("Got unexpected %v, while expecting got one of %v", actual, expectations)
						} else {
							if x > 0 {
								remaining = append(remaining[0:x], remaining[x+1:]...)
							} else {
								remaining = remaining[1:]
							}
						}
					} else {
						t.Logf("While performing %v", action)
						t.Errorf("Result Channel closed while expecting one of %v", expectations)
					}

				case <-time.After(10000 * time.Millisecond):
					t.Logf("While performing %v", action)
					t.Logf("Got %v", received)
					t.Errorf("Watcher failed to emit expected event or took too long to do so... expected one of %v", expectations)
				}
			}
		}
		select {
		case got, ok := <-rchan:
			if ok {
				t.Logf("While performing %v", action)
				t.Errorf("Watch emitted unexpected event %v", got)
			}

		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestWatchListHistoricalCreate(t *testing.T) {
	test := &WatcherTest{
		Actions: []WatcherTestAction{
			{
				Type:  TestActionCreate,
				Key:   "/some/key/bar",
				Value: []byte("bar"),
			},
			{
				Type:  TestActionCreate,
				Key:   "/some/key/baz",
				Value: []byte("baz"),
			},
			{
				Type:  TestActionCreate,
				Key:   "/some/foreign/key",
				Value: []byte("fail"),
			},
			{
				Type:   TestActionStartWatchList,
				Key:    "/some/key",
				Expect: 2,
			},
			{
				Type: TestActionStopWatch,
			},
		},
		Expectations: []WatcherTestExpect{
			// 1st batch (Expect 2 of the following 2)
			{
				Type:     watch.Added,
				ValueCur: []byte("bar"),
			},
			{
				Type:     watch.Added,
				ValueCur: []byte("baz"),
			},
		},
	}

	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	test.RunTest(t, stg)
}

func TestWatchListHistoricalModify(t *testing.T) {
	test := &WatcherTest{
		Actions: []WatcherTestAction{
			{
				Type:  TestActionCreate,
				Key:   "/some/key/bar",
				Value: []byte("bar"),
			},
			{
				Type:  TestActionCreate,
				Key:   "/some/key/baz",
				Value: []byte("baz"),
			},
			{
				Type:  TestActionCreate,
				Key:   "/some/foreign/key",
				Value: []byte("fail"),
			},
			{
				Type: TestActionRememberVersion,
			},
			{
				Type:  TestActionSet,
				Key:   "/some/key/baz",
				Value: []byte("baz v2.0"),
			},
			{
				Type:  TestActionSet,
				Key:   "/some/foreign/key",
				Value: []byte("fail v2.0"),
			},
			{
				Type:     TestActionStartWatchList,
				Key:      "/some/key",
				Expect:   1,
				ExpectOf: 2,
			},
			{
				Type: TestActionStopWatch,
			},
		},
		Expectations: []WatcherTestExpect{
			// 1st batch (Expect 1 of the following 2)
			{
				Type:      watch.Modified,
				ValueCur:  []byte("baz v2.0"),
				ValuePrev: []byte("baz"),
			},
			{
				Type:      watch.Modified,
				ValueCur:  []byte("baz v2.0"),
				ValuePrev: []byte("baz v2.0"),
			},
		},
	}

	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	test.RunTest(t, stg)
}

func TestWatchValueHistoricalCreate(t *testing.T) {
	test := &WatcherTest{
		Actions: []WatcherTestAction{
			{
				Type:  TestActionCreate,
				Key:   "/some/key/bar",
				Value: []byte("bar"),
			},
			{
				Type:  TestActionCreate,
				Key:   "/some/key/baz",
				Value: []byte("fail"),
			},
			{
				Type:   TestActionStartWatchValue,
				Key:    "/some/key/bar",
				Expect: 1,
			},
			{
				Type: TestActionStopWatch,
			},
		},
		Expectations: []WatcherTestExpect{
			// 1st batch (Expect 1 of the following 1)
			{
				Type:     watch.Added,
				ValueCur: []byte("bar"),
			},
		},
	}

	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	test.RunTest(t, stg)
}

func TestWatchValueHistoricalModify(t *testing.T) {
	test := &WatcherTest{
		Actions: []WatcherTestAction{
			{
				Type:  TestActionCreate,
				Key:   "/some/key/bar",
				Value: []byte("bar"),
			},
			{
				Type:  TestActionCreate,
				Key:   "/some/key/baz",
				Value: []byte("baz"),
			},
			{
				Type: TestActionRememberVersion,
			},
			{
				Type:  TestActionSet,
				Key:   "/some/key/baz",
				Value: []byte("fail"),
			},
			{
				Type:  TestActionSet,
				Key:   "/some/key/bar",
				Value: []byte("bar v2.0"),
			},
			{
				Type:     TestActionStartWatchValue,
				Key:      "/some/key/bar",
				Expect:   1,
				ExpectOf: 2,
			},
			{
				Type: TestActionStopWatch,
			},
		},
		Expectations: []WatcherTestExpect{
			// 1st batch (Expect 1 of the following 2)
			{
				Type:      watch.Modified,
				ValueCur:  []byte("bar v2.0"),
				ValuePrev: []byte("bar"),
			},
			{
				Type:      watch.Modified,
				ValueCur:  []byte("bar v2.0"),
				ValuePrev: []byte("bar v2.0"),
			},
		},
	}

	server := factory.NewTestClientServer(t)
	defer server.Terminate(t)
	key := etcdtest.AddPrefix("/some/key")
	stg := NewRawPrefixer(server.NewRawStorage(), key)

	test.RunTest(t, stg)
}
