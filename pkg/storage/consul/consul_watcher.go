package consul

import (
	"fmt"
	"reflect"
	"sync"

	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"
	consulwatch "github.com/hashicorp/consul/watch"
)

type retainedValue struct {
	value   []byte
	version uint64
}

type consulWatch struct {
	address    string
	stopChan   chan bool
	resultChan chan watch.Event
	storage    *consulHelper
	stopLock   sync.Mutex
	stopped    bool
	versioner  storage.Versioner
	prevKV     *consulapi.KVPair
	prevKVs    consulapi.KVPairs
}

func (w *consulWatch) emit(ev watch.Event) bool {
	select {
	case <-w.stopChan:
		return false

	case w.resultChan <- ev:
		return true
	}
}

func nullEmitter(w watch.Event) bool {
	return false
}

func (s *consulHelper) newConsulWatch(key string, version uint64, deep bool, versioner storage.Versioner, address string) (*consulWatch, error) {
	w := &consulWatch{
		address:    address,
		stopChan:   make(chan bool, 1),
		resultChan: make(chan watch.Event),
		storage:    s,
		stopped:    false,
		versioner:  versioner,
	}

	kv, _, err := s.consulKv.Get(key, nil)
	if err != nil {
		return nil, err
	}

	if deep {
		go w.watchNative(key, version, kv, true)
		return w, nil
	}

	//non-deep
	go w.watchNative(key, version, kv, false)
	return w, nil
}

type ByKey []*consulapi.KVPair

func (kvs ByKey) Len() int      { return len(kvs) }
func (kvs ByKey) Swap(i, j int) { kvs[i], kvs[j] = kvs[j], kvs[i] }

// *** assume that nil entries will NEVER be produced by consul's client
//func(kvs ByKey) Less(i, j int)  { (kvs[i] == nil && kvs[j] != nil) || (kvs[j] != nil && kvs[i].Key < kvs[j].Key) }
func (kvs ByKey) Less(i, j int) bool { return kvs[i].Key < kvs[j].Key }

func (w *consulWatch) decodeObject(kv *consulapi.KVPair) (runtime.Object, error) {
	obj, err := runtime.Decode(w.storage.Codec(), kv.Value)
	if err != nil {
		return nil, err
	}

	// ensure resource version is set on the object we load from etcd
	if err := w.versioner.UpdateObject(obj, kv.ModifyIndex); err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to version api object (%d) %#v: %v", kv.ModifyIndex, obj, err))
	}

	return obj, nil
}

func (w *consulWatch) decodeString(text string) (runtime.Object, error) {
	obj, err := runtime.Decode(w.storage.Codec(), []byte(text))
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (w *consulWatch) watchNative(key string, version uint64, kvLast *consulapi.KVPair, deep bool) {
	params := map[string]interface{}{}
	if deep == true {
		params["type"] = "keyprefix"
		params["prefix"] = key
	} else {
		params["type"] = "key"
		params["key"] = key
	}

	watchPlan, err := consulwatch.Parse(params)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to parse watch plan parameters: %v\n %#v", err, params))
		return
	}

	watchPlan.Handler = func(index uint64, obj interface{}) {
		if deep == true {
			var kvs consulapi.KVPairs
			if obj == nil { //the watched resoure does not exist yet
				kvs = make([]*consulapi.KVPair, 0)
			} else {
				kvs = obj.(consulapi.KVPairs)
			}

			w.handleWatchEventList(kvs)
			w.prevKVs = kvs
		} else {
			var kv *consulapi.KVPair
			if obj == nil { //the watched resoure does not exist yet
				w.handleWatchEvent(nil)
			} else {
				kv = obj.(*consulapi.KVPair)
				w.handleWatchEvent(kv)
			}

			w.prevKV = kv
		}
	}
	watchPlan.Run(w.address)
}

func (w *consulWatch) handleWatchEventList(newKVs consulapi.KVPairs) {
	var added, changed, deleted []*consulapi.KVPair
	var found, equal bool
	for _, k := range newKVs {
		found, equal = containsAndEqual(w.prevKVs, k)

		if !found {
			added = append(added, k)
		}
		if found && !equal {
			changed = append(changed, k)
		}
	}

	for _, k := range w.prevKVs {
		found, equal = containsAndEqual(newKVs, k)

		if !found {
			deleted = append(deleted, k)
		}
	}

	var decoded runtime.Object
	var err error
	for _, k := range added {
		decoded, err = w.decodeObject(k)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failure to decode object: %v\n %#v", err, k))
			continue
		}
		w.sendAdded(decoded)
	}
	for _, k := range changed {
		decoded, err = w.decodeObject(k)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failure to decode object: %v\n %#v", err, k))
			continue
		}
		w.sendModified(decoded)
	}
	for _, k := range deleted {
		decoded, err = w.decodeObject(k)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failure to decode object: %v\n %#v", err, k))
			continue
		}
		w.sendDeleted(decoded)
	}
}

func containsAndEqual(s consulapi.KVPairs, e *consulapi.KVPair) (bool, bool) {
	for _, a := range s {
		if a.Key == e.Key {
			return true, reflect.DeepEqual(a, e)
		}
	}
	return false, false
}

func (w *consulWatch) sendAdded(obj runtime.Object) {
	w.emit(watch.Event{
		Type:   watch.Added,
		Object: obj,
	})
}

func (w *consulWatch) sendModified(obj runtime.Object) {
	w.emit(watch.Event{
		Type:   watch.Modified,
		Object: obj,
	})
}

func (w *consulWatch) sendDeleted(obj runtime.Object) {
	w.emit(watch.Event{
		Type:   watch.Deleted,
		Object: obj,
	})
}

func (w *consulWatch) sendError(obj runtime.Object) {
	w.emit(watch.Event{
		Type:   watch.Error,
		Object: obj,
	})
}

func (w *consulWatch) handleWatchEvent(kv *consulapi.KVPair) {
	var decoded runtime.Object
	var err error

	//delete case, decode KVPair which was deleted
	if kv == nil && w.prevKV != nil {
		decoded, err = w.decodeObject(w.prevKV)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failure to decode object: %v\n %#v", err, kv))
			return
		}
	} else if kv != nil { //create/modify case
		decoded, err = w.decodeObject(kv)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failure to decode object: %v\n %#v", err, kv))
			return
		}
	} else {
		//this is unexpected
		//we should not get a nil kv when w.prevKV is nil as well. This indicates that we deleted a non existing key
		glog.Errorf("Unexpected watch behaviour. kv and prevKV are nil. Happens when we start watching a non existing key(prefix)")
		return
	}

	//DELETE
	if kv == nil {
		w.sendDeleted(decoded)
		return
	}

	//CREATE
	if kv.CreateIndex == kv.ModifyIndex {
		w.sendAdded(decoded)
		return
	}

	//MODIFY
	if kv.ModifyIndex > kv.CreateIndex {
		w.sendModified(decoded)
		return
	}

	w.sendError(decoded)
}

func (w *consulWatch) clean() {
	close(w.resultChan)
}

func (w *consulWatch) emitEvent(action watch.EventType, kv *consulapi.KVPair) bool {
	if kv == nil {
		return false
	}

	obj, err := w.decodeObject(kv)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to decode api object %#v: %v", kv, err))
		return false
	}

	w.emit(watch.Event{
		Type:   action,
		Object: obj,
	})

	return !w.stopped
}

func (w *consulWatch) emitError(key string, err error) {
	obj, err := w.decodeString(err.Error())
	if err != nil {
		return
	}

	w.emit(watch.Event{
		Type:   watch.Error,
		Object: obj,
	})
}

func (w *consulWatch) Stop() {
	//w.emit = nullEmitter
	if w.stopped {
		return
	}
	w.stopLock.Lock()
	defer w.stopLock.Unlock()
	if w.stopped {
		return
	}
	w.stopped = true
	close(w.stopChan)
}

func (w *consulWatch) ResultChan() <-chan watch.Event {
	return w.resultChan
}
