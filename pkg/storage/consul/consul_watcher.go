package consul

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/watch"

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

func (w *consulWatch) watchDeep(key string, version uint64, versionNext uint64, kvsLast []*consulapi.KVPair) {
	defer w.clean()
	cont := true
	sort.Sort(ByKey(kvsLast))
	kvs := kvsLast
	//versionNext := version
	for cont {
		j := 0
		for _, kv := range kvs {
			for ; j < len(kvsLast) && kvsLast[j].Key < kv.Key; j++ {
				cont = w.emitEvent(watch.Deleted, kvsLast[j])
			}

			if j >= len(kvsLast) {
				cont = w.emitEvent(watch.Added, kv)
				if !cont {
					return
				}
				continue
			}

			kvLast := kvsLast[j]

			if kv.Key != kvLast.Key {
				cont = w.emitEvent(watch.Added, kv)
			} else {
				j++
				if kv.ModifyIndex > version {
					if kv.CreateIndex > version {
						cont = w.emitEvent(watch.Added, kv)
					} else {
						cont = w.emitEvent(watch.Modified, kv)
					}
				}
			}

			if !cont {
				return
			}
		}
		for ; j < len(kvsLast); j++ {
			cont = w.emitEvent(watch.Deleted, kvsLast[j])
		}

		timeout := time.Second * 10

		kvsLast = kvs
		for {
			version = versionNext
			var qm *consulapi.QueryMeta
			var err error
			kvs, qm, err = w.storage.consulKv.List(key, &consulapi.QueryOptions{WaitIndex: version, WaitTime: timeout})
			if err != nil {
				w.emitError(key, err)
				return
			}
			// if we did not timeout
			if len(kvs) != 0 {
				versionNext = qm.LastIndex
				break
			}
			if w.stopped {
				return
			}
		}
		sort.Sort(ByKey(kvs))
	}
}

//TODO: proper implementation
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
		if obj == nil {
			return
		}

		if deep == true {
			kvs := obj.(consulapi.KVPairs)
			if len(kvs) == 0 {
				//TODO: we can't deal with empty results, do nothing
			}
			for _, kv := range kvs {
				w.handlewatchEvent(kv)
			}
		} else {
			kv := obj.(*consulapi.KVPair)
			w.handlewatchEvent(kv)
		}
	}
	watchPlan.Run(w.address)
}

func (w *consulWatch) handlewatchEvent(kv *consulapi.KVPair) {
	decoded, err := w.decodeObject(kv)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to decode object: %v\n %#v", err, kv))
		return
	}

	//CREATE
	if kv.CreateIndex == kv.ModifyIndex {
		w.emit(watch.Event{
			Type:   watch.Added,
			Object: decoded,
		})
		return
	}

	//MODIFY
	if kv.ModifyIndex > kv.CreateIndex {
		w.emit(watch.Event{
			Type:   watch.Modified,
			Object: decoded,
		})
		return
	}

	w.emit(watch.Event{
		Type:   watch.Error,
		Object: decoded,
	})
}

func (w *consulWatch) watchSingle(key string, version uint64, kvLast *consulapi.KVPair) {
	defer w.clean()
	cont := true
	kv := kvLast
	for cont {
		if kv == nil && kvLast != nil {
			obj, err := w.decodeObject(kvLast)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("failure to decode api object: %v\n'%v' from %#v", err, string(kvLast.Value), kvLast))
				return
			}

			cont = w.emit(watch.Event{
				Type:   watch.Deleted,
				Object: obj,
			})
		}
		if kv != nil {
			if kv.ModifyIndex > version {
				if kv.CreateIndex > version || kvLast == nil {
					obj, err := w.decodeObject(kvLast)
					if err != nil {
						utilruntime.HandleError(fmt.Errorf("failure to decode api object: %v\n'%v' from %#v", err, string(kv.Value), kv))
						return
					}

					cont = w.emit(watch.Event{
						Type:   watch.Added,
						Object: obj,
					})
				} else {
					obj, err := w.decodeObject(kv)
					if err != nil {
						utilruntime.HandleError(fmt.Errorf("failure to decode api object: %v\n'%v' from %#v", err, string(kv.Value), kv))
						return
					}
					cont = w.emit(watch.Event{
						Type:   watch.Modified,
						Object: obj,
					})
				}
			}
		}

		if !cont {
			return
		}

		timeout := time.Second * 10

		kvLast = kv
		var err error
		kv, _, err = w.storage.consulKv.Get(key, &consulapi.QueryOptions{WaitIndex: version, WaitTime: timeout})
		if err != nil {
			w.emitError(key, err)
			return
		}
	}
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
