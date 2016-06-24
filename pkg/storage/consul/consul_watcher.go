package consul 

import (
	//"fmt"
	"net/http"
	//"errors"
	//"strings"
	"sort"
	"sync"
	//"sync/atomic"
	//"time"
  
	"k8s.io/kubernetes/pkg/api/unversioned"
	//"k8s.io/kubernetes/pkg/conversion"
	//"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/generic"
	// TODO: relocate APIObjectVersioner to storage.APIObjectVersioner_uint64
	//"k8s.io/kubernetes/pkg/storage/etcd" // for the purpose of APIObjectVersioner
	"k8s.io/kubernetes/pkg/watch"

	consulapi "github.com/hashicorp/consul/api"
)

type retainedValue struct {
	value       []byte
	version     uint64
}

type consulWatch struct {
	stopChan    chan bool
	resultChan  chan generic.RawEvent
	storage     *ConsulKvStorage
	stopLock    sync.Mutex
	stopped     bool
	emit        func(generic.RawEvent) bool
}

func nullEmitter(w generic.RawEvent) bool {
	return false
}

func(s *ConsulKvStorage) newConsulWatch(key string, version uint64, deep bool) (*consulWatch, error) {
	if deep {
		KVs, _, err := s.ConsulKv.List(key, nil)
		if err != nil {
			return nil, err
		}
		w := &consulWatch{
			stopChan:   make( chan bool, 1 ),
			resultChan: make( chan generic.RawEvent ),
			storage:    s,
			stopped:    false,
		}
		w.emit = func(ev generic.RawEvent) bool {
			select {
				case <-w.stopChan:
					return false
					
				case w.resultChan <- ev:
					return true 
			}
		}
		go w.watchDeep(key, version, KVs)
		return w, nil
	} else {
		kv, _, err := s.ConsulKv.Get(key, nil)
		if err != nil {
			return nil, err
		}
		w := &consulWatch{
			stopChan:   make( chan bool, 1 ),
			resultChan: make( chan generic.RawEvent ),
			storage:    s,
			stopped:    false,
		}
		w.emit = func(ev generic.RawEvent) bool {
			select {
				case <-w.stopChan:
					return false
					
				case w.resultChan <- ev:
					return true 
			}
		}
		go w.watchSingle(key, version, kv)
		return w, nil
	}
}

type ByKey []*consulapi.KVPair
func(kvs ByKey) Len() int           { return len(kvs) }
func(kvs ByKey) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }

// *** assume that nil entries will NEVER be produced by consul's client
//func(kvs ByKey) Less(i, j int)  { (kvs[i] == nil && kvs[j] != nil) || (kvs[j] != nil && kvs[i].Key < kvs[j].Key) }
func(kvs ByKey) Less(i, j int) bool { return kvs[i].Key < kvs[j].Key }

func(w *consulWatch) watchDeep(key string, version uint64, kvsLast []*consulapi.KVPair) {
	defer w.clean()
	cont := true
	sort.Sort(ByKey(kvsLast))
	kvs := kvsLast
	versionNext := version
	for cont {
		j := 0
		for _, kv := range kvs {
			for ; j < len(kvsLast) && kvsLast[j].Key < kv.Key; j++ {
				cont = w.emitEvent( watch.Deleted, nil, kvsLast[j] )
			}

			if j >= len(kvsLast) {
				cont = w.emitEvent( watch.Added, kv, nil )
				if !cont {
					return
				}
				continue
			}

			kvLast := kvsLast[j]
			
			if kv.Key != kvLast.Key {
				cont = w.emitEvent( watch.Added, kv, nil )
			} else {
				j++
				if kv.ModifyIndex > version {
					if kv.CreateIndex > version {
						cont = w.emitEvent( watch.Added, kv, nil )
					} else {
						cont = w.emitEvent( watch.Modified, kv, kvLast )
					}
				}
			}
					
			if !cont {
				return
			}
		}
		for ; j < len(kvsLast); j++ {
			cont = w.emitEvent( watch.Deleted, nil, kvsLast[j] )
		}

		
		kvsLast = kvs
		for {
			version = versionNext
			var qm *consulapi.QueryMeta
			var err error
			kvs, qm, err = w.storage.ConsulKv.List( key, &consulapi.QueryOptions{ WaitIndex: version, WaitTime: w.storage.WaitTimeout } )
			if err != nil {
				w.emitError( key, err )
				return
			}
			// if we did not timeout
			if len(kvs) != 0 || qm.HttpStatusCode != 200 {
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


func(w *consulWatch) watchSingle(key string, version uint64, kvLast *consulapi.KVPair) {
	defer w.clean()
	cont := true
	kv := kvLast
	versionNext := version
	for cont {
		if kv == nil && kvLast != nil {
			cont = w.emit( generic.RawEvent{
					Type: watch.Deleted,
					Previous: generic.RawObject{
						Data:     kvLast.Value,
						Version:  kvLast.ModifyIndex,
					},
			} )
		}
		if kv != nil {
			if kv.ModifyIndex > version {
				if kv.CreateIndex > version || kvLast == nil {
					cont = w.emit( generic.RawEvent{
							Type: watch.Added,
							Current:  generic.RawObject{
								Data:     kv.Value,
								Version:  kv.ModifyIndex,
							},
					} )
				} else {
					cont = w.emit( generic.RawEvent{
							Type: watch.Modified,
							Current:  generic.RawObject{
								Data:     kv.Value,
								Version:  kv.ModifyIndex,
							},
							Previous: generic.RawObject{
								Data:     kvLast.Value,
								Version:  kvLast.ModifyIndex,
							},
					} )
				}
			}
		}
		
		if !cont {
			return
		}
		
		kvLast = kv
		version = versionNext
		var qm *consulapi.QueryMeta
		var err error
		kv, qm, err = w.storage.ConsulKv.Get( key, &consulapi.QueryOptions{ WaitIndex: version, WaitTime: w.storage.WaitTimeout } )
		if err != nil {
			w.emitError( key, err )
			return
		}
		versionNext = qm.LastIndex
	}
}

func(w *consulWatch) clean() {
	close(w.resultChan)
}

func(w *consulWatch) emitEvent( action watch.EventType, kvCur *consulapi.KVPair, kvPrev *consulapi.KVPair ) bool {
	event := generic.RawEvent{
		Type: action,
	}
	if kvCur != nil {
		event.Current.Data = kvCur.Value
		event.Current.Version = kvCur.ModifyIndex
	}
	if kvPrev != nil {
		event.Previous.Data = kvPrev.Value
		event.Previous.Version = kvPrev.ModifyIndex
	}
	w.emit(event)
	return !w.stopped
}

func(w *consulWatch) emitError( key string, err error ) {
	w.emit( generic.RawEvent{
			Type:           watch.Error,
			ErrorStatus:    &unversioned.Status{
				Status:  unversioned.StatusFailure,
				Message: err.Error(),
				Code:    http.StatusGone, // Gone
				Reason:  unversioned.StatusReasonExpired,
			},
	} )
}

func(w *consulWatch) Stop() {
	w.emit = nullEmitter
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

func(w *consulWatch) ResultChan() <-chan generic.RawEvent {
	return w.resultChan
}
