package storage

import (
	"fmt"
	"time"

	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage/generic"
	utilruntime "k8s.io/kubernetes/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
)

// Implements watch.Interface
type genericWatcher struct {
	resultChan chan watch.Event
	stopped    bool
	raw        generic.InterfaceRawWatch
	storage    *GenericWrapper
	filter     FilterFunc
	name       string
}

func NewGenericWatcher(raw generic.InterfaceRawWatch, storage *GenericWrapper, filter FilterFunc, name string) *genericWatcher {
	ret := &genericWatcher{
		resultChan: make(chan watch.Event, 100),
		raw:        raw,
		storage:    storage,
		filter:     filter,
		name:       name,
	}
	go ret.run()
	return ret
}

func (w *genericWatcher) run() {
	defer w.cleanup()
	internalResultChan := w.raw.ResultChan()
	for evIn := range internalResultChan {
		var evOut watch.Event
		evOut.Type = evIn.Type
		if evOut.Type == watch.Error {
			evOut.Object = evIn.ErrorStatus.(runtime.Object)
			w.resultChan <- evOut
			return
		} else {
			var curFilt, prevFilt bool
			if len(evIn.Previous.Data) > 0 {
				obj, err := w.decodeObject(&evIn.Previous)
				if err != nil {
					glog.Infof("Watcher-cooked %s: error ignore", w.name)
					continue
				}
				prevFilt = w.filter(obj)
				if prevFilt {
					evOut.Object = obj
				}
			}
			if len(evIn.Current.Data) > 0 {
				obj, err := w.decodeObject(&evIn.Current)
				if err != nil {
					glog.Infof("Watcher-cooked %s: error ignore", w.name)
					continue
				}
				curFilt = w.filter(obj)
				if curFilt {
					evOut.Object = obj
				}
			}
			switch {
			case prevFilt && !curFilt:
				evOut.Type = watch.Deleted

			case !prevFilt && curFilt:
				evOut.Type = watch.Added

			case !prevFilt && !curFilt:
				continue

			default:
				evOut.Type = watch.Modified
			}
		}
		if evOut.Type != "" {
			select {
			case <-time.After(30 * time.Second):
				glog.Errorf("Cooked watcher left with dangling output while watching %s", w.name)
				return

			case w.resultChan <- evOut:
				if evOut.Type == watch.Error {
					return
				}
			}
		}
	}
}

func (w *genericWatcher) cleanup() {
	close(w.resultChan)
}

func (w *genericWatcher) decodeObject(raw *generic.RawObject) (runtime.Object, error) {
	if obj, found := w.storage.getFromCache(raw.Version, Everything); found {
		return obj, nil
	}

	obj, err := runtime.Decode(w.storage.Codec(), raw.Data)
	if err != nil {
		return nil, err
	}

	// ensure resource version is set on the object we load from etcd
	if err := w.storage.versioner.UpdateObject(obj, raw.Version); err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to version api object (%d) %#v: %v", raw.Version, obj, err))
	}

	// perform any necessary transformation
	//if w.transform != nil {
	//	obj, err = w.transform(obj)
	//	if err != nil {
	//		utilruntime.HandleError(fmt.Errorf("failure to transform api object %#v: %v", obj, err))
	//		return nil, err
	//	}
	//}

	if raw.Version != 0 {
		w.storage.addToCache(raw.Version, obj)
	}
	return obj, nil

}

func (w *genericWatcher) Stop() {
	w.stopped = true
	w.raw.Stop()
}

func (w *genericWatcher) ResultChan() <-chan watch.Event {
	return w.resultChan
}
