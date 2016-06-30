package etcd3

import (
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/generic"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/coreos/etcd/clientv3"
	etcdrpc "github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"net/http"
	"sync"
)

const (
	// We have set a buffer in order to reduce times of context switches.
	incomingBufSize = 100
	outgoingBufSize = 100
)

// watchChan implements generic.InterfaceRawWatch
type watchChanRaw struct {
	cancel            context.CancelFunc
	resultChan        chan generic.RawEvent
	watcher           *watcher
	key               string
	initialRev        int64
	recursive         bool
	ctx               context.Context
	incomingEventChan chan *event
	errChan           chan error
}

func (wc *watchChanRaw) Stop() {
	wc.cancel()
}

func (wc *watchChanRaw) ResultChan() <-chan generic.RawEvent {
	return wc.resultChan
}

func (wc *watchChanRaw) run() {
	go wc.startWatching()

	var resultChanWG sync.WaitGroup
	resultChanWG.Add(1)
	go wc.processEvent(&resultChanWG)

	select {
	case err := <-wc.errChan:
		errResult := parseError(err)
		if errResult != nil {
			// error result is guaranteed to be received by user before closing ResultChan.
			select {
			case wc.resultChan <- *errResult:
			case <-wc.ctx.Done(): // user has given up all results
			}
		}
		wc.cancel()
	case <-wc.ctx.Done():
	}
	// we need to wait until resultChan wouldn't be sent to anymore
	resultChanWG.Wait()
	close(wc.resultChan)
}

// sync tries to retrieve existing data and send them to process.
// The revision to watch will be set to the revision in response.
func (wc *watchChanRaw) sync() error {
	opts := []clientv3.OpOption{}
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	getResp, err := wc.watcher.client.Get(wc.ctx, wc.key, opts...)
	if err != nil {
		return err
	}
	wc.initialRev = getResp.Header.Revision

	for _, kv := range getResp.Kvs {
		wc.sendEvent(parseKV(kv))
	}
	return nil
}

// startWatching does:
// - get current objects if initialRev=0; set initialRev to current rev
// - watch on given key and send events to process.
func (wc *watchChanRaw) startWatching() {
	if wc.initialRev == 0 {
		if err := wc.sync(); err != nil {
			wc.sendError(err)
			return
		}
	}
	opts := []clientv3.OpOption{clientv3.WithRev(wc.initialRev + 1)}
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	wch := wc.watcher.client.Watch(wc.ctx, wc.key, opts...)
	for wres := range wch {
		if wres.Err() != nil {
			// If there is an error on server (e.g. compaction), the channel will return it before closed.
			wc.sendError(wres.Err())
			return
		}
		for _, e := range wres.Events {
			wc.sendEvent(parseEvent(e))
		}
	}
}

// processEvent processes events from etcd watcher and sends results to resultChan.
func (wc *watchChanRaw) processEvent(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case e := <-wc.incomingEventChan:
			res := wc.transform(e)
			if res == nil {
				continue
			}
			// If user couldn't receive results fast enough, we also block incoming events from watcher.
			// Because storing events in local will cause more memory usage.
			// The worst case would be closing the fast watcher.
			select {
			case wc.resultChan <- *res:
			case <-wc.ctx.Done():
				return
			}
		case <-wc.ctx.Done():
			return
		}
	}
}

// transform transforms an event into a result for user if not filtered.
// TODO (Optimization):
// - Save remote round-trip.
//   Currently, DELETE and PUT event don't contain the previous value.
//   We need to do another Get() in order to get previous object and have logic upon it.
//   We could potentially do some optimizations:
//   - For PUT, we can save current and previous objects into the value.
//   - For DELETE, See https://github.com/coreos/etcd/issues/4620
func (wc *watchChanRaw) transform(e *event) (res *generic.RawEvent) {
	curObj, oldObj, err := prepareObjs(wc.ctx, e, wc.watcher.client, wc.watcher.codec, wc.watcher.versioner)
	if err != nil {
		wc.sendError(err)
		return nil
	}

	switch {
	case e.isDeleted:
		res = &generic.RawEvent{
			Type:     watch.Deleted,
			Previous: *oldObj,
			Current:  *curObj,
		}
	case e.isCreated:
		res = &generic.RawEvent{
			Type:    watch.Added,
			Current: *curObj,
		}
	default:
		res = &generic.RawEvent{
			Type:     watch.Modified,
			Current:  *curObj,
			Previous: *oldObj,
		}
	}
	return res
}

func parseError(err error) *generic.RawEvent {
	var status *unversioned.Status
	switch {
	case err == etcdrpc.ErrCompacted:
		status = &unversioned.Status{
			Status:  unversioned.StatusFailure,
			Message: err.Error(),
			Code:    http.StatusGone,
			Reason:  unversioned.StatusReasonExpired,
		}
	default:
		status = &unversioned.Status{
			Status:  unversioned.StatusFailure,
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
			Reason:  unversioned.StatusReasonInternalError,
		}
	}

	return &generic.RawEvent{
		Type:        watch.Error,
		ErrorStatus: status,
	}
}

func (wc *watchChanRaw) sendError(err error) {
	// Context.canceled is an expected behavior.
	// We should just stop all goroutines in watchChan without returning error.
	// TODO: etcd client should return context.Canceled instead of grpc specific error.
	if grpc.Code(err) == codes.Canceled || err == context.Canceled {
		return
	}
	select {
	case wc.errChan <- err:
	case <-wc.ctx.Done():
	}
}

func (wc *watchChanRaw) sendEvent(e *event) {
	if len(wc.incomingEventChan) == incomingBufSize {
		glog.V(2).Infof("Fast watcher, slow processing. Number of buffered events: %d."+
			"Probably caused by slow decoding, user not receiving fast, or other processing logic",
			incomingBufSize)
	}
	select {
	case wc.incomingEventChan <- e:
	case <-wc.ctx.Done():
	}
}

func prepareObjs(ctx context.Context, e *event, client *clientv3.Client, codec runtime.Codec, versioner storage.Versioner) (curObj *generic.RawObject, oldObj *generic.RawObject, err error) {
	if !e.isDeleted {
		curObj, err = decodeObj(codec, versioner, e.value, e.rev)
		if err != nil {
			return nil, nil, err
		}
	}
	if e.isDeleted || !e.isCreated {
		getResp, err := client.Get(ctx, e.key, clientv3.WithRev(e.rev-1))
		if err != nil {
			return nil, nil, err
		}
		// Note that this sends the *old* object with the etcd revision for the time at
		// which it gets deleted.
		// We assume old object is returned only in Deleted event. Users (e.g. cacher) need
		// to have larger than previous rev to tell the ordering.
		oldObj, err = decodeObj(codec, versioner, getResp.Kvs[0].Value, e.rev)
		if err != nil {
			return nil, nil, err
		}
	}
	return curObj, oldObj, nil
}

func decodeObj(codec runtime.Codec, versioner storage.Versioner, data []byte, rev int64) (*generic.RawObject, error) {
	var obj generic.RawObject
	obj.Data = data
	obj.Version = uint64(rev)
	// TODO: TTL, UID
	return &obj, nil
}
