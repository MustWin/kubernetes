package consul

import (
	"strconv"
	"strings"
	"time"
	"net/url"

	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/generic"
	"k8s.io/kubernetes/pkg/util"

	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
)

const DefaultWaitTimeout = time.Duration(10 * time.Second)

type ConsulKvStorage struct {
	ConsulKv   consulapi.KV
	ServerList []string
	WaitTimeout time.Duration
}

type ConsulKvStorageConfig struct {
	// TBD
}

func (s *ConsulKvStorage) Backends(ctx context.Context) []string {
	return s.ServerList
}

func (s *ConsulKvStorage) Create(ctx context.Context, key string, data []byte, out *generic.RawObject, ttl uint64) error {
	trace := util.NewTrace("ConsulKvStorage::Create")
	defer trace.LogIfLong(250 * time.Millisecond)
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.transformKeyName(key)
	// TODO: metrics and stuff
	// startTime := time.Now()
	kv := &consulapi.KVPair{
		Key:         key,
		Value:       data,
		ModifyIndex: 0, // explicitly set to indicate Create-Only behavior
		// TODO: TTL, if and when this functionality becomes available
	}
	succeeded, _, err := s.ConsulKv.CAS_v2(kv, nil)
	// metrics.RecordStuff
	trace.Step("Object created")
	if err != nil {
		return toStorageErr(err, key, 0)
	}
	if !succeeded {
		return storage.NewKeyExistsError(key, 0)
		//kv, _, err = s.ConsulKv.Get(key, nil)
		//if err != nil {
		//	return toStorageErr( err, key, 0 )
		//}
	}
	if out != nil {
		out.Data = kv.Value
		out.Version = kv.ModifyIndex
		// TODO: emulate TTL if possible
	}
	return err
}

func (s *ConsulKvStorage) Set(ctx context.Context, key string, raw *generic.RawObject) (bool, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.transformKeyName(key)

	kv := consulapi.KVPair{
		Key:         key,
		ModifyIndex: raw.Version,
		Value:       raw.Data,
	}

	// Create and CAS are the same operation distinguished by
	// the same distinguishing value here - ModifyIndex == 0
	success, _, err := s.ConsulKv.CAS_v2(&kv, nil)

	if success {
		raw.Version = kv.ModifyIndex
	}

	return success, toStorageErr(err, key, 0)
}

func (s *ConsulKvStorage) Delete(ctx context.Context, key string, rawOut *generic.RawObject, preconditions generic.RawFilterFunc) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.transformKeyName(key)

	// kv declared outside of the spin-loop so that we can decode subsequent successful Gets
	// in the event that another client deletes our key before we do.. this value is possibly
	// lacking certified freshness
	var kvPrev *consulapi.KVPair
	var succeeded bool
	// spin cycle Get;DeleteCAS to ensure the returned value is the exact value prior to deletion
	// TODO: perhaps a timeout or spincount would be wise here
	for {
		// empty QueryOptions is explicitly setting AllowStale to false
		kv, _, err := s.ConsulKv.Get(key, &consulapi.QueryOptions{})
		if err != nil {
			return toStorageErr(err, key, 0)
		}
		if kv == nil {
			break
		}

		kvPrev = kv

		if preconditions != nil {
			rawForTest := generic.RawObject{
				Data:    kv.Value,
				Version: kv.ModifyIndex,
			}
			accepted, err := preconditions(&rawForTest)
			if err != nil {
				return err
			}
			if !accepted {
				return storage.NewResourceVersionConflictsError(key, 0)
			}
		}
		succeeded, _, err = s.ConsulKv.DeleteCAS(kv, nil)
		if err != nil {
			//if isErrNotFound( err ) {
			//	// if we have previously succeeded in getting a value, but not deleting it
			//	// then decode the most recently gotten value (unless we have already done
			//	// so in order to test for preconditions)
			//	if len(kv.Value) != 0 && preconditions == nil {
			//		err = s.extractObj(kv, err, out, false)
			//	}
			//}
			return toStorageErr(err, key, 0)
		}
		if succeeded {
			break
		}
		glog.Infof("delection of %s failed because of a conflict, going to retry", key)
	}
	if kvPrev != nil && rawOut != nil {
		rawOut.Data = kvPrev.Value
		rawOut.Version = kvPrev.ModifyIndex
	}
	if kvPrev == nil || len(kvPrev.Value) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}
	return nil
}

func (s *ConsulKvStorage) Watch(ctx context.Context, key string, resourceVersion string) (generic.InterfaceRawWatch, error) {
	key = s.transformKeyName(key)
	version, err := strconv.ParseUint(resourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	return s.newConsulWatch(key, version, false)
}

func (s *ConsulKvStorage) WatchList(ctx context.Context, key string, resourceVersion string) (generic.InterfaceRawWatch, error) {
	key = s.transformKeyName(key)
	version, err := strconv.ParseUint(resourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	return s.newConsulWatch(key, version, true)
}

func (s *ConsulKvStorage) Get(ctx context.Context, key string, raw *generic.RawObject) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.transformKeyName(key)
	kv, _, err := s.ConsulKv.Get(key, nil)
	if err != nil {
		return toStorageErr(err, key, 0)
	}
	if kv == nil || len(kv.Value) == 0 {
		return storage.NewKeyNotFoundError( key, 0 )
	}
	if raw != nil {
		raw.Data = kv.Value
		raw.Version = kv.ModifyIndex
		// TODO:TTL
	}
	return nil
}

type keyFilterFunc func(key string) bool

func (s *ConsulKvStorage) GetToList(ctx context.Context, key string, rawList *[]generic.RawObject) (uint64, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	// ensure that our path is terminated with a / to make it a directory
	key = s.transformKeyName(key) + "/"

	// create a filter that will omit deep finds
	myLastIndex := strings.LastIndex(key, "/")
	fnKeyFilter := func(key string) bool {
		return myLastIndex == strings.LastIndex(key, "/")
	}

	return s.listInternal("GetToList ", key, fnKeyFilter, rawList)
}

func (s *ConsulKvStorage) List(ctx context.Context, key string, resourceVersion string, rawList *[]generic.RawObject) (uint64, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = s.transformKeyName(key)
	fnKeyFilter := func(keyIn string) bool {
		return keyIn != key
	}
	return s.listInternal("List ", key, fnKeyFilter, rawList)
}

func (s *ConsulKvStorage) listInternal(fnName string, key string, keyFilter keyFilterFunc, rawList *[]generic.RawObject) (uint64, error) {
	trace := util.NewTrace(fnName + key)
	defer trace.LogIfLong(time.Second)

	kvlist, _, err := s.ConsulKv.List(key, nil)

	// TODO: record metrics
	if err != nil {
		return 0, toStorageErr(err, key, 0)
	}

	// unlike etcd, reads are not rafted, so they don't get an index of their own
	// so in order to version the resulting list consistantly, we apply the index
	// of the most recent member
	maxIndex := uint64(0)

	for _, kv := range kvlist {
		if kv != nil && keyFilter(kv.Key) {
			rawVal := generic.RawObject{
				Data:    kv.Value,
				Version: kv.ModifyIndex,
			}
			*rawList = append(*rawList, rawVal)
			if maxIndex < kv.ModifyIndex {
				maxIndex = kv.ModifyIndex
			}
		}
	}
	return maxIndex, nil
}

func (s *ConsulKvStorage) transformKeyName(keyIn string) string {
	return strings.Trim(keyIn, "/")
}


func toStorageErr(err error, key string, rv int64) error {
	switch err := err.(type) {
		case *url.Error:
			_ = err
			storeErr := storage.NewUnreachableError(key, rv)
			storeErr.AdditionalErrorMsg = "Consul agent not responding"
			return storeErr
	}
	return err
}
