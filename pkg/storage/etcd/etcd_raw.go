package etcd

import (
	"time"

	"k8s.io/kubernetes/pkg/storage"
	etcdutil "k8s.io/kubernetes/pkg/storage/etcd/util"
	"k8s.io/kubernetes/pkg/storage/generic"

	etcd "github.com/coreos/etcd/client"
	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type etcdLowLevel struct {
	etcdMembersAPI etcd.MembersAPI
	etcdKeysAPI    etcd.KeysAPI
	quorum         bool
}

func NewEtcdRawStorage(client etcd.Client, quorum bool) generic.InterfaceRaw {
	return &etcdLowLevel{
		etcdMembersAPI: etcd.NewMembersAPI(client),
		etcdKeysAPI:    etcd.NewKeysAPI(client),
		quorum:         quorum,
	}
}

func (s *etcdLowLevel) Backends(ctx context.Context) []string {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	members, err := s.etcdMembersAPI.List(ctx)
	if err != nil {
		glog.Errorf("Error obtaining etcd members list: %q", err)
		return nil
	}
	mlist := []string{}
	for _, member := range members {
		mlist = append(mlist, member.ClientURLs...)
	}
	return mlist
}

func (s *etcdLowLevel) Create(ctx context.Context, key string, data []byte, raw *generic.RawObject, ttl uint64) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	opts := etcd.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		PrevExist: etcd.PrevNoExist,
	}
	response, err := s.etcdKeysAPI.Set(ctx, key, string(data), &opts)
	if err != nil {
		return toStorageErr(err, key, 0)
	}
	copyResponse(response, raw, false)
	return nil
}

// Delete removes the specified key and returns the value that existed at that spot.
// If key didn't exist, it will return NotFound storage error.
func (s *etcdLowLevel) Delete(ctx context.Context, key string, raw *generic.RawObject, preconditions generic.RawFilterFunc) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	if preconditions == nil {
		response, err := s.etcdKeysAPI.Delete(ctx, key, nil)
		copyResponse(response, raw, true)
		if etcdutil.IsEtcdNotFound(err) {
			return nil
		}
		return toStorageErr(err, key, 0)
	}

	var lastRead generic.RawObject
	for {
		opts := &etcd.GetOptions{
			Quorum: s.quorum,
		}

		response, err := s.etcdKeysAPI.Get(ctx, key, opts)

		copyResponse(response, &lastRead, false)
		if err != nil {
			if raw != nil {
				*raw = lastRead
			}
			return toStorageErr(err, key, 0)
		}
		succeeded, err := preconditions(&lastRead)
		if err != nil {
			return err
		}
		if !succeeded {
			return storage.NewResourceVersionConflictsError(key, 0)
		}
		opt := etcd.DeleteOptions{PrevIndex: lastRead.Version}
		response, err = s.etcdKeysAPI.Delete(ctx, key, &opt)
		if etcdutil.IsEtcdTestFailed(err) {
			glog.Infof("deletion of %s failed because of a conflict, going to retry", key)
		} else {
			if !etcdutil.IsEtcdNotFound(err) && raw != nil {
				copyResponse(response, raw, true)
				return nil
			}
			return toStorageErr(err, key, 0)
		}
	}
}

// Watch begins watching the specified key. Events are decoded into API objects,
// and any items passing 'filter' are sent down to returned watch.Interface.
// resourceVersion may be used to specify what version to begin watching,
// which should be the current resourceVersion, and no longer rv+1
// (e.g. reconnecting without missing any updates).
func (s *etcdLowLevel) Watch(ctx context.Context, key string, resourceVersion string) (generic.InterfaceRawWatch, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	watchRV, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	ret := newEtcdWatcherRaw(false, s.quorum, nil)
	go ret.etcdWatch(ctx, s.etcdKeysAPI, key, watchRV)
	return ret, nil
}

// WatchList begins watching the specified key's items. Items are decoded into API
// objects and any item passing 'filter' are sent down to returned watch.Interface.
// resourceVersion may be used to specify what version to begin watching,
// which should be the current resourceVersion, and no longer rv+1
// (e.g. reconnecting without missing any updates).
func (s *etcdLowLevel) WatchList(ctx context.Context, key string, resourceVersion string) (generic.InterfaceRawWatch, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	watchRV, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	ret := newEtcdWatcherRaw(true, s.quorum, exceptKey(key))
	go ret.etcdWatch(ctx, s.etcdKeysAPI, key, watchRV)
	return ret, nil
}

// Get unmarshals json found at key into objPtr. On a not found error, will either
// return a zero object of the requested type, or an error, depending on ignoreNotFound.
// Treats empty responses and nil response nodes exactly like a not found error.
func (s *etcdLowLevel) Get(ctx context.Context, key string, raw *generic.RawObject) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}

	opts := &etcd.GetOptions{
		Quorum: s.quorum,
	}

	response, err := s.etcdKeysAPI.Get(ctx, key, opts)
	copyResponse(response, raw, false)
	if err != nil {
		return toStorageErr(err, key, 0)
	}
	return nil
}

func (s *etcdLowLevel) Set(ctx context.Context, key string, raw *generic.RawObject) (bool, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	if raw == nil {
		return false, nil // maybe this should be an error.. a well behaved client should ask us to Set nothing
	}
	opts := etcd.SetOptions{
		PrevIndex: raw.Version,
		TTL:       time.Duration(raw.TTL) * time.Second,
	}
	if raw.Version == 0 {
		opts.PrevExist = etcd.PrevNoExist
	}
	response, err := s.etcdKeysAPI.Set(ctx, key, string(raw.Data), &opts)
	copyResponse(response, raw, false)
	if err != nil {
		if etcdutil.IsEtcdTestFailed(err) || (raw.Version == 0 && etcdutil.IsEtcdNodeExist(err)) {
			return false, nil
		}
		return false, toStorageErr(err, key, 0)
	}
	return true, nil
}

func (s *etcdLowLevel) List(ctx context.Context, key string, resourceVersion string, rawList *[]generic.RawObject) (uint64, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}

	opts := &etcd.GetOptions{
		Recursive: true,
		Sort:      true,
		Quorum:    s.quorum,
	}
	response, err := s.etcdKeysAPI.Get(ctx, key, opts)

	var index uint64
	if response != nil {
		index = response.Index
	}
	if err != nil {
		if etcdutil.IsEtcdNotFound(err) {
			return index, nil
		}
		return index, toStorageErr(err, key, 0)
	}

	if response == nil || response.Node == nil || response.Node.Nodes == nil {
		return index, nil
	}

	for _, node := range response.Node.Nodes {
		copyNodeList(node, rawList)
	}
	return index, nil
}

func copyNodeList(node *etcd.Node, rawList *[]generic.RawObject) {
	if node == nil {
		return
	}
	var raw generic.RawObject
	copyNode(node, &raw)
	if len(raw.Data) > 0 {
		*rawList = append(*rawList, raw)
	}
	if node.Dir {
		for _, subNode := range node.Nodes {
			copyNodeList(subNode, rawList)
		}
	}
}

func copyResponse(response *etcd.Response, raw *generic.RawObject, previous bool) {
	var node *etcd.Node

	if response == nil || raw == nil {
		return
	}
	if previous {
		node = response.PrevNode
	} else {
		node = response.Node
	}
	if node == nil {
		return
	}
	copyNode(node, raw)
}

func copyNode(node *etcd.Node, raw *generic.RawObject) {
	raw.Version = node.ModifiedIndex
	raw.Data = []byte(node.Value)
	if node.Expiration == nil {
		raw.TTL = 0
	} else {
		ttl := int64(node.Expiration.Sub(time.Now().UTC()) / time.Second)
		if ttl == 0 {
			ttl = 1
		}
		raw.TTL = ttl
	}
}
