package genericstorage

import (
	"fmt"
	"time"

	"k8s.io/kubernetes/contrib/mesos/pkg/scheduler/components/framework/frameworkid"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/generic"

	"golang.org/x/net/context"
)

type storageImpl struct {
	frameworkid.LookupFunc
	frameworkid.StoreFunc
	frameworkid.RemoveFunc
}

func Store(api generic.InterfaceRaw, path string, ttl time.Duration) frameworkid.Storage {
	// TODO(jdef) validate Config
	return &storageImpl{
		LookupFunc: func(ctx context.Context) (string, error) {
			var obj generic.RawObject
			if err := api.Get(ctx, path, &obj); err != nil {
				if !storage.IsNotFound(err) {
					return "", fmt.Errorf("unexpected failure attempting to load framework ID: %v", err)
				}
			} else {
				return string(obj.Data), nil
			}
			return "", nil
		},
		RemoveFunc: func(ctx context.Context) (err error) {
			if err = api.Delete(ctx, path, nil, nil); err != nil {
				if !storage.IsNotFound(err) {
					return fmt.Errorf("failed to delete framework ID: %v", err)
				}
			}
			return
		},
		StoreFunc: func(ctx context.Context, id string) (err error) {
			_, err = api.Set(ctx, path, &generic.RawObject{Data: []byte(id), TTL: int64(ttl / time.Second)})
			return
		},
	}
}
