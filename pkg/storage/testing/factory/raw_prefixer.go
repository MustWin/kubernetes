package testing

import (
	"k8s.io/kubernetes/pkg/storage/generic"

	"golang.org/x/net/context"
)


type RawPrefixer struct {
	Internal	generic.InterfaceRaw
	Prefix		string
}


func NewRawPrefixer(storage generic.InterfaceRaw, prefix string) generic.InterfaceRaw {
	if len(prefix) == 0 {
		return storage
	}
	return &RawPrefixer{
		Internal:	storage,
		Prefix:		prefix,
	}
}


func(r *RawPrefixer) Backends(ctx context.Context) []string {
	return r.Internal.Backends(ctx)
}

func(r *RawPrefixer) Create(ctx context.Context, key string, data []byte, raw *generic.RawObject, ttl uint64) error {
	return r.Internal.Create(ctx, r.Prefix + key, data, raw, ttl)
}

func(r *RawPrefixer) Delete(ctx context.Context, key string, raw *generic.RawObject, preconditions generic.RawFilterFunc) error {
	return r.Internal.Delete(ctx, r.Prefix + key, raw, preconditions)
}

func(r *RawPrefixer) Watch(ctx context.Context, key string, resourceVersion string) (generic.InterfaceRawWatch, error) {
	return r.Internal.Watch(ctx, r.Prefix + key, resourceVersion)
}

func(r *RawPrefixer) WatchList(ctx context.Context, key string, resourceVersion string) (generic.InterfaceRawWatch, error) {
	return r.Internal.WatchList(ctx, r.Prefix + key, resourceVersion)
}

func(r *RawPrefixer) Get(ctx context.Context, key string, raw *generic.RawObject) error {
	return r.Internal.Get(ctx, r.Prefix + key, raw)
}

func(r *RawPrefixer) List(ctx context.Context, key string, resourceVersion string, rawList *[]generic.RawObject) (uint64, error) {
	return r.Internal.List(ctx, r.Prefix + key, resourceVersion, rawList)
}

func(r *RawPrefixer) Set(ctx context.Context, key string, raw *generic.RawObject) (bool, error) {
	return r.Internal.Set(ctx, r.Prefix + key, raw)
}
