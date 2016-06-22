package generic

import (
	//"time"

	"k8s.io/kubernetes/pkg/watch"

	"golang.org/x/net/context"
)

type RawObject struct {
	Data    []byte
	Version uint64
	TTL     int64
	UID     []byte
}

type RawFilterFunc func(raw *RawObject) (bool, error)

type RawEvent struct {
	Type        watch.EventType
	Current     RawObject
	Previous    RawObject
	ErrorStatus interface{}
}

type InterfaceRawWatch interface {
	Stop()
	ResultChan() <-chan RawEvent
}

type UpdateFunc func(raw *RawObject) error

type InterfaceRaw interface {
	Backends(ctx context.Context) []string

	Create(ctx context.Context, key string, data []byte, raw *RawObject, ttl uint64) error

	// Delete removes the specified key and returns the value that existed at that spot.
	// If key didn't exist, it will return NotFound storage error.
	Delete(ctx context.Context, key string, raw *RawObject, preconditions RawFilterFunc) error

	// Watch begins watching the specified key. Events are decoded into API objects,
	// and any items passing 'filter' are sent down to returned watch.Interface.
	// resourceVersion may be used to specify what version to begin watching,
	// which should be the current resourceVersion, and no longer rv+1
	// (e.g. reconnecting without missing any updates).
	Watch(ctx context.Context, key string, resourceVersion string) (InterfaceRawWatch, error)

	// WatchList begins watching the specified key's items. Items are decoded into API
	// objects and any item passing 'filter' are sent down to returned watch.Interface.
	// resourceVersion may be used to specify what version to begin watching,
	// which should be the current resourceVersion, and no longer rv+1
	// (e.g. reconnecting without missing any updates).
	WatchList(ctx context.Context, key string, resourceVersion string) (InterfaceRawWatch, error)

	// Get unmarshals json found at key into objPtr. On a not found error, will either
	// return a zero object of the requested type, or an error, depending on ignoreNotFound.
	// Treats empty responses and nil response nodes exactly like a not found error.
	Get(ctx context.Context, key string, raw *RawObject) error

	// List unmarshalls jsons found at directory defined by key and opaque them
	// into *List api object (an object that satisfies runtime.IsList definition).
	// The returned contents may be delayed, but it is guaranteed that they will
	// be have at least 'resourceVersion'.
	List(ctx context.Context, key string, resourceVersion string, rawList *[]RawObject) (uint64, error)

	// TODO: Figure out what, exactly, must be done to facilitate GuaranteedUpdate
	// from the raw layer
	Set(ctx context.Context, key string, raw *RawObject) (bool, error)
}
