package election

import (
	"bytes"
	//"fmt"
	"time"

	//"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/generic"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

func NewGenericMasterElector(storage generic.InterfaceRaw) MasterElector {
	return &genericMasterElector{storage: storage}
}

// internal implementation struct
type genericMasterElector struct {
	storage generic.InterfaceRaw
	done    chan empty
	events  chan watch.Event
}

// Elect implements the election.MasterElector interface.
func (e *genericMasterElector) Elect(path, id string) watch.Interface {
	e.done = make(chan empty)
	e.events = make(chan watch.Event)
	go wait.Until(func() { e.run(path, id) }, time.Second*5, wait.NeverStop)
	return e
}

func (e *genericMasterElector) run(path, id string) {
	masters := make(chan string)
	errors := make(chan error)
	go e.master(path, id, 30, masters, errors, e.done) // TODO(jdef) extract constant
	for {
		select {
		case m := <-masters:
			e.events <- watch.Event{
				Type:   watch.Modified,
				Object: Master(m),
			}
		case e := <-errors:
			glog.Errorf("Error in election: %v", e)
		}
	}
}

// ResultChan implements the watch.Interface interface.
func (e *genericMasterElector) ResultChan() <-chan watch.Event {
	return e.events
}

// extendMaster attempts to extend ownership of a master lock for TTL seconds.
// returns "", nil if extension failed
// returns id, nil if extension succeeded
// returns "", err if an error occurred
func (e *genericMasterElector) extendMaster(path, id string, ttl uint64, raw *generic.RawObject) (string, error) {
	// If it matches the passed in id, extend the lease by writing a new entry.
	// Uses compare and swap, so that if we TTL out in the meantime, the write will fail.
	// We don't handle the TTL delete w/o a write case here, it's handled in the next loop
	// iteration.
	newRaw := generic.RawObject{
		Data:    []byte(id),
		Version: raw.Version,
		TTL:     int64(ttl),
	}
	succeeded, err := e.storage.Set(context.TODO(), path, &newRaw)
	if err != nil {
		return "", err
	}
	if !succeeded {
		return "", nil
	}
	return id, nil
}

// becomeMaster attempts to become the master for this lock.
// returns "", nil if the attempt failed
// returns id, nil if the attempt succeeded
// returns "", err if an error occurred
func (e *genericMasterElector) becomeMaster(path, id string, ttl uint64) (string, error) {
	newRaw := generic.RawObject{}

	data := []byte(id)
	err := e.storage.Create(context.TODO(), path, data, &newRaw, ttl)
	if err != nil {
		// unexpected error
		return "", err
	}
	if !bytes.Equal(data, newRaw.Data) {
		return "", nil
	}
	return id, nil
}

// handleMaster performs one loop of master locking.
// on success it returns <master>, nil
// on error it returns "", err
// in situations where you should try again due to concurrent state changes (e.g. another actor simultaneously acquiring the lock)
// it returns "", nil
func (e *genericMasterElector) handleMaster(path, id string, ttl uint64) (string, error) {
	raw := generic.RawObject{}
	err := e.storage.Get(context.TODO(), path, &raw)

	// Unexpected error, bail out
	if err != nil {
		if storage.IsNotFound(err) {
			return e.becomeMaster(path, id, ttl)
		}
		return "", err
	}

	// There is no master, try to become the master.
	if len(raw.Data) == 0 {
		return e.becomeMaster(path, id, ttl)
	}

	// We're not the master, just return the current value
	value := string(raw.Data)
	if value != id {
		return value, nil
	}

	// We are the master, try to extend out lease
	return e.extendMaster(path, id, ttl, &raw)
}

// master provices a distributed master election lock, maintains lock until failure, or someone sends something in the done channel.
// The basic algorithm is:
// while !done
//   Get the current master
//   If there is no current master
//      Try to become the master
//   Otherwise
//      If we are the master, extend the lease
//      If the master is different than the last time through the loop, report the master
//   Sleep 80% of TTL
func (e *genericMasterElector) master(path, id string, ttl uint64, masters chan<- string, errors chan<- error, done <-chan empty) {
	lastMaster := ""
	for {
		master, err := e.handleMaster(path, id, ttl)
		if err != nil {
			errors <- err
		} else if len(master) == 0 {
			continue
		} else if master != lastMaster {
			lastMaster = master
			masters <- master
		}
		// TODO(k8s): Add Watch here, skip the polling for faster reactions
		// If done is closed, break out.
		select {
		case <-done:
			return
		case <-time.After(time.Duration((ttl*8)/10) * time.Second):
		}
	}
}

// ResultChan implements the watch.Interface interface
func (e *genericMasterElector) Stop() {
	close(e.done)
}
