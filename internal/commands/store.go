package commands

import "sync"

// TODO (eatkinson): Should probably make this another package, it is not *really(
// related to commands and needs a better interface

type kvStore struct {
	store      map[string]storedValue
	storeMutex *sync.RWMutex
}

func (s kvStore) setKey(key string, value storedValue) *storedValue {
	s.storeMutex.Lock()
	s.store[key] = value
	s.storeMutex.Unlock()
	return &value
}

func (s kvStore) findKey(key string) (*storedValue, bool) {
	s.storeMutex.RLock()
	storedValue, ok := s.store[key]
	s.storeMutex.RUnlock()

	if !ok {
		return nil, false
	}

	return &storedValue, true
}

func (s kvStore) getKey(key string) (*storedValue, bool) {
	value, found := s.findKey(key)

	if !found {
		return nil, false
	}

	now := nowMilli()

	if value.expiresAt < now {
		s.deleteKey(key)
		return nil, false
	}
	return value, true
}

func (s kvStore) deleteKey(key string) {
	delete(s.store, key)
}

func newKVStore() kvStore {
	return kvStore{
		store:      map[string]storedValue{},
		storeMutex: &sync.RWMutex{},
	}
}
