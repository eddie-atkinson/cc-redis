package kvstore

import (
	"codecrafters/internal/serde"
	"codecrafters/internal/time"
	"context"
	"sync"

	"github.com/tilinna/clock"
)

type StoredValue interface {
	Value() serde.Value
	Type() string
	IsExpired(context.Context) bool
}

type KVStore struct {
	store      map[string]StoredValue
	storeMutex *sync.RWMutex
}

func (s KVStore) SetKeyWithExpiresAt(key string, value string, expiresAtMs *uint64) StoredValue {
	storedValue := NewStoredString(value, expiresAtMs)
	s.setKey(key, storedValue)
	return storedValue
}

func (s KVStore) SetKeyWithExpiry(ctx context.Context, key string, value string, expiresInMs *uint64) StoredString {
	contextClock := clock.FromContext(ctx)
	var expiresAt *uint64 = nil

	if expiresInMs != nil {
		expiresAt = new(uint64)
		*expiresAt = time.NowMilli(contextClock) + *expiresInMs
	}

	storedValue := NewStoredString(value, expiresAt)

	s.setKey(key, storedValue)
	return storedValue
}

func (s KVStore) SetStream(ctx context.Context, key string, id string, value map[string]string) (StreamId, StoredStream, error) {
	existingStream, exists := s.getStream(ctx, key)

	if !exists {
		existingStream = NewStoredStream()
	}

	streamId, err := existingStream.Insert(ctx, id, value)

	if err != nil {
		return streamId, existingStream, err
	}

	s.setKey(key, existingStream)

	return streamId, existingStream, nil
}

func (s KVStore) getStream(ctx context.Context, key string) (StoredStream, bool) {
	existingStream, exists := s.GetKey(ctx, key)

	if !exists {
		return StoredStream{}, exists
	}

	stream, ok := existingStream.(StoredStream)

	if !ok {
		return StoredStream{}, false
	}

	return stream, true
}

func (s KVStore) setKey(key string, value StoredValue) *StoredValue {
	s.storeMutex.Lock()
	defer s.storeMutex.Unlock()
	s.store[key] = value
	return &value
}

func (s KVStore) findKey(key string) (StoredValue, bool) {
	s.storeMutex.RLock()
	defer s.storeMutex.RUnlock()
	storedValue, ok := s.store[key]

	if !ok {
		return nil, false
	}

	return storedValue, true
}

func (s KVStore) GetKeys(ctx context.Context) []string {
	s.storeMutex.RLock()
	defer s.storeMutex.RUnlock()

	keys := []string{}

	for k, v := range s.store {
		_, exists := s.maybeDeleteExpiredEntry(ctx, k, v)
		if exists {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s KVStore) maybeDeleteExpiredEntry(ctx context.Context, key string, stored StoredValue) (StoredValue, bool) {

	if stored.IsExpired(ctx) {
		s.deleteKey(key)
		return nil, false
	}

	return stored, true
}

func (s KVStore) GetKey(ctx context.Context, key string) (StoredValue, bool) {

	value, found := s.findKey(key)

	if !found {
		return nil, false
	}
	return s.maybeDeleteExpiredEntry(ctx, key, value)
}

func (s KVStore) deleteKey(key string) {
	delete(s.store, key)
}

func NewKVStore() KVStore {
	return KVStore{
		store:      map[string]StoredValue{},
		storeMutex: &sync.RWMutex{},
	}
}
