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

type storeChan struct {
	key   string
	id    StreamId
	value map[string]string
}

type KVStore struct {
	store             map[string]StoredValue
	storeMutex        *sync.RWMutex
	streamSubscribers map[string][]chan storeChan
	subscribersMutex  *sync.RWMutex
}

func (s *KVStore) Subscribe(key string, ch chan storeChan) {
	s.subscribersMutex.Lock()
	defer s.subscribersMutex.Unlock()
	s.streamSubscribers[key] = append(s.streamSubscribers[key], ch)
}

func (s *KVStore) Unsubscribe(key string, ch chan storeChan) {
	s.subscribersMutex.Lock()
	defer s.subscribersMutex.Unlock()

	subs := s.streamSubscribers[key]
	for i, sub := range subs {
		if sub == ch {
			s.streamSubscribers[key] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	if len(s.streamSubscribers[key]) == 0 {
		delete(s.streamSubscribers, key)
	}
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

	s.subscribersMutex.RLock()
	defer s.subscribersMutex.RUnlock()

	for _, ch := range s.streamSubscribers[key] {
		select {
		case ch <- storeChan{key, streamId, value}:
		default:
			// Non-blocking send to avoid blocking SetStream if a subscriber is slow
			// or has already timed out.
		}
	}

	return streamId, existingStream, nil
}

func (s KVStore) ReadStream(ctx context.Context, key string, startId string) ([]StreamQueryResult, error) {
	return s.QueryStream(ctx, key, startId, END_OF_STREAM)
}

func (s KVStore) ReadStreamBlocking(ctx context.Context, key string, startId string, result chan BlockingQueryResult) {
	defer close(result)
	ch := make(chan storeChan)
	s.Subscribe(key, ch)
	defer s.Unsubscribe(key, ch)

	select {
	case <-ctx.Done():
		return
	case data := <-ch:
		if data.key == key && data.id.ToString() > startId {
			queryResult, err := s.ReadStream(ctx, key, data.id.ToString())
			if err != nil {
				return
			}
			result <- BlockingQueryResult{data.key, queryResult}
		}
	}

}

func (s KVStore) QueryStream(ctx context.Context, key string, startId string, endId string) ([]StreamQueryResult, error) {
	result := []StreamQueryResult{}

	existingStream, exists := s.getStream(ctx, key)

	if !exists {
		return result, nil
	}

	startStreamId, err := GetQueryStreamId(startId)

	if err != nil {
		return result, err
	}

	endStreamId, err := GetQueryStreamId(endId)

	if err != nil {
		return result, err
	}

	existingStream.value.Walk(func(s string, v interface{}) bool {

		if err != nil || s < startStreamId.ToString() || s > endStreamId.ToString() {
			return false
		}

		maybeMap, ok := v.(map[string]string)

		if !ok {
			return false
		}

		kvList := []string{}

		for k, v := range maybeMap {
			kvList = append(kvList, k)
			kvList = append(kvList, v)
		}
		result = append(result, StreamQueryResult{s, kvList})
		return false
	})

	return result, nil
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
		store:             map[string]StoredValue{},
		storeMutex:        &sync.RWMutex{},
		subscribersMutex:  &sync.RWMutex{},
		streamSubscribers: map[string][]chan storeChan{},
	}
}
