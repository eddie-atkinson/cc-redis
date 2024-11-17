package kvstore

import (
	"context"
	"testing"
	"time"

	"github.com/tilinna/clock"
)

func TestKVStore_SetKeyWithExpiry(t *testing.T) {
	eternalKey := "FOO"
	var eternalExpiry *uint64 = nil
	eternalValue := NewStoredString("BAR", eternalExpiry)

	var limitedExpiry *uint64 = new(uint64)
	// Give us a second
	*limitedExpiry = 1000
	limitedKey := "LIMITED"
	limitedValue := NewStoredString("TIME", limitedExpiry)

	store := NewKVStore()

	start := time.Date(2018, 1, 1, 10, 0, 0, 0, time.UTC)
	mock := clock.NewMock(start)
	ctx, _ := mock.DeadlineContext(context.Background(), start)

	store.SetKeyWithExpiry(ctx, eternalKey, eternalValue, eternalExpiry)
	store.SetKeyWithExpiry(ctx, limitedKey, limitedValue, limitedExpiry)

	// Test initial retrieval where there's no expiry
	mock = clock.NewMock(start.Add(time.Millisecond * 500))
	ctx, _ = mock.DeadlineContext(context.Background(), start)

	retrievedEternal, exists := store.GetKey(ctx, eternalKey)

	if !exists || retrievedEternal == nil || retrievedEternal.Value() != eternalValue.Value() {
		t.Fatalf("Expected eternal key value to be present in output, got %v", retrievedEternal)
	}

	retrievedLimited, exists := store.GetKey(ctx, limitedKey)

	if !exists || retrievedLimited == nil || retrievedLimited.Value() != limitedValue.Value() {
		t.Fatalf("Expected limited key value to be present in output, got %v", retrievedLimited)
	}

	mock = clock.NewMock(start.Add(time.Millisecond * 1001))
	ctx, _ = mock.DeadlineContext(context.Background(), start)

	retrievedEternal, exists = store.GetKey(ctx, eternalKey)

	if !exists || retrievedEternal == nil || retrievedEternal.Value() != eternalValue.Value() {
		t.Fatalf("Expected eternal key value to be present in output, got %v", retrievedEternal)
	}

	retrievedLimited, exists = store.GetKey(ctx, limitedKey)

	if exists || retrievedLimited != nil {
		t.Fatalf("Expected limited key value to NOT be present in output, got %v", retrievedLimited)
	}

}
