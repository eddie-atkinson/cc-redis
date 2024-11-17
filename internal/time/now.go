package time

import (
	"context"
	"time"

	"github.com/tilinna/clock"
)

func NowMilli(clock clock.Clock) uint64 {
	return uint64((clock.Now().UnixNano() / int64(time.Millisecond)))
}

func HasExpired(ctx context.Context, expiresAt *uint64) bool {
	contextClock := clock.FromContext(ctx)
	now := NowMilli(contextClock)
	return expiresAt != nil && *expiresAt < now
}
